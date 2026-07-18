#!/usr/bin/env node
// channel.mjs — Minimal "Discord Channel" replaced by MQTT transport.
// Hand-rolled MQTT 3.1.1 client over raw net.Socket. No mqtt npm package.
// Spirit: channel is the same abstraction (topic -> message stream), only the
// transport changed from Discord gateway to MQTT broker.

import net from 'node:net';
import { spawn } from 'node:child_process';
import { writeFileSync, appendFileSync } from 'node:fs';
import crypto from 'node:crypto';

const HOST = '127.0.0.1';
const PORT = 1883;
const KEEPALIVE_SEC = 30;
const CLIENT_ID = 'jizo-channel-' + Math.random().toString(16).slice(2, 10);

// ---------- MQTT 3.1.1 packet encoding helpers ----------

// Remaining Length is a varint: 7 bits of data + 1 continuation bit per byte,
// little-endian order, up to 4 bytes (max value 268,435,455).
function encodeRemainingLength(length) {
  const bytes = [];
  let x = length;
  do {
    let encodedByte = x % 128;
    x = Math.floor(x / 128);
    if (x > 0) encodedByte |= 0x80; // continuation bit set: more bytes follow
    bytes.push(encodedByte);
  } while (x > 0);
  return Buffer.from(bytes);
}

// Returns { value, bytesUsed } decoded from buf starting at offset.
function decodeRemainingLength(buf, offset) {
  let multiplier = 1;
  let value = 0;
  let bytesUsed = 0;
  let byte;
  do {
    if (offset + bytesUsed >= buf.length) return null; // not enough data yet
    byte = buf[offset + bytesUsed];
    value += (byte & 0x7f) * multiplier;
    multiplier *= 128;
    bytesUsed++;
    if (multiplier > 128 * 128 * 128 * 128) throw new Error('malformed remaining length');
  } while ((byte & 0x80) !== 0);
  return { value, bytesUsed };
}

// MQTT strings are UTF-8 prefixed with a 2-byte big-endian length.
function encodeString(str) {
  const strBuf = Buffer.from(str, 'utf8');
  const lenBuf = Buffer.alloc(2);
  lenBuf.writeUInt16BE(strBuf.length, 0);
  return Buffer.concat([lenBuf, strBuf]);
}

function buildPacket(packetType, flags, variableHeaderAndPayload) {
  const fixedHeaderByte1 = (packetType << 4) | (flags & 0x0f);
  const remainingLength = encodeRemainingLength(variableHeaderAndPayload.length);
  return Buffer.concat([Buffer.from([fixedHeaderByte1]), remainingLength, variableHeaderAndPayload]);
}

// ---------- CONNECT ----------
function buildConnectPacket(clientId, keepAliveSec) {
  const protocolName = encodeString('MQTT');
  const protocolLevel = Buffer.from([0x04]); // MQTT 3.1.1
  // Connect flags: bit1 = Clean Session. No username/password/will/retain here.
  const connectFlags = Buffer.from([0x02]);
  const keepAlive = Buffer.alloc(2);
  keepAlive.writeUInt16BE(keepAliveSec, 0);
  const clientIdBuf = encodeString(clientId);
  const variableHeaderAndPayload = Buffer.concat([
    protocolName, protocolLevel, connectFlags, keepAlive, clientIdBuf,
  ]);
  return buildPacket(1, 0x00, variableHeaderAndPayload); // CONNECT = type 1
}

// ---------- SUBSCRIBE (QoS 0) ----------
let packetIdCounter = 1;
function buildSubscribePacket(topic) {
  const packetId = packetIdCounter++;
  const packetIdBuf = Buffer.alloc(2);
  packetIdBuf.writeUInt16BE(packetId, 0);
  const topicFilter = encodeString(topic);
  const qos = Buffer.from([0x00]); // requested QoS 0
  const variableHeaderAndPayload = Buffer.concat([packetIdBuf, topicFilter, qos]);
  // SUBSCRIBE = type 8, flags MUST be 0x02 per spec (reserved bits)
  return { packet: buildPacket(8, 0x02, variableHeaderAndPayload), packetId };
}

// ---------- PINGREQ ----------
function buildPingReqPacket() {
  return buildPacket(12, 0x00, Buffer.alloc(0)); // PINGREQ = type 12, no payload
}

// ---------- PUBLISH parsing ----------
function parsePublish(variableHeaderAndPayload) {
  const topicLen = variableHeaderAndPayload.readUInt16BE(0);
  const topic = variableHeaderAndPayload.toString('utf8', 2, 2 + topicLen);
  // QoS 0 publish has no packet identifier, payload starts right after topic
  const payload = variableHeaderAndPayload.subarray(2 + topicLen);
  return { topic, payload };
}

// ---------- Socket-level packet stream reassembly ----------
// TCP is a byte stream, not a message stream — a PUBLISH can arrive split
// across multiple 'data' events, or several packets can arrive in one event.
// We buffer everything and repeatedly try to peel off one full packet.
class MqttPacketReader {
  constructor(onPacket) {
    this.buf = Buffer.alloc(0);
    this.onPacket = onPacket;
  }
  push(chunk) {
    this.buf = Buffer.concat([this.buf, chunk]);
    while (true) {
      if (this.buf.length < 2) return; // need at least fixed header byte1 + 1 length byte
      const byte1 = this.buf[0];
      const packetType = (byte1 >> 4) & 0x0f;
      const flags = byte1 & 0x0f;
      const decoded = decodeRemainingLength(this.buf, 1);
      if (decoded === null) return; // remaining length itself not fully buffered yet
      const { value: remainingLength, bytesUsed } = decoded;
      const totalPacketLength = 1 + bytesUsed + remainingLength;
      if (this.buf.length < totalPacketLength) return; // full packet not arrived yet
      const body = this.buf.subarray(1 + bytesUsed, totalPacketLength);
      this.onPacket(packetType, flags, body);
      this.buf = this.buf.subarray(totalPacketLength);
    }
  }
}

// ---------- Main channel logic ----------
function log(topic, payload) {
  console.log(`[${topic}] ${payload}`);
}

function connectChannel({ topic, onMessage, onSubscribed }) {
  const socket = net.createConnection({ host: HOST, port: PORT });
  let pingTimer = null;

  socket.on('connect', () => {
    socket.write(buildConnectPacket(CLIENT_ID, KEEPALIVE_SEC));
  });

  const reader = new MqttPacketReader((packetType, flags, body) => {
    switch (packetType) {
      case 2: { // CONNACK
        const returnCode = body[1];
        if (returnCode !== 0) {
          console.error(`CONNACK refused, return code ${returnCode}`);
          process.exit(1);
        }
        const { packet } = buildSubscribePacket(topic);
        socket.write(packet);
        // keepalive: send PINGREQ well before KEEPALIVE_SEC elapses
        pingTimer = setInterval(() => {
          socket.write(buildPingReqPacket());
        }, KEEPALIVE_SEC * 1000 * 0.5);
        break;
      }
      case 9: { // SUBACK
        if (onSubscribed) onSubscribed();
        break;
      }
      case 3: { // PUBLISH
        const { topic: msgTopic, payload } = parsePublish(body);
        onMessage(msgTopic, payload.toString('utf8'));
        break;
      }
      case 13: { // PINGRESP
        break;
      }
      default:
        break;
    }
  });

  socket.on('data', (chunk) => reader.push(chunk));
  socket.on('error', (err) => {
    console.error('socket error:', err.message);
  });
  socket.on('close', () => {
    if (pingTimer) clearInterval(pingTimer);
  });

  return socket;
}

// ---------- Modes ----------

function runListenMode(topic) {
  connectChannel({
    topic,
    onSubscribed: () => console.error(`subscribed to ${topic}, waiting for messages...`),
    onMessage: (t, payload) => log(t, payload),
  });
}

function runProveMode() {
  const topic = 'jizo/inbox';
  const nonce = crypto.randomBytes(6).toString('hex');
  const expectedPayload = `MQTT-PROOF-${nonce}`;
  let settled = false;

  const timeout = setTimeout(() => {
    if (settled) return;
    settled = true;
    console.error(`TIMEOUT waiting for proof (nonce ${nonce})`);
    process.exit(1);
  }, 15000);

  const socket = connectChannel({
    topic,
    onSubscribed: () => {
      console.error(`subscribed to ${topic}, publishing proof via real mosquitto_pub...`);
      // Interop check: use the actual mosquitto client binary as the publisher,
      // not our own socket, so success proves we speak real MQTT to a real client.
      const pub = spawn('mosquitto_pub', ['-h', HOST, '-t', topic, '-m', expectedPayload]);
      pub.on('error', (err) => {
        console.error('failed to spawn mosquitto_pub:', err.message);
      });
    },
    onMessage: (t, payload) => {
      log(t, payload);
      if (settled) return;
      if (t === topic && payload === expectedPayload) {
        settled = true;
        clearTimeout(timeout);
        console.log(`PROOF OK ${nonce}`);
        const line = `${new Date().toISOString()} nonce=${nonce} topic=${t}\n`;
        appendFileSync('proof.log', line);
        socket.end();
        process.exit(0);
      }
    },
  });
}

// ---------- Entry point ----------
const args = process.argv.slice(2);
if (args.includes('--prove')) {
  runProveMode();
} else {
  const topicArg = args.find((a) => !a.startsWith('--')) || 'jizo/inbox';
  runListenMode(topicArg);
}
