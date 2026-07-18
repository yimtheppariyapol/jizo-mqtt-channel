# jizo-mqtt-channel

แกะโปรโตคอล MQTT 3.1.1 เองด้วยมือ ไม่พึ่ง library `mqtt` npm ใดๆ
ใช้แค่ `net.Socket` ของ Node ล้วนๆ ต่อกับ broker mosquitto ที่รันอยู่แล้วบนเครื่อง

โจทย์จากพี่นัท (9 ก.ค., ยิ้มสั่งปิดในวันนี้): ถอดแอป Discord Channel ให้เหลือแค่แกนที่จำเป็น
แล้วสลับ transport จาก Discord gateway ไปเป็น MQTT แทน ตัว channel abstraction
(รับข้อความจาก topic แล้วพิมพ์ `[topic] payload`) ยังเหมือนเดิมทุกอย่าง เปลี่ยนแค่ท่อที่ข้อความไหลผ่าน

## ไฟล์

- `channel.mjs` เอนจินหลัก hand-roll MQTT CONNECT / SUBSCRIBE / PUBLISH / PINGREQ เอง
- `ARTICLE.md` บทความเล่ากายวิภาคของ MQTT packet ที่ library ปกติซ่อนไว้
- `proof.log` หลักฐานการรัน `--prove` แต่ละครั้ง (timestamp + nonce + topic)

## วิธีรัน

โหมดฟังข้อความจริง (ต้องมี broker ที่ 127.0.0.1:1883 อยู่แล้ว):

```
node channel.mjs jizo/inbox
```

จะพิมพ์ทุกข้อความที่มีคนส่งเข้า topic นั้นในรูปแบบ `[topic] payload`
ทดสอบคู่กับ mosquitto client จริงได้ เช่นเปิดอีก terminal แล้วรัน
`mosquitto_pub -h 127.0.0.1 -t jizo/inbox -m "hello"`

## วิธีพิสูจน์ (--prove)

```
node channel.mjs --prove
```

ลำดับที่เกิดขึ้นจริง:

1. เปิด socket ต่อ broker, ส่ง CONNECT, รอ CONNACK
2. ส่ง SUBSCRIBE ไปที่ topic `jizo/inbox`, รอ SUBACK
3. เมื่อ subscribe สำเร็จ สุ่ม nonce แล้ว spawn คำสั่งจริง
   `mosquitto_pub -h 127.0.0.1 -t jizo/inbox -m "MQTT-PROOF-<nonce>"`
   ใช้ client ตัวจริงของ mosquitto เป็นคู่ทดสอบ ไม่ยิงหาตัวเอง
   เพื่อพิสูจน์ว่า parser ของเราคุยกับ implementation อื่นได้จริง (interop)
4. รอรับ PUBLISH ผ่าน parser ของตัวเองภายใน 15 วินาที ถ้า nonce ตรงกับที่ส่งไป
   จะพิมพ์ `PROOF OK <nonce>`, เขียนบรรทัดลง `proof.log`, และ exit 0
   ถ้าไม่ทันภายในเวลาจะ exit 1

ผลจริงจากการรันบนเครื่องนี้:

```
subscribed to jizo/inbox, publishing proof via real mosquitto_pub...
[jizo/inbox] MQTT-PROOF-bb784dc9bfd5
PROOF OK bb784dc9bfd5
```

exit code 0, และ `proof.log` มีบรรทัด `nonce=bb784dc9bfd5 topic=jizo/inbox` พร้อม timestamp
ไม่มีข้อมูล sensitive ใดๆ ในไฟล์ log นี้

## ประเด็นเทคนิคที่ต้องระวังตอนแกะเอง

- Remaining Length ในทุก MQTT packet เป็น varint แบบ 7 bit ต่อ byte พร้อม continuation bit
  ต้อง encode ตอนส่งและ decode ตอนอ่านให้ตรงกันทั้งสองทาง
- TCP เป็น byte stream ไม่ใช่ message stream แพ็กเก็ต PUBLISH อาจมาไม่ครบใน event เดียว
  หรือหลายแพ็กเก็ตมาพร้อมกันใน event เดียว ต้องมี buffer สะสมแล้วค่อยตัดเป็นแพ็กเก็ตทีละอัน
- SUBSCRIBE ต้องใช้ fixed header flags = `0x02` เท่านั้นตาม spec ไม่ใช่ `0x00` เหมือน packet อื่น
- QoS 0 PUBLISH ไม่มี packet identifier ต่างจาก QoS 1/2 ที่ต้องอ่าน 2 byte เพิ่ม

รายละเอียดเชิงลึกกว่านี้อยู่ใน `ARTICLE.md`

---

Jizo 🗿 เขียนโดย AI (Rule 6)
