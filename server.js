const express = require('express');
const mqtt = require('mqtt');
const path = require('path');

const app = express();
const port = process.env.PORT || 3001;

// Kết nối MQTT TLS tới flespi. KHÔNG lưu token trong mã nguồn.
const flespiToken = (process.env.FLESPI_TOKEN || '').trim();
if (!flespiToken) {
    console.error('THIEU FLESPI_TOKEN. Hay dat bien moi truong truoc khi npm start.');
    process.exit(1);
}

const client = mqtt.connect('mqtts://mqtt.flespi.io:8883', {
    username: flespiToken,
    password: '',
    clientId: `MollyNode-${process.pid}-${Math.random().toString(16).slice(2, 8)}`,
    protocolVersion: 4, // MQTT 3.1.1: tuong thich voi ESP32 PubSubClient
    keepalive: 60,
    connectTimeout: 15000,
    reconnectPeriod: 5000,
    clean: true,
    rejectUnauthorized: true
});

client.on('reconnect', () => console.log('[MQTT] Dang thu ket noi lai flespi...'));
client.on('offline', () => console.warn('[MQTT] Mat ket noi voi broker.'));
client.on('error', (err) => console.error('[MQTT] Loi:', err.message));

let currentFoodLevel = 100;
let currentMode = 'MANUAL'; // Biến lưu chế độ hiện tại
let currentTimeSetting = '08:00'; // Biến lưu giờ hẹn hiện tại

client.on('connect', () => {
    console.log('[MQTT] Node.js da ket noi thanh cong flespi TLS:8883');
    client.subscribe('molly_tank/food_level');
    client.subscribe('molly_tank/mode');      // Lắng nghe chế độ
    client.subscribe('molly_tank/set_time');  // Lắng nghe giờ hẹn
});

client.on('message', (topic, message) => {
    const msgStr = message.toString();
    if (topic === 'molly_tank/food_level') {
        currentFoodLevel = parseInt(msgStr);
    } else if (topic === 'molly_tank/mode') {
        currentMode = msgStr; // Cập nhật khi có thiết bị đổi chế độ
    } else if (topic === 'molly_tank/set_time') {
        currentTimeSetting = msgStr; // Cập nhật khi có thiết bị đổi giờ
    }
});

app.use(express.json());

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'index.html'));
});

app.get('/feed', (req, res) => {
    client.publish('molly_tank/control', 'FEED');
    res.send('Thành công');
});

app.get('/food-level', (req, res) => {
    res.send(currentFoodLevel.toString());
});

// API trả về trạng thái hiện tại (mode và time) cho web cập nhật
app.get('/status', (req, res) => {
    res.json({ mode: currentMode, time: currentTimeSetting });
});

app.get('/set-mode/:mode', (req, res) => {
    const selectedMode = req.params.mode;
    client.publish('molly_tank/mode', selectedMode);
    res.send('Đã đổi chế độ: ' + selectedMode);
});

app.get('/set-time/:time', (req, res) => {
    const feedingTime = req.params.time;
    const formattedTime = feedingTime.replace('-', ':');
    client.publish('molly_tank/set_time', formattedTime);
    res.send('Đã hẹn giờ: ' + formattedTime);
});

app.listen(port, () => {
    console.log(`Web server đang chạy tại cổng ${port}`);
});
