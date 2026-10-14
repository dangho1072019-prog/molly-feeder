const express = require('express');
const mqtt = require('mqtt');
const path = require('path');

const app = express();
const port = process.env.PORT || 3001;

const client = mqtt.connect('mqtt://broker.hivemq.com');

let currentFoodLevel = 100;
let currentMode = 'MANUAL'; // Biến lưu chế độ hiện tại
let currentTimeSetting = '08:00'; // Biến lưu giờ hẹn hiện tại

client.on('connect', () => {
    console.log('Node.js đã kết nối MQTT Broker');
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
