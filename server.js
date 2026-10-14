const express = require('express');
const mqtt = require('mqtt');
const path = require('path');

const app = express();
const port = 3001; 

const client = mqtt.connect('mqtt://broker.hivemq.com');

let currentFoodLevel = 100; // Mặc định 100%

client.on('connect', () => {
    console.log('Node.js đã kết nối MQTT Broker');
    // Đăng ký nhận dữ liệu mức thức ăn
    client.subscribe('molly_tank/food_level'); 
});

// Xử lý khi có dữ liệu từ ESP32 gửi lên
client.on('message', (topic, message) => {
    if (topic === 'molly_tank/food_level') {
        currentFoodLevel = parseInt(message.toString());
        console.log(`Cập nhật lượng thức ăn: ${currentFoodLevel}%`);
    }
});

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'index.html'));
});

app.get('/feed', (req, res) => {
    client.publish('molly_tank/control', 'FEED');
    res.send('Thành công');
});

// API mới để Web lấy % thức ăn
app.get('/food-level', (req, res) => {
    res.send(currentFoodLevel.toString());
});

app.listen(port, () => {
    console.log(`Web server chạy tại http://localhost:${port}`);
});