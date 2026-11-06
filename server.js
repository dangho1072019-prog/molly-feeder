
const express = require('express');
const mqtt = require('mqtt');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');

const app = express();
const PORT = process.env.PORT || 3001;
const STATE_FILE = path.join(__dirname, 'molly-state.json');
const MAX_SCHEDULES = 20;
const FEED_TIMEOUT_MS = 15000;

const TOPIC = {
  control: 'molly_tank/control',
  result: 'molly_tank/feed_result',
  food: 'molly_tank/food_level',
  mode: 'molly_tank/mode',
  setTime: 'molly_tank/set_time',
  schedules: 'molly_tank/schedules'
};

function validTime(value) {
  return typeof value === 'string' &&
    /^([01]\d|2[0-3]):[0-5]\d$/.test(value);
}

function cleanSchedules(values) {
  if (!Array.isArray(values)) return [];

  return [...new Set(values.filter(validTime))]
    .sort()
    .slice(0, MAX_SCHEDULES);
}

function readState() {
  try {
    const data = JSON.parse(
      fs.readFileSync(STATE_FILE, 'utf8')
    );

    return {
      schedules: cleanSchedules(data.schedules),
      mode: data.mode === 'AUTO' ? 'AUTO' : 'MANUAL'
    };
  } catch (err) {
    return {
      schedules: ['08:00'],
      mode: 'MANUAL'
    };
  }
}

const saved = readState();

let schedules = saved.schedules;
let currentMode = saved.mode;
let currentFoodLevel = null;
let lastFoodLevelAt = null;
let activitySeq = 0;

const activities = [];
const pendingFeeds = new Map();
const handledResultIds = new Set();

function saveState() {
  fs.writeFileSync(
    STATE_FILE,
    JSON.stringify({
      schedules,
      mode: currentMode
    }, null, 2) + '\n'
  );
}

function publishSchedules() {
  if (client.connected) {
    client.publish(
      TOPIC.schedules,
      schedules.length ? schedules.join(',') : '-',
      { qos: 1, retain: true }
    );
  }
}

function addSchedule(time) {
  if (!validTime(time)) {
    return {
      error: 'Giờ phải đúng định dạng HH:MM (00:00–23:59).'
    };
  }

  if (schedules.includes(time)) {
    return { error: 'Giờ này đã tồn tại.' };
  }

  if (schedules.length >= MAX_SCHEDULES) {
    return {
      error: `Chỉ được đặt tối đa ${MAX_SCHEDULES} giờ.`
    };
  }

  schedules = cleanSchedules([...schedules, time]);

  saveState();
  publishSchedules();

  return { schedules };
}

function deleteSchedule(time) {
  if (!validTime(time) || !schedules.includes(time)) {
    return false;
  }

  schedules = schedules.filter(t => t !== time);

  saveState();
  publishSchedules();

  return true;
}

const client = mqtt.connect('mqtt://broker.hivemq.com', {
  clientId:
    `molly_web_${crypto.randomBytes(5).toString('hex')}`,
  reconnectPeriod: 3000
});

client.on('connect', () => {
  console.log('Node.js đã kết nối MQTT Broker');

  client.subscribe(
    [TOPIC.food, TOPIC.mode, TOPIC.setTime, TOPIC.result],
    { qos: 1 },
    err => {
      if (err) {
        console.error('Lỗi subscribe:', err.message);
      }
    }
  );

  publishSchedules();

  client.publish(
    TOPIC.mode,
    currentMode,
    { qos: 1, retain: true }
  );
});

client.on('error', err => {
  console.error('MQTT:', err.message);
});

function addActivity(event) {
  const item = {
    seq: ++activitySeq,
    ...event
  };

  activities.push(item);

  if (activities.length > 100) {
    activities.shift();
  }

  return item;
}

client.on('message', (topic, message) => {
  const text = message.toString().trim();

  if (topic === TOPIC.food) {
    const value = Number(text);

    if (
      text !== '' &&
      Number.isFinite(value) &&
      Number.isInteger(value) &&
      value >= 0 &&
      value <= 100
    ) {
      currentFoodLevel = value;
      lastFoodLevelAt = Date.now();
    }

  } else if (topic === TOPIC.mode) {
    const next = text.toUpperCase();

    if (
      (next === 'MANUAL' || next === 'AUTO') &&
      next !== currentMode
    ) {
      currentMode = next;
      saveState();

      client.publish(
        TOPIC.mode,
        currentMode,
        { qos: 1, retain: true }
      );
    }

  } else if (topic === TOPIC.setTime) {
    if (validTime(text) && !schedules.includes(text)) {
      addSchedule(text);
    }

  } else if (topic === TOPIC.result) {
    let data;

    try {
      data = JSON.parse(text);
    } catch {
      return;
    }

    if (
      !data ||
      typeof data.id !== 'string' ||
      !/^[a-zA-Z0-9_-]{1,80}$/.test(data.id)
    ) {
      return;
    }

    if (
      data.status !== 'DONE' &&
      data.status !== 'ERROR'
    ) {
      return;
    }

    if (handledResultIds.has(data.id)) return;

    handledResultIds.add(data.id);

    if (handledResultIds.size > 500) {
      handledResultIds.clear();
    }

    const result = addActivity({
      type: 'feed',
      id: data.id,
      status: data.status,
      source: data.source === 'AUTO' ? 'AUTO' : 'MANUAL',
      time: validTime(data.time) ? data.time : '',
      receivedAt: new Date().toISOString()
    });

    const pending = pendingFeeds.get(data.id);

    if (pending) {
      clearTimeout(pending.timeout);
      pendingFeeds.delete(data.id);

      pending.resolve({
        ok: data.status === 'DONE',
        event: result
      });
    }
  }
});

app.use(express.json());

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

function snapshot() {
  return {
    mode: currentMode,
    time: schedules[0] || '',
    schedules,
    foodLevel: currentFoodLevel,
    foodLevelAt: lastFoodLevelAt,
    mqttConnected: client.connected
  };
}

app.get('/status', (req, res) => {
  res.json(snapshot());
});

app.get('/api/status', (req, res) => {
  res.json(snapshot());
});

app.get('/food-level', (req, res) => {
  res.send(
    currentFoodLevel === null ?
      '--' : String(currentFoodLevel)
  );
});

app.get('/api/activity', (req, res) => {
  const after = Number(req.query.after) || 0;

  res.json({
    events: activities.filter(item => item.seq > after)
  });
});

app.post('/api/schedules', (req, res) => {
  const time = req.body && req.body.time;
  const result = addSchedule(time);

  if (result.error) {
    return res.status(400).json({
      ok: false,
      message: result.error
    });
  }

  res.status(201).json({
    ok: true,
    schedules,
    mqttConnected: client.connected
  });
});

app.delete('/api/schedules/:time', (req, res) => {
  if (!deleteSchedule(req.params.time)) {
    return res.status(404).json({
      ok: false,
      message: 'Không tìm thấy giờ hẹn này.'
    });
  }

  res.json({
    ok: true,
    schedules,
    mqttConnected: client.connected
  });
});

app.post('/api/mode', (req, res) => {
  const mode = String(
    (req.body && req.body.mode) || ''
  ).toUpperCase();

  if (mode !== 'AUTO' && mode !== 'MANUAL') {
    return res.status(400).json({
      ok: false,
      message: 'Chế độ không hợp lệ.'
    });
  }

  currentMode = mode;
  saveState();

  if (client.connected) {
    client.publish(
      TOPIC.mode,
      currentMode,
      { qos: 1, retain: true }
    );
  }

  res.json({
    ok: true,
    mode: currentMode,
    mqttConnected: client.connected
  });
});

async function requestFeed(req, res) {
  if (!client.connected) {
    return res.status(503).json({
      ok: false,
      message:
        'Server chưa kết nối MQTT; chưa thể gửi lệnh.'
    });
  }

  const id = crypto.randomUUID();

  const confirmation = new Promise(resolve => {
    const timeout = setTimeout(() => {
      pendingFeeds.delete(id);

      resolve({
        ok: false,
        message:
          'Chưa nhận được xác nhận từ ESP32 trong 15 giây. Kiểm tra ESP32 trước khi thử lại để tránh cho ăn lặp.'
      });

    }, FEED_TIMEOUT_MS);

    pendingFeeds.set(id, { resolve, timeout });
  });

  client.publish(
    TOPIC.control,
    `FEED:${id}`,
    { qos: 1 },
    err => {
      if (err) {
        const pending = pendingFeeds.get(id);

        if (pending) {
          clearTimeout(pending.timeout);
          pendingFeeds.delete(id);

          pending.resolve({
            ok: false,
            message: 'Không gửi được lệnh MQTT.'
          });
        }
      }
    }
  );

  const result = await confirmation;

  if (!result.ok) {
    return res.status(504).json({
      ok: false,
      id,
      message: result.message ||
        'ESP32 báo không hoàn thành lệnh.'
    });
  }

  res.json({
    ok: true,
    id,
    message: 'ESP32 đã xác nhận động cơ chạy xong.',
    event: result.event
  });
}

app.post('/api/feed', requestFeed);

app.get('/feed', requestFeed);

app.get('/set-mode/:mode', (req, res) => {
  const mode = String(req.params.mode).toUpperCase();

  if (mode !== 'AUTO' && mode !== 'MANUAL') {
    return res.status(400).send(
      'Chế độ không hợp lệ'
    );
  }

  currentMode = mode;
  saveState();

  if (client.connected) {
    client.publish(
      TOPIC.mode,
      mode,
      { qos: 1, retain: true }
    );
  }

  res.send('Đã đổi chế độ: ' + mode);
});

app.get('/set-time/:time', (req, res) => {
  const time = req.params.time.replace('-', ':');
  const result = addSchedule(time);

  if (result.error) {
    return res.status(400).send(result.error);
  }

  res.send('Đã thêm giờ: ' + time);
});

app.listen(PORT, () => {
  console.log(`Web server: http://localhost:${PORT}`);
});
