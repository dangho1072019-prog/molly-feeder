
'use strict';

// MOLLY CLOUD SYNC + ACS712
// Node.js 18+
// Luu lich va che do vao Upstash Redis.
// Nhan canh bao dong cao tu ESP32 qua MQTT.

const express = require('express');
const mqtt = require('mqtt');
const path = require('path');
const crypto = require('crypto');

const PORT = Number(process.env.PORT) || 3001;

const REDIS_URL =
  (process.env.UPSTASH_REDIS_REST_URL || '')
    .replace(/\/$/, '');

const REDIS_TOKEN =
  process.env.UPSTASH_REDIS_REST_TOKEN || '';

const REDIS_KEY = 'becamolly:settings:v2';

const MQTT_PREFIX =
  process.env.MOLLY_MQTT_PREFIX || 'molly_tank';

const MAX_SCHEDULES = 20;
const FEED_TIMEOUT_MS = 15000;
const ADMIN_PIN = process.env.MOLLY_ADMIN_PIN || '';

if (!REDIS_URL.startsWith('https://') ||
    !REDIS_TOKEN) {
  console.error('THIEU CAU HINH UPSTASH REDIS.');
  process.exit(1);
}

const TOPIC = {
  control: `${MQTT_PREFIX}/control`,
  result: `${MQTT_PREFIX}/feed_result`,
  motor: `${MQTT_PREFIX}/motor_current`,
  food: `${MQTT_PREFIX}/food_level`,
  mode: `${MQTT_PREFIX}/mode`,
  setTime: `${MQTT_PREFIX}/set_time`,
  schedules: `${MQTT_PREFIX}/schedules`
};

const app = express();

app.disable('x-powered-by');
app.use(express.json({ limit: '10kb' }));

app.use((req, res, next) => {
  res.set('Cache-Control', 'no-store');
  next();
});

// ===== KIEM TRA GIO =====
function validTime(t) {
  return typeof t === 'string' &&
    /^([01]\d|2[0-3]):[0-5]\d$/.test(t);
}

// ===== DU LIEU REDIS =====
function normalize(value) {
  if (!value ||
      typeof value !== 'object' ||
      !Array.isArray(value.schedules) ||
      !['AUTO', 'MANUAL'].includes(value.mode) ||
      value.schedules.length > MAX_SCHEDULES ||
      !value.schedules.every(validTime)) {
    throw new Error(
      'Du lieu Redis khong hop le - khong ghi de.'
    );
  }

  return {
    mode: value.mode,
    schedules: [...new Set(value.schedules)].sort(),
    revision:
      Number.isSafeInteger(value.revision) &&
      value.revision >= 0
        ? value.revision
        : 0
  };
}

async function redisCommand(...parts) {
  const response = await fetch(REDIS_URL, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${REDIS_TOKEN}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(parts),
    signal: AbortSignal.timeout(10000)
  });

  if (!response.ok) {
    throw new Error(`Upstash HTTP ${response.status}`);
  }

  const payload = await response.json();

  if (payload.error) {
    throw new Error(`Upstash: ${payload.error}`);
  }

  return payload.result;
}

// ===== TRANG THAI CLOUD =====
let settings;
let client;
let writeQueue = Promise.resolve();

let foodLevel = null;
let foodUpdatedAt = null;

let motorStatus = null;
let motorUpdatedAt = null;
let lastMotorId = null;

let activitySeq = 0;

const activities = [];
const pendingFeeds = new Map();
const confirmedIds = new Set();

// ===== GUI CAU HINH CHO ESP32 =====
function sendSettingsToEsp() {
  if (!client || !client.connected) {
    return;
  }

  // '-' de xoa toan bo lich ma khong xoa retained topic.
  client.publish(
    TOPIC.schedules,
    settings.schedules.length
      ? settings.schedules.join(',')
      : '-',
    { qos: 1, retain: true }
  );

  client.publish(
    TOPIC.mode,
    settings.mode,
    { qos: 1, retain: true }
  );
}

// ===== LUU CHE DO / LICH =====
function updateSettings(mutator) {
  const work = writeQueue.then(async () => {
    const draft = {
      mode: settings.mode,
      schedules: [...settings.schedules],
      revision: settings.revision
    };

    const next = mutator(draft);

    if (next.mode === settings.mode &&
        JSON.stringify(next.schedules) ===
        JSON.stringify(settings.schedules)) {
      return settings;
    }

    next.revision = settings.revision + 1;

    await redisCommand(
      'SET',
      REDIS_KEY,
      JSON.stringify(next)
    );

    settings = next;
    sendSettingsToEsp();

    console.log(
      'Saved Redis revision:',
      settings.revision
    );

    return settings;
  });

  writeQueue = work.catch(err => {
    console.error(
      'Update rejected:',
      err.message
    );
  });

  return work;
}

// ===== NHAT KY =====
function addActivity(event) {
  const result = {
    seq: ++activitySeq,
    ...event
  };

  activities.push(result);

  if (activities.length > 100) {
    activities.shift();
  }

  return result;
}

// ===== NHAN MQTT =====
function onMqttMessage(topic, raw, packet) {
  const text = raw.toString().trim();

  // ---- MUC THUC AN ----
  if (topic === TOPIC.food) {
    const value = Number(text);

    if (text &&
        Number.isInteger(value) &&
        value >= 0 &&
        value <= 100) {
      foodLevel = value;
      foodUpdatedAt = Date.now();
    }

    return;
  }

  // ---- GIAM SAT DONG DIEN ACS712 ----
  if (topic === TOPIC.motor) {
    let data;

    try {
      data = JSON.parse(text);
    } catch {
      return;
    }

    const allowedStates = [
      'NORMAL',
      'WATCH',
      'HIGH_CURRENT_RISK',
      'BENCH_TEST',
      'SENSOR_FAULT',
      'ROTATION_TIMEOUT',
      'BUSY'
    ];

    if (!data ||
        typeof data.id !== 'string' ||
        !/^[a-zA-Z0-9_-]{1,80}$/.test(data.id) ||
        !allowedStates.includes(data.state) ||
        !Number.isFinite(data.current_mA) ||
        !Number.isFinite(data.rolling_max_mA) ||
        !Number.isFinite(data.vcc_mV) ||
        data.current_mA < 0 ||
        data.current_mA > 5000 ||
        data.rolling_max_mA < 0 ||
        data.rolling_max_mA > 5000 ||
        data.vcc_mV < 0 ||
        data.vcc_mV > 5500) {
      return;
    }

    // Khong ghi nhat ky lap cho cung ID.
    if (data.id === lastMotorId) {
      return;
    }

    lastMotorId = data.id;

    motorStatus = {
      id: data.id,
      source:
        data.source === 'AUTO' ? 'AUTO' : 'MANUAL',
      state: data.state,
      current_mA: data.current_mA,
      rolling_max_mA: data.rolling_max_mA,
      vcc_mV: data.vcc_mV,
      threshold_mA:
        Number.isFinite(data.threshold_mA)
          ? data.threshold_mA
          : 280,
      samples:
        Number.isInteger(data.samples)
          ? data.samples
          : null,
      time: validTime(data.time) ? data.time : ''
    };

    motorUpdatedAt = Date.now();

    if (data.state === 'HIGH_CURRENT_RISK') {
      addActivity({
        type: 'motor_warning',
        id: data.id,
        state: data.state,
        rolling_max_mA: data.rolling_max_mA,
        threshold_mA: motorStatus.threshold_mA,
        receivedAt: new Date().toISOString()
      });
    }

    return;
  }

  // ---- KET QUA CHO AN ----
  if (topic === TOPIC.result) {
    let data;

    try {
      data = JSON.parse(text);
    } catch {
      return;
    }

    if (!data ||
        typeof data.id !== 'string' ||
        !/^[a-zA-Z0-9_-]{1,80}$/.test(data.id) ||
        !['DONE', 'ERROR'].includes(data.status)) {
      return;
    }

    if (confirmedIds.has(data.id)) {
      return;
    }

    confirmedIds.add(data.id);

    if (confirmedIds.size > 500) {
      confirmedIds.clear();
    }

    const event = addActivity({
      type: 'feed',
      id: data.id,
      status: data.status,
      source:
        data.source === 'AUTO' ? 'AUTO' : 'MANUAL',
      time: validTime(data.time) ? data.time : '',
      receivedAt: new Date().toISOString()
    });

    const pending = pendingFeeds.get(data.id);

    if (pending) {
      clearTimeout(pending.timer);
      pendingFeeds.delete(data.id);

      pending.resolve({
        ok: data.status === 'DONE',
        event
      });
    }

    return;
  }

  // Redis la nguon chinh cho lich.
  // Bo qua ban tin retained cu tu MQTT.
  if (packet && packet.retain) {
    return;
  }

  // ---- DOI CHE DO TU APP KHAC ----
  if (topic === TOPIC.mode) {
    if (!['AUTO', 'MANUAL'].includes(text) ||
        settings.mode === text) {
      return;
    }

    updateSettings(draft => {
      draft.mode = text;
      return draft;
    }).catch(err => {
      console.error('MQTT mode error:', err.message);
    });
  }

  // ---- THEM GIO TU APP KHAC ----
  else if (topic === TOPIC.setTime) {
    if (!validTime(text) ||
        settings.schedules.includes(text)) {
      return;
    }

    updateSettings(draft => {
      if (!draft.schedules.includes(text) &&
          draft.schedules.length < MAX_SCHEDULES) {
        draft.schedules.push(text);
        draft.schedules.sort();
      }

      return draft;
    }).catch(err => {
      console.error('MQTT time error:', err.message);
    });
  }
}

// ===== XU LY LOI =====
function errorResponse(res, error) {
  console.error('Request failed:', error.message);

  return res.status(503).json({
    ok: false,
    message:
      'Không lưu được dữ liệu Cloud. Lịch cũ vẫn được giữ.'
  });
}

// ===== MA PIN TUY CHON =====
function requirePin(req, res, next) {
  if (!ADMIN_PIN) {
    return next();
  }

  const submitted = req.get('x-molly-pin') || '';

  const a = Buffer.from(submitted);
  const b = Buffer.from(ADMIN_PIN);

  if (a.length !== b.length ||
      !crypto.timingSafeEqual(a, b)) {
    return res.status(401).json({
      ok: false,
      message: 'Mã PIN điều khiển không đúng.'
    });
  }

  next();
}

// ===== TRANG THAI GUI CHO WEB =====
function status() {
  return {
    mode: settings.mode,
    time: settings.schedules[0] || '',
    schedules: settings.schedules,
    revision: settings.revision,

    foodLevel,
    foodLevelAt: foodUpdatedAt,

    motor: motorStatus,
    motorUpdatedAt,

    mqttConnected: !!(client && client.connected),
    storage: 'upstash'
  };
}

// ===== API DOC DU LIEU =====
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

app.get('/api/status', (req, res) => {
  res.json(status());
});

app.get('/status', (req, res) => {
  res.json(status());
});

app.get('/food-level', (req, res) => {
  res.send(
    foodLevel == null ? '--' : String(foodLevel)
  );
});

app.get('/api/activity', (req, res) => {
  const after = Number(req.query.after) || 0;

  res.json({
    events: activities.filter(x => x.seq > after)
  });
});

// ===== API THEM LICH =====
app.post('/api/schedules', requirePin, async (req, res) => {
  const time = req.body && req.body.time;

  if (!validTime(time)) {
    return res.status(400).json({
      ok: false,
      message: 'Giờ không hợp lệ (HH:MM).'
    });
  }

  try {
    const next = await updateSettings(draft => {
      if (draft.schedules.includes(time)) {
        throw new Error('DUPLICATE');
      }

      if (draft.schedules.length >= MAX_SCHEDULES) {
        throw new Error('LIMIT');
      }

      draft.schedules.push(time);
      draft.schedules.sort();
      return draft;
    });

    return res.status(201).json({
      ok: true,
      schedules: next.schedules,
      revision: next.revision,
      mqttConnected: !!client.connected
    });
  } catch (err) {
    if (err.message === 'DUPLICATE') {
      return res.status(409).json({
        ok: false,
        message: 'Giờ đã tồn tại.'
      });
    }

    if (err.message === 'LIMIT') {
      return res.status(400).json({
        ok: false,
        message: 'Tối đa 20 giờ.'
      });
    }

    return errorResponse(res, err);
  }
});

// ===== API XOA LICH =====
app.delete(
  '/api/schedules/:time',
  requirePin,
  async (req, res) => {
    const time = req.params.time;

    if (!validTime(time)) {
      return res.status(400).json({
        ok: false,
        message: 'Giờ không hợp lệ.'
      });
    }

    try {
      const next = await updateSettings(draft => {
        if (!draft.schedules.includes(time)) {
          throw new Error('NOT_FOUND');
        }

        draft.schedules =
          draft.schedules.filter(x => x !== time);

        return draft;
      });

      res.json({
        ok: true,
        schedules: next.schedules,
        revision: next.revision,
        mqttConnected: !!client.connected
      });
    } catch (err) {
      if (err.message === 'NOT_FOUND') {
        return res.status(404).json({
          ok: false,
          message: 'Không tìm thấy giờ.'
        });
      }

      return errorResponse(res, err);
    }
  }
);

// ===== API CHUYEN CHE DO =====
app.post('/api/mode', requirePin, async (req, res) => {
  const selectedMode =
    String((req.body && req.body.mode) || '')
      .toUpperCase();

  if (!['AUTO', 'MANUAL'].includes(selectedMode)) {
    return res.status(400).json({
      ok: false,
      message: 'Chế độ không hợp lệ.'
    });
  }

  try {
    const next = await updateSettings(draft => {
      draft.mode = selectedMode;
      return draft;
    });

    res.json({
      ok: true,
      mode: next.mode,
      revision: next.revision,
      mqttConnected: !!client.connected
    });
  } catch (err) {
    return errorResponse(res, err);
  }
});

// ===== API CHO AN =====
async function feed(req, res) {
  if (!client.connected) {
    return res.status(503).json({
      ok: false,
      message: 'MQTT chưa kết nối.'
    });
  }

  if (settings.mode === 'AUTO') {
    return res.status(409).json({
      ok: false,
      message:
        'Chuyển sang Thủ công trước khi cho ăn ngay.'
    });
  }

  const id = crypto.randomUUID();

  const confirm = new Promise(resolve => {
    const timer = setTimeout(() => {
      pendingFeeds.delete(id);

      resolve({
        ok: false,
        message:
          'Chưa nhận xác nhận trong 15 giây. Kiểm tra ESP32 trước khi thử lại để tránh xả lặp.'
      });
    }, FEED_TIMEOUT_MS);

    pendingFeeds.set(id, { resolve, timer });
  });

  client.publish(
    TOPIC.control,
    'FEED:' + id,
    { qos: 1 },
    err => {
      if (!err) {
        return;
      }

      const pending = pendingFeeds.get(id);

      if (pending) {
        clearTimeout(pending.timer);
        pendingFeeds.delete(id);

        pending.resolve({
          ok: false,
          message: 'Không gửi được lệnh MQTT.'
        });
      }
    }
  );

  const result = await confirm;

  if (!result.ok) {
    return res.status(504).json({
      ok: false,
      id,
      message: result.message || 'ESP32 báo lỗi.'
    });
  }

  res.json({
    ok: true,
    id,
    event: result.event,
    message:
      'ESP32 báo động cơ đã hoàn thành số bước.'
  });
}

app.post('/api/feed', requirePin, feed);

// ===== API TUONG THICH WEB/APP CU =====
app.get('/feed', requirePin, feed);

app.get(
  '/set-mode/:mode',
  requirePin,
  async (req, res) => {
    const selected =
      String(req.params.mode).toUpperCase();

    if (!['AUTO', 'MANUAL'].includes(selected)) {
      return res.status(400).send(
        'Chế độ không hợp lệ.'
      );
    }

    try {
      await updateSettings(draft => {
        draft.mode = selected;
        return draft;
      });

      res.send('Đã đổi chế độ: ' + selected);
    } catch (err) {
      res.status(503).send(
        'Không lưu được lên Cloud.'
      );
    }
  }
);

app.get(
  '/set-time/:time',
  requirePin,
  async (req, res) => {
    const time =
      req.params.time.replace('-', ':');

    if (!validTime(time)) {
      return res.status(400).send(
        'Giờ không hợp lệ.'
      );
    }

    try {
      await updateSettings(draft => {
        if (!draft.schedules.includes(time)) {
          if (draft.schedules.length >= MAX_SCHEDULES) {
            throw new Error('LIMIT');
          }

          draft.schedules.push(time);
          draft.schedules.sort();
        }

        return draft;
      });

      res.send('Đã lưu giờ: ' + time);
    } catch (err) {
      res.status(503).send(
        'Không lưu được lên Cloud.'
      );
    }
  }
);

// ===== KHOI DONG CLOUD =====
async function start() {
  const stored = await redisCommand(
    'GET', REDIS_KEY
  );

  if (stored === null) {
    const first = {
      mode: 'MANUAL',
      schedules: [],
      revision: 0
    };

    await redisCommand(
      'SET',
      REDIS_KEY,
      JSON.stringify(first)
    );

    settings = first;
    console.log('Created empty Cloud configuration.');
  }
  else {
    settings = normalize(JSON.parse(stored));

    console.log(
      'Restored settings from Upstash:',
      settings
    );
  }

  client = mqtt.connect(
    process.env.MOLLY_MQTT_URL ||
      'mqtt://broker.hivemq.com',
    {
      clientId:
        `molly_cloud_${
          crypto.randomBytes(6).toString('hex')
        }`,
      reconnectPeriod: 3000
    }
  );

  client.on('connect', () => {
    console.log('MQTT connected');

    client.subscribe(
      [
        TOPIC.food,
        TOPIC.mode,
        TOPIC.setTime,
        TOPIC.result,
        TOPIC.motor
      ],
      { qos: 1 },
      err => {
        if (err) {
          console.error(
            'MQTT subscribe error:',
            err.message
          );
        }
      }
    );

    sendSettingsToEsp();
  });

  client.on('message', onMqttMessage);

  client.on('error', err => {
    console.error('MQTT error:', err.message);
  });

  app.listen(PORT, () => {
    console.log(
      `Molly Cloud Sync listening on ${PORT}`
    );
  });
}

start().catch(err => {
  console.error(
    'UPSTASH LOI - KHONG KHOI DONG:',
    err.message
  );

  process.exit(1);
});
