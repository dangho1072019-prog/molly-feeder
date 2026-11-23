
'use strict';

const express = require('express');
const mqtt = require('mqtt');
const path = require('path');
const crypto = require('crypto');

// ================= CAU HINH =================

const PORT = Number(process.env.PORT) || 3001;

const REDIS_URL =
  (process.env.UPSTASH_REDIS_REST_URL || '')
    .replace(/\/$/, '');

const REDIS_TOKEN =
  process.env.UPSTASH_REDIS_REST_TOKEN || '';

const REDIS_KEY = 'becamolly:settings:v2';

const MQTT_PREFIX =
  process.env.MOLLY_MQTT_PREFIX || 'molly_tank';

const ADMIN_PIN =
  process.env.MOLLY_ADMIN_PIN || '';

const MAX_SCHEDULES = 20;
const FEED_TIMEOUT_MS = 15000;

// Sau 20 giay khong co heartbeat: OFFLINE.
const ESP_OFFLINE_AFTER_MS = 20000;

if (!REDIS_URL.startsWith('https://') ||
    !REDIS_TOKEN) {
  console.error('THIEU CAU HINH UPSTASH REDIS');
  process.exit(1);
}

// ================= MQTT TOPICS =================

const TOPIC = {
  control: `${MQTT_PREFIX}/control`,
  result: `${MQTT_PREFIX}/feed_result`,
  motor: `${MQTT_PREFIX}/motor_current`,
  device: `${MQTT_PREFIX}/device_status`,
  food: `${MQTT_PREFIX}/food_level`,
  mode: `${MQTT_PREFIX}/mode`,
  setTime: `${MQTT_PREFIX}/set_time`,
  schedules: `${MQTT_PREFIX}/schedules`
};

// ================= EXPRESS =================

const app = express();

app.disable('x-powered-by');

app.use(express.json({ limit: '10kb' }));

app.use((req, res, next) => {
  res.set('Cache-Control', 'no-store');
  next();
});

// ================= BIEN HE THONG =================

let settings;
let client;
let writeQueue = Promise.resolve();

let foodLevel = null;
let foodUpdatedAt = null;

let motorStatus = null;
let motorUpdatedAt = null;
let lastMotorId = null;

// Trang thai ESP32 khac voi ket noi MQTT cua Render.
let lastEspHeartbeatAt = null;
let lastEspSignal = 'UNKNOWN';
let espObserveSince = Date.now();
let lastReportedEspState = 'UNKNOWN';

let activitySeq = 0;

const activities = [];
const pendingFeeds = new Map();
const confirmedIds = new Set();

// ================= KIEM TRA GIO =================

function validTime(t) {
  return (
    typeof t === 'string' &&
    /^([01]\d|2[0-3]):[0-5]\d$/.test(t)
  );
}

// ================= UPSTASH REDIS =================

function normalize(value) {
  if (!value ||
      typeof value !== 'object' ||
      !Array.isArray(value.schedules) ||
      !['AUTO', 'MANUAL'].includes(value.mode) ||
      value.schedules.length > MAX_SCHEDULES ||
      !value.schedules.every(validTime)) {
    throw new Error('Du lieu Redis khong hop le');
  }

  return {
    mode: value.mode,
    schedules:
      [...new Set(value.schedules)].sort(),
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
    throw new Error(
      `Upstash HTTP ${response.status}`
    );
  }

  const payload = await response.json();

  if (payload.error) {
    throw new Error(payload.error);
  }

  return payload.result;
}

// ================= DONG BO ESP32 =================

function sendSettingsToEsp() {
  if (!client || !client.connected) return;

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

// ================= CAP NHAT LICH / CHE DO =================

function updateSettings(mutator) {
  const work = writeQueue.then(async () => {
    const draft = {
      mode: settings.mode,
      schedules: [...settings.schedules],
      revision: settings.revision
    };

    const next = mutator(draft);

    if (
      next.mode === settings.mode &&
      JSON.stringify(next.schedules) ===
      JSON.stringify(settings.schedules)
    ) {
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
      'REDIS SAVED, REVISION:',
      settings.revision
    );

    return settings;
  });

  writeQueue = work.catch(err => {
    console.error('UPDATE ERROR:', err.message);
  });

  return work;
}

// ================= NHAT KY =================

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

// ================= TRANG THAI ESP32 =================

function getEspStatus() {
  // Render mat MQTT: khong xac dinh duoc ESP32.
  if (!client || !client.connected) {
    return 'UNKNOWN';
  }

  // Last Will hoac ban tin OFFLINE.
  if (lastEspSignal === 'OFFLINE') {
    return 'OFFLINE';
  }

  // Heartbeat moi trong 20 giay.
  if (lastEspSignal === 'ONLINE' &&
      lastEspHeartbeatAt !== null &&
      Date.now() - lastEspHeartbeatAt <=
      ESP_OFFLINE_AFTER_MS) {
    return 'ONLINE';
  }

  // Da tung nhan heartbeat nhung het han.
  if (lastEspHeartbeatAt !== null) {
    return 'OFFLINE';
  }

  // Render moi khoi dong: cho heartbeat dau tien.
  return (
    Date.now() - espObserveSince >
    ESP_OFFLINE_AFTER_MS
  )
    ? 'OFFLINE'
    : 'UNKNOWN';
}

function syncEspStatusEvent() {
  const state = getEspStatus();

  if (state === lastReportedEspState) {
    return;
  }

  const previous = lastReportedEspState;
  lastReportedEspState = state;

  if (
    state === 'OFFLINE' ||
    (state === 'ONLINE' && previous === 'OFFLINE')
  ) {
    addActivity({
      type: 'device_status',
      state,
      receivedAt: new Date().toISOString()
    });
  }
}

// ================= NHAN DU LIEU MQTT =================

function onMqttMessage(topic, raw, packet) {
  const text = raw.toString().trim();

  // ----- ESP32 ONLINE / OFFLINE -----

  if (topic === TOPIC.device) {
    if (text === 'OFFLINE') {
      lastEspSignal = 'OFFLINE';
    }
    else if (
      text === 'ONLINE' &&
      !(packet && packet.retain)
    ) {
      // Khong lay ONLINE retained cu lam heartbeat.
      lastEspHeartbeatAt = Date.now();
      lastEspSignal = 'ONLINE';
    }

    syncEspStatusEvent();
    return;
  }

  // ----- MUC THUC AN -----

  if (topic === TOPIC.food) {
    const number = Number(text);

    if (
      text !== '' &&
      Number.isInteger(number) &&
      number >= 0 &&
      number <= 100
    ) {
      foodLevel = number;
      foodUpdatedAt = Date.now();
    }

    return;
  }

  // ----- ACS712 -----

  if (topic === TOPIC.motor) {
    let data;

    try {
      data = JSON.parse(text);
    }
    catch {
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

    if (
      !data ||
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
      data.vcc_mV > 5500
    ) {
      return;
    }

    // Khong ghi lap cung mot lan do.
    if (data.id === lastMotorId) return;

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
      time:
        validTime(data.time)
          ? data.time
          : ''
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

  // ----- KET QUA CHO AN -----

  if (topic === TOPIC.result) {
    let data;

    try {
      data = JSON.parse(text);
    }
    catch {
      return;
    }

    if (
      !data ||
      typeof data.id !== 'string' ||
      !/^[a-zA-Z0-9_-]{1,80}$/.test(data.id) ||
      !['DONE', 'ERROR'].includes(data.status)
    ) {
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
      time:
        validTime(data.time) ? data.time : '',
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

  // Redis la nguon chinh cua lich va che do.
  // Bo qua cac tin retained cu.
  if (packet && packet.retain) return;

  // ----- DOI CHE DO -----

  if (topic === TOPIC.mode) {
    if (
      !['AUTO', 'MANUAL'].includes(text) ||
      settings.mode === text
    ) {
      return;
    }

    updateSettings(draft => {
      draft.mode = text;
      return draft;
    }).catch(err => {
      console.error('MODE ERROR:', err.message);
    });
  }

  // ----- THEM LICH TU APP KHAC -----

  else if (topic === TOPIC.setTime) {
    if (
      !validTime(text) ||
      settings.schedules.includes(text)
    ) {
      return;
    }

    updateSettings(draft => {
      if (
        !draft.schedules.includes(text) &&
        draft.schedules.length < MAX_SCHEDULES
      ) {
        draft.schedules.push(text);
        draft.schedules.sort();
      }

      return draft;
    }).catch(err => {
      console.error('TIME ERROR:', err.message);
    });
  }
}

// ================= MA PIN TUY CHON =================

function requirePin(req, res, next) {
  if (!ADMIN_PIN) return next();

  const submitted =
    req.get('x-molly-pin') || '';

  const a = Buffer.from(submitted);
  const b = Buffer.from(ADMIN_PIN);

  if (
    a.length !== b.length ||
    !crypto.timingSafeEqual(a, b)
  ) {
    return res.status(401).json({
      ok: false,
      message: 'Mã PIN điều khiển không đúng.'
    });
  }

  next();
}

function errorResponse(res, error) {
  console.error('REQUEST ERROR:', error.message);

  return res.status(503).json({
    ok: false,
    message:
      'Không lưu được dữ liệu Cloud. ' +
      'Lịch cũ vẫn được giữ.'
  });
}

// ================= API TRANG THAI =================

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

    espStatus: getEspStatus(),
    espLastSeenAt: lastEspHeartbeatAt,

    mqttConnected:
      !!(client && client.connected),

    storage: 'upstash'
  };
}

app.get('/', (req, res) => {
  res.sendFile(
    path.join(__dirname, 'index.html')
  );
});

app.get('/api/status', (req, res) => {
  res.json(status());
});

app.get('/status', (req, res) => {
  res.json(status());
});

app.get('/food-level', (req, res) => {
  res.send(
    foodLevel === null
      ? '--'
      : String(foodLevel)
  );
});

app.get('/api/activity', (req, res) => {
  const after =
    Number(req.query.after) || 0;

  res.json({
    events:
      activities.filter(x => x.seq > after)
  });
});

// ================= THEM LICH =================

app.post(
  '/api/schedules',
  requirePin,
  async (req, res) => {
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

        if (
          draft.schedules.length >= MAX_SCHEDULES
        ) {
          throw new Error('LIMIT');
        }

        draft.schedules.push(time);
        draft.schedules.sort();

        return draft;
      });

      res.status(201).json({
        ok: true,
        schedules: next.schedules,
        revision: next.revision,
        mqttConnected: !!client.connected
      });
    }
    catch (err) {
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
  }
);

// ================= XOA LICH =================

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
    }
    catch (err) {
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

// ================= CHUYEN CHE DO =================

app.post(
  '/api/mode',
  requirePin,
  async (req, res) => {
    const selected =
      String(
        (req.body && req.body.mode) || ''
      ).toUpperCase();

    if (!['AUTO', 'MANUAL'].includes(selected)) {
      return res.status(400).json({
        ok: false,
        message: 'Chế độ không hợp lệ.'
      });
    }

    try {
      const next = await updateSettings(draft => {
        draft.mode = selected;
        return draft;
      });

      res.json({
        ok: true,
        mode: next.mode,
        revision: next.revision,
        mqttConnected: !!client.connected
      });
    }
    catch (err) {
      return errorResponse(res, err);
    }
  }
);

// ================= CHO AN THU CONG =================

async function feed(req, res) {
  if (!client.connected) {
    return res.status(503).json({
      ok: false,
      message: 'MQTT chưa kết nối.'
    });
  }

  if (getEspStatus() !== 'ONLINE') {
    return res.status(503).json({
      ok: false,
      message:
        'ESP32 chưa trực tuyến. ' +
        'Hãy kiểm tra Wi-Fi và nguồn trước.'
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
          'Chưa nhận xác nhận trong 15 giây. ' +
          'Kiểm tra ESP32 trước khi thử lại.'
      });
    }, FEED_TIMEOUT_MS);

    pendingFeeds.set(id, {
      resolve,
      timer
    });
  });

  client.publish(
    TOPIC.control,
    'FEED:' + id,
    { qos: 1 },
    err => {
      if (!err) return;

      const pending = pendingFeeds.get(id);

      if (pending) {
        clearTimeout(pending.timer);
        pendingFeeds.delete(id);

        pending.resolve({
          ok: false,
          message:
            'Không gửi được lệnh MQTT.'
        });
      }
    }
  );

  const result = await confirm;

  if (!result.ok) {
    return res.status(504).json({
      ok: false,
      id,
      message:
        result.message || 'ESP32 báo lỗi.'
    });
  }

  res.json({
    ok: true,
    id,
    event: result.event,
    message:
      'ESP32 đã phát đủ lệnh bước.'
  });
}

app.post('/api/feed', requirePin, feed);

// ================= API TUONG THICH =================

// Giữ tương thích với các ứng dụng cũ.
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
    }
    catch {
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
    }
    catch {
      res.status(503).send(
        'Không lưu được lên Cloud.'
      );
    }
  }
);

// ================= KHOI DONG =================

async function start() {
  const stored = await redisCommand(
    'GET',
    REDIS_KEY
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

    console.log(
      'Khoi tao cau hinh Cloud moi.'
    );
  }
  else {
    settings =
      normalize(JSON.parse(stored));

    console.log(
      'Da khoi phuc lich tu Redis:',
      settings
    );
  }

  client = mqtt.connect(
    process.env.MOLLY_MQTT_URL ||
      'mqtt://broker.hivemq.com',
    {
      clientId:
        'molly_cloud_' +
        crypto.randomBytes(6).toString('hex'),
      reconnectPeriod: 3000
    }
  );

  client.on('connect', () => {
    espObserveSince = Date.now();

    console.log('MQTT CONNECTED');

    client.subscribe(
      [
        TOPIC.food,
        TOPIC.mode,
        TOPIC.setTime,
        TOPIC.result,
        TOPIC.motor,
        TOPIC.device
      ],
      { qos: 1 },
      err => {
        if (err) {
          console.error(
            'MQTT SUBSCRIBE ERROR:',
            err.message
          );
        }
      }
    );

    sendSettingsToEsp();
  });

  client.on('message', onMqttMessage);

  client.on('error', err => {
    console.error('MQTT ERROR:', err.message);
  });

  client.on('close', () => {
    syncEspStatusEvent();
  });

  // Kiem tra timeout heartbeat 2 giay mot lan.
  setInterval(syncEspStatusEvent, 2000);

  app.listen(PORT, () => {
    console.log(
      `Molly Cloud running on port ${PORT}`
    );
  });
}

start().catch(err => {
  console.error(
    'KHONG DOC DUOC UPSTASH:',
    err.message
  );

  process.exit(1);
});
