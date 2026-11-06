
'use strict';

// MOLLY CLOUD SYNC
// Luu lich va che do vao Upstash Redis.
// Tach MQTT topic nhan lenh va topic trang thai.

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

const ADMIN_PIN =
  process.env.MOLLY_ADMIN_PIN || '';

if (!REDIS_URL.startsWith('https://') || !REDIS_TOKEN) {
  console.error(
    'THIEU UPSTASH_REDIS_REST_URL / UPSTASH_REDIS_REST_TOKEN.'
  );

  process.exit(1);
}

// QUAN TRONG:
// mode la trang thai GUI XUONG ESP32.
// modeSet la lenh NHIEN TU APP khac.
const TOPIC = {
  control: `${MQTT_PREFIX}/control`,
  result: `${MQTT_PREFIX}/feed_result`,
  food: `${MQTT_PREFIX}/food_level`,
  mode: `${MQTT_PREFIX}/mode`,
  modeSet: `${MQTT_PREFIX}/mode_set`,
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

// KIEM TRA GIO HH:MM
function validTime(t) {
  return typeof t === 'string' &&
    /^([01]\d|2[0-3]):[0-5]\d$/.test(t);
}

// CHUAN HOA DU LIEU REDIS
function normalize(value) {
  if (
    !value ||
    typeof value !== 'object' ||
    !Array.isArray(value.schedules) ||
    (value.mode !== 'AUTO' && value.mode !== 'MANUAL') ||
    value.schedules.length > MAX_SCHEDULES ||
    !value.schedules.every(validTime)
  ) {
    throw new Error(
      'Du lieu luu trong Redis khong hop le.'
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

// GOI REDIS REST API
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
    throw new Error(`Upstash: ${payload.error}`);
  }

  return payload.result;
}

// CAC BIEN TRANG THAI
let settings;
let client;
let writeQueue = Promise.resolve();

let foodLevel = null;
let foodUpdatedAt = null;
let activitySeq = 0;

const activities = [];
const pendingFeeds = new Map();
const confirmedIds = new Set();

// DONG BO CHE DO VA LICH XUONG ESP32
function sendSettingsToEsp() {
  if (!client || !client.connected) return;

  client.publish(
    TOPIC.schedules,
    settings.schedules.length
      ? settings.schedules.join(',')
      : '-',
    { qos: 1, retain: true }
  );

  // Chi GUI che do, khong nhan lai nhu lenh.
  client.publish(
    TOPIC.mode,
    settings.mode,
    { qos: 1, retain: true }
  );
}

// LUU THAY DOI VAO REDIS
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
      'Saved to Redis, revision:',
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

// NHAT KY HOAT DONG
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

// XU LY TIN NHAN MQTT
function onMqttMessage(topic, raw, packet) {
  const text = raw.toString().trim();

  // NHAN PHAN TRAM THUC AN
  if (topic === TOPIC.food) {
    const number = Number(text);

    if (
      text &&
      Number.isInteger(number) &&
      number >= 0 &&
      number <= 100
    ) {
      foodLevel = number;
      foodUpdatedAt = Date.now();
    }

    return;
  }

  // NHAN XAC NHAN DONG CO
  if (topic === TOPIC.result) {
    let data;

    try {
      data = JSON.parse(text);
    } catch {
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

    if (confirmedIds.has(data.id)) return;

    confirmedIds.add(data.id);

    if (confirmedIds.size > 500) {
      confirmedIds.clear();
    }

    const event = addActivity({
      type: 'feed',
      id: data.id,
      status: data.status,
      source:
        data.source === 'AUTO'
          ? 'AUTO'
          : 'MANUAL',
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

  // BO QUA LENH RETAINED CU.
  // REDIS LA NOI LUU TRANG THAI CHINH.
  if (packet && packet.retain) return;

  // CHI NHAN LENH DOI CHE DO QUA mode_set.
  // KHONG NHAN LAI TOPIC mode.
  if (topic === TOPIC.modeSet) {
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
      console.error(
        'MQTT mode not saved:',
        err.message
      );
    });

  } else if (topic === TOPIC.setTime) {

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
      console.error(
        'MQTT time not saved:',
        err.message
      );
    });
  }
}

// BAO LOI KHI KHONG LUU DUOC CLOUD
function errorResponse(res, error) {
  console.error(
    'Request failed:',
    error.message
  );

  return res.status(503).json({
    ok: false,
    message:
      'Không lưu được dữ liệu Cloud. Vui lòng thử lại; lịch cũ vẫn được giữ.'
  });
}

// XAC THUC MA PIN
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

// TRANG THAI HE THONG
function status() {
  return {
    mode: settings.mode,
    time: settings.schedules[0] || '',
    schedules: settings.schedules,
    revision: settings.revision,
    foodLevel,
    foodLevelAt: foodUpdatedAt,
    mqttConnected:
      !!(client && client.connected),
    storage: 'upstash'
  };
}

// GIAO DIEN WEB
app.get('/', (req, res) => {
  res.sendFile(
    path.join(__dirname, 'index.html')
  );
});

// DOC TRANG THAI
app.get('/api/status', (req, res) => {
  res.json(status());
});

app.get('/status', (req, res) => {
  res.json(status());
});

app.get('/food-level', (req, res) => {
  res.send(
    foodLevel == null
      ? '--'
      : String(foodLevel)
  );
});

// DOC NHAT KY
app.get('/api/activity', (req, res) => {
  const after = Number(req.query.after) || 0;

  res.json({
    events: activities.filter(
      x => x.seq > after
    )
  });
});

// THEM GIO CHO AN
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
  }
);

// XOA GIO CHO AN
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

// CHUYEN CHE DO THU CONG / TU DONG
app.post(
  '/api/mode',
  requirePin,
  async (req, res) => {
    const mode = String(
      (req.body && req.body.mode) || ''
    ).toUpperCase();

    if (!['AUTO', 'MANUAL'].includes(mode)) {
      return res.status(400).json({
        ok: false,
        message: 'Chế độ không hợp lệ.'
      });
    }

    try {
      const next = await updateSettings(draft => {
        draft.mode = mode;
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
  }
);

// LENH CHO AN THU CONG
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
      message:
        result.message || 'ESP32 báo lỗi.'
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

// API CHO AN
app.post('/api/feed', requirePin, feed);

// HO TRO DUONG DAN CU
app.get('/feed', requirePin, feed);

app.get(
  '/set-mode/:mode',
  requirePin,
  async (req, res) => {
    const mode =
      String(req.params.mode).toUpperCase();

    if (!['AUTO', 'MANUAL'].includes(mode)) {
      return res.status(400).send(
        'Chế độ không hợp lệ.'
      );
    }

    try {
      await updateSettings(draft => {
        draft.mode = mode;
        return draft;
      });

      res.send('Đã đổi chế độ: ' + mode);

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

// KHOI DONG SERVER
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
      'Created a new empty Cloud configuration. Add your desired schedules on the Web.'
    );

  } else {
    settings = normalize(
      JSON.parse(stored)
    );

    console.log(
      'Restored schedules and mode from Upstash:',
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

    // CHI NHAN LENH mode_set.
    // KHONG SUBSCRIBE TOPIC mode.
    client.subscribe(
      [
        TOPIC.food,
        TOPIC.modeSet,
        TOPIC.setTime,
        TOPIC.result
      ],
      { qos: 1 },
      err => {
        if (err) {
          console.error(
            'MQTT subscribe failed:',
            err.message
          );
        }
      }
    );

    sendSettingsToEsp();
  });

  client.on('message', onMqttMessage);

  client.on('error', err => {
    console.error(
      'MQTT error:',
      err.message
    );
  });

  app.listen(PORT, () => {
    console.log(
      `Molly Cloud Sync listening on ${PORT}`
    );
  });
}

start().catch(err => {
  console.error(
    'KHONG DOC DUOC UPSTASH, DUNG KHOI DONG DE BAO VE LICH:',
    err.message
  );

  process.exit(1);
});
