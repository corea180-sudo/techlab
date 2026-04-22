/**
 * ╔══════════════════════════════════════════════════════════╗
 * ║           FORE Bridge Server  v1.0                      ║
 * ║  Firebase Firestore ↔ Admin / 키오스크 / 룸PC 허브       ║
 * ║  실행: node bridge.js                                    ║
 * ╚══════════════════════════════════════════════════════════╝
 *
 * 신호 흐름:
 *  [앱] → Firestore 예약 생성(req)
 *       → Bridge가 onSnapshot으로 감지
 *       → WebSocket으로 Admin에 전달
 *       → Admin 승인/취소
 *       → Bridge → Firestore 업데이트
 *       → 앱에 FCM 푸시
 */

require('dotenv').config();
const express    = require('express');
const cors       = require('cors');
const http       = require('http');
const WebSocket  = require('ws');
const admin      = require('firebase-admin');

// ──────────────────────────────────────────
// 1. Firebase Admin 초기화
// ──────────────────────────────────────────
let db, messaging;
try {
  const serviceAccount = require(process.env.FIREBASE_KEY_PATH || './firebase-key.json');
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    projectId:  process.env.FIREBASE_PROJECT_ID
  });
  db        = admin.firestore();
  messaging = admin.messaging();
  console.log('[Firebase] ✅ 연결 성공');
} catch (e) {
  console.warn('[Firebase] ⚠️  키 파일 없음 → 로컬 전용 모드로 실행');
  console.warn('           firebase-key.json 을 이 폴더에 넣으면 Firebase 연동됩니다.');
  db = null; messaging = null;
}

const STORE_ID = process.env.STORE_ID || 'store_001';
const PORT     = parseInt(process.env.PORT || '3000');

// ──────────────────────────────────────────
// 2. Express HTTP 서버
// ──────────────────────────────────────────
const app    = express();
const server = http.createServer(app);
app.use(cors());
app.use(express.json());

// ──────────────────────────────────────────
// 3. WebSocket 서버
// ──────────────────────────────────────────
const wss = new WebSocket.Server({ server });

// 연결된 클라이언트 맵  { clientId → { ws, type, roomId? } }
// type: 'admin' | 'kiosk' | 'room_agent'
const clients = new Map();
let clientSeq = 0;

wss.on('connection', (ws) => {
  const clientId = ++clientSeq;
  clients.set(clientId, { ws, type: 'unknown' });
  console.log(`[WS] 클라이언트 연결 #${clientId} (총 ${clients.size}명)`);

  // 연결 즉시 현재 상태 전송
  ws.send(JSON.stringify({ type: 'connected', clientId }));

  ws.on('message', async (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    await handleClientMessage(clientId, msg);
  });

  ws.on('close', () => {
    const info = clients.get(clientId);
    // 룸 에이전트면 offline 처리
    if (info?.type === 'room_agent' && info.roomId) {
      broadcast({ type: 'agent_offline', roomId: info.roomId }, 'admin');
    }
    clients.delete(clientId);
    console.log(`[WS] 클라이언트 해제 #${clientId} (총 ${clients.size}명)`);
  });
});

// ──────────────────────────────────────────
// 4. 클라이언트 메시지 처리
// ──────────────────────────────────────────
async function handleClientMessage(clientId, msg) {
  const info = clients.get(clientId);
  if (!info) return;

  switch (msg.type) {

    // ── 클라이언트 등록 ──
    case 'register':
      info.type   = msg.clientType;  // 'admin' | 'kiosk' | 'room_agent'
      info.roomId = msg.roomId || null;
      console.log(`[WS] #${clientId} 등록: ${info.type}${info.roomId ? ' ('+info.roomId+')' : ''}`);
      // Admin 등록 시 현재 예약 목록 즉시 전송
      if (info.type === 'admin') {
        const reservations = await getReservations();
        send(clientId, { type: 'reservations_sync', reservations });
      }
      // 룸 에이전트 등록 시 Admin에 알림
      if (info.type === 'room_agent') {
        broadcast({ type: 'agent_online', roomId: info.roomId, hostname: msg.hostname, ip: msg.ip, mac: msg.mac }, 'admin');
      }
      break;

    // ── Admin → 예약 승인 ──
    case 'reservation_approve': {
      const { reservationId } = msg;
      const result = await approveReservation(reservationId);
      send(clientId, { type: 'reservation_approved', reservationId, ok: result.ok, error: result.error });
      if (result.ok) {
        // 앱에 FCM 푸시
        const res = result.reservation;
        await sendFCM(res.userFcmToken, {
          title: '예약이 확정되었습니다 ✅',
          body:  `${res.storeName || STORE_ID} · ${formatTime(res.startTime)} · ${res.people}명`,
        }, { reservationId, type: 'reservation_confirmed' });
        // 다른 Admin에도 브로드캐스트
        broadcast({ type: 'reservation_status_changed', reservationId, status: 'app' }, 'admin', clientId);
      }
      break;
    }

    // ── Admin → 예약 취소 (매장 일방 취소) ──
    case 'reservation_cancel': {
      const { reservationId, reason } = msg;
      const result = await cancelReservation(reservationId, reason || '매장 취소');
      send(clientId, { type: 'reservation_cancelled', reservationId, ok: result.ok });
      if (result.ok) {
        const res = result.reservation;
        await sendFCM(res.userFcmToken, {
          title: '예약이 취소되었습니다',
          body:  `${res.storeName || STORE_ID} · ${formatTime(res.startTime)} 예약이 취소되었습니다.`,
        }, { reservationId, type: 'reservation_cancelled', reason });
        broadcast({ type: 'reservation_status_changed', reservationId, status: 'cancelled', reason }, 'admin', clientId);
      }
      break;
    }

    // ── 룸 에이전트 상태 보고 ──
    case 'agent_report':
      info.lastSeen = Date.now();
      broadcast({
        type:     'agent_report',
        roomId:   info.roomId,
        hostname: msg.hostname,
        ip:       msg.ip,
        mac:      msg.mac,
        lastSeen: info.lastSeen
      }, 'admin');
      break;

    // ── IP 설정 (Admin → Bridge → 룸 에이전트) ──
    case 'set_ip': {
      const { roomId, ip, subnet, gateway, dns } = msg;
      const target = [...clients.values()].find(c => c.type === 'room_agent' && c.roomId === roomId);
      if (target) {
        target.ws.send(JSON.stringify({ type: 'set_ip', ip, subnet, gateway, dns }));
      } else {
        send(clientId, { type: 'ip_set_result', ok: false, error: '에이전트 오프라인' });
      }
      break;
    }

    default:
      console.log(`[WS] 알 수 없는 메시지 타입: ${msg.type}`);
  }
}

// ──────────────────────────────────────────
// 5. Firebase Firestore CRUD
// ──────────────────────────────────────────

// 예약 컬렉션 경로: stores/{storeId}/reservations/{id}
function reservationsRef() {
  return db.collection('stores').doc(STORE_ID).collection('reservations');
}

async function getReservations() {
  if (!db) return [];
  try {
    const snap = await reservationsRef()
      .where('status', 'in', ['req', 'app'])
      .orderBy('startTime', 'asc')
      .get();
    return snap.docs.map(d => ({ id: d.id, ...d.data() }));
  } catch (e) {
    console.error('[Firestore] getReservations 오류:', e.message);
    return [];
  }
}

async function approveReservation(reservationId) {
  if (!db) return { ok: false, error: 'Firebase 미연결' };
  try {
    const ref  = reservationsRef().doc(String(reservationId));
    const snap = await ref.get();
    if (!snap.exists) return { ok: false, error: '예약 없음' };
    const data = snap.data();
    if (data.status !== 'req') return { ok: false, error: '요청 상태가 아님' };
    await ref.update({ status: 'app', approvedAt: admin.firestore.FieldValue.serverTimestamp() });
    return { ok: true, reservation: { ...data, id: reservationId } };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

async function cancelReservation(reservationId, reason) {
  if (!db) return { ok: false, error: 'Firebase 미연결' };
  try {
    const ref  = reservationsRef().doc(String(reservationId));
    const snap = await ref.get();
    if (!snap.exists) return { ok: false, error: '예약 없음' };
    const data = snap.data();
    await ref.update({
      status:      'cancelled',
      cancelReason: reason,
      cancelledAt: admin.firestore.FieldValue.serverTimestamp()
    });
    return { ok: true, reservation: { ...data, id: reservationId } };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

// ──────────────────────────────────────────
// 예약 완료(정산) - status: app → done
// ──────────────────────────────────────────
async function doneReservation(reservationId, payload = {}) {
  if (!db) return { ok: false, error: 'Firebase 미연결' };
  try {
    const ref  = reservationsRef().doc(String(reservationId));
    const snap = await ref.get();
    if (!snap.exists) return { ok: false, error: '예약 없음' };
    const data = snap.data();
    const update = {
      status:     'done',
      completedAt: admin.firestore.FieldValue.serverTimestamp()
    };
    if (typeof payload.finalPrice === 'number') update.finalPrice = payload.finalPrice;
    if (typeof payload.paidAmount === 'number') update.paidAmount = payload.paidAmount;
    if (typeof payload.usedMin === 'number')    update.usedMin    = payload.usedMin;
    if (payload.source)                         update.completedSource = payload.source;
    await ref.update(update);
    return { ok: true, reservation: { ...data, ...update, id: reservationId } };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

// ──────────────────────────────────────────
// 룸 상태 컬렉션: stores/{storeId}/rooms/{roomId}
// ──────────────────────────────────────────
function roomsRef() {
  return db.collection('stores').doc(STORE_ID).collection('rooms');
}

// 룸 사용 시작 (키오스크 입장, 현장 결제)
// body: { userName, people, source, plannedEndAt, startedAt? }
async function startRoomSession(roomId, body = {}) {
  if (!db) return { ok: false, error: 'Firebase 미연결' };
  try {
    const now = Date.now();
    const startedAt = body.startedAt || now;
    // plannedEndAt 없으면 기본 2시간
    const plannedEndAt = body.plannedEndAt || (now + 2 * 60 * 60 * 1000);
    const doc = {
      roomId,
      storeName:     body.storeName || '',
      status:       'using',
      currentSession: {
        userName:     body.userName || '현장 고객',
        startedAt,
        plannedEndAt,
        people:       body.people || 1,
        source:       body.source || 'kiosk'
      },
      updatedAt:    admin.firestore.FieldValue.serverTimestamp()
    };
    await roomsRef().doc(roomId).set(doc, { merge: true });
    return { ok: true, room: doc };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

// 룸 사용 종료 (정산 완료)
async function endRoomSession(roomId) {
  if (!db) return { ok: false, error: 'Firebase 미연결' };
  try {
    await roomsRef().doc(roomId).set({
      roomId,
      status:         'idle',
      currentSession: null,
      updatedAt:      admin.firestore.FieldValue.serverTimestamp()
    }, { merge: true });
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

// 룸 예상 종료시간 업데이트 (시간 연장/단축)
async function updateRoomSession(roomId, body = {}) {
  if (!db) return { ok: false, error: 'Firebase 미연결' };
  try {
    const ref = roomsRef().doc(roomId);
    const snap = await ref.get();
    if (!snap.exists) return { ok: false, error: '룸 상태 없음' };
    const data = snap.data();
    if (!data.currentSession) return { ok: false, error: '사용중 세션 없음' };

    const newSession = { ...data.currentSession };
    if (typeof body.plannedEndAt === 'number') newSession.plannedEndAt = body.plannedEndAt;
    if (typeof body.people === 'number') newSession.people = body.people;
    if (body.userName) newSession.userName = body.userName;

    await ref.update({
      currentSession: newSession,
      updatedAt:      admin.firestore.FieldValue.serverTimestamp()
    });
    return { ok: true, session: newSession };
  } catch (e) {
    return { ok: false, error: e.message };
  }
}

// ──────────────────────────────────────────
// 6. Firebase Firestore 실시간 리슨
//    앱이 예약 생성(req) → Bridge가 감지 → Admin에 전달
// ──────────────────────────────────────────
function startFirestoreListener() {
  if (!db) {
    console.warn('[Firestore] ⚠️  리슨 스킵 (Firebase 미연결)');
    return;
  }

  reservationsRef()
    .where('status', '==', 'req')
    .onSnapshot(snapshot => {
      snapshot.docChanges().forEach(change => {
        const data = { id: change.doc.id, ...change.doc.data() };

        if (change.type === 'added') {
          console.log(`[Firestore] 새 예약 요청 → roomId:${data.roomId} / ${data.people}명`);
          // Admin에 실시간 전달
          broadcast({ type: 'new_reservation', reservation: data }, 'admin');
          // 키오스크에도 알림
          broadcast({ type: 'new_reservation_kiosk', roomId: data.roomId }, 'kiosk');
        }

        if (change.type === 'modified') {
          broadcast({ type: 'reservation_updated', reservation: data }, 'admin');
        }

        if (change.type === 'removed') {
          broadcast({ type: 'reservation_removed', reservationId: data.id }, 'admin');
        }
      });
    }, err => {
      console.error('[Firestore] 리슨 오류:', err.message);
    });

  console.log(`[Firestore] ✅ 예약 실시간 리슨 시작 (store: ${STORE_ID})`);
}

// ──────────────────────────────────────────
// 7. FCM 푸시 전송
// ──────────────────────────────────────────
async function sendFCM(fcmToken, notification, data = {}) {
  if (!messaging || !fcmToken) return;
  try {
    await messaging.send({
      token:        fcmToken,
      notification: { title: notification.title, body: notification.body },
      data:         Object.fromEntries(Object.entries(data).map(([k,v]) => [k, String(v)])),
      android:      { priority: 'high' },
      apns:         { payload: { aps: { sound: 'default' } } }
    });
    console.log(`[FCM] ✅ 푸시 전송: ${notification.title}`);
  } catch (e) {
    console.warn(`[FCM] 전송 실패: ${e.message}`);
  }
}

// ──────────────────────────────────────────
// 8. HTTP REST API (Admin ↔ Bridge)
// ──────────────────────────────────────────

// 연결된 에이전트 목록
app.get('/agents', (req, res) => {
  const result = {};
  clients.forEach((info, id) => {
    if (info.type === 'room_agent' && info.roomId) {
      result[info.roomId] = {
        clientId: id,
        hostname: info.hostname,
        ip:       info.ip,
        mac:      info.mac,
        lastSeen: info.lastSeen || Date.now()
      };
    }
  });
  res.json(result);
});

// 예약 목록 (Admin 초기 로드용)
app.get('/reservations', async (req, res) => {
  const list = await getReservations();
  res.json(list);
});

// 예약 승인
app.post('/reservations/:id/approve', async (req, res) => {
  const result = await approveReservation(req.params.id);
  if (result.ok) {
    broadcast({ type: 'reservation_status_changed', reservationId: req.params.id, status: 'app' }, 'admin');
    const r = result.reservation;
    await sendFCM(r.userFcmToken, {
      title: '예약이 확정되었습니다 ✅',
      body:  `${formatTime(r.startTime)} · ${r.people}명`,
    }, { reservationId: req.params.id, type: 'reservation_confirmed' });
  }
  res.json(result);
});

// 예약 취소 (매장 일방)
app.post('/reservations/:id/cancel', async (req, res) => {
  const result = await cancelReservation(req.params.id, req.body.reason || '매장 취소');
  if (result.ok) {
    broadcast({ type: 'reservation_status_changed', reservationId: req.params.id, status: 'cancelled' }, 'admin');
    const r = result.reservation;
    await sendFCM(r.userFcmToken, {
      title: '예약이 취소되었습니다',
      body:  `${formatTime(r.startTime)} 예약이 취소되었습니다.`,
    }, { reservationId: req.params.id, type: 'reservation_cancelled' });
  }
  res.json(result);
});

// 예약 완료(정산) - app → done
app.post('/reservations/:id/done', async (req, res) => {
  const result = await doneReservation(req.params.id, req.body || {});
  if (result.ok) {
    broadcast({ type: 'reservation_status_changed', reservationId: req.params.id, status: 'done' }, 'admin');
  }
  res.json(result);
});

// ──────────────────────────────────────────
// 룸 상태 엔드포인트
// ──────────────────────────────────────────

// 룸 사용 시작 (키오스크/Admin에서 입장 시)
// body: { userName, people, source, plannedEndAt, storeName }
app.post('/rooms/:roomId/start-session', async (req, res) => {
  const result = await startRoomSession(req.params.roomId, req.body || {});
  if (result.ok) {
    broadcast({ type: 'room_status_changed', roomId: req.params.roomId, status: 'using' }, 'admin');
  }
  res.json(result);
});

// 룸 사용 종료 (정산 완료)
app.post('/rooms/:roomId/end-session', async (req, res) => {
  const result = await endRoomSession(req.params.roomId);
  if (result.ok) {
    broadcast({ type: 'room_status_changed', roomId: req.params.roomId, status: 'idle' }, 'admin');
  }
  res.json(result);
});

// 룸 세션 업데이트 (시간 연장/단축, 인원 변경)
app.post('/rooms/:roomId/update-session', async (req, res) => {
  const result = await updateRoomSession(req.params.roomId, req.body || {});
  if (result.ok) {
    broadcast({ type: 'room_status_changed', roomId: req.params.roomId, status: 'using' }, 'admin');
  }
  res.json(result);
});

// 룸 상태 조회 (전체 또는 특정 룸)
app.get('/rooms', async (req, res) => {
  if (!db) return res.json([]);
  try {
    const snap = await roomsRef().get();
    res.json(snap.docs.map(d => ({ id: d.id, ...d.data() })));
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/rooms/:roomId', async (req, res) => {
  if (!db) return res.status(503).json({ error: 'Firebase 미연결' });
  try {
    const snap = await roomsRef().doc(req.params.roomId).get();
    if (!snap.exists) return res.status(404).json({ error: '룸 상태 없음' });
    res.json({ id: snap.id, ...snap.data() });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// IP 일괄 설정 (Admin → Bridge → 룸 에이전트들)
app.post('/rooms/set-all-ip', (req, res) => {
  const { baseIp, subnet, gateway, dns, startOctet } = req.body;
  let sent = 0;
  clients.forEach((info) => {
    if (info.type !== 'room_agent' || !info.roomId) return;
    const idx = parseInt(info.roomId.replace(/\D/g, '')) - 1;
    const ip  = `${baseIp}.${startOctet + idx}`;
    info.ws.send(JSON.stringify({ type: 'set_ip', ip, subnet, gateway, dns }));
    sent++;
  });
  res.json({ ok: true, sent });
});

// 특정 룸 에이전트 명령
app.post('/agent/:roomId/command', (req, res) => {
  const { roomId } = req.params;
  const target = [...clients.values()].find(c => c.type === 'room_agent' && c.roomId === roomId);
  if (!target) return res.status(404).json({ ok: false, error: '에이전트 오프라인' });
  target.ws.send(JSON.stringify(req.body));
  res.json({ ok: true });
});

// 헬스체크
app.get('/health', (req, res) => {
  res.json({
    ok:       true,
    uptime:   process.uptime().toFixed(0) + 's',
    clients:  clients.size,
    firebase: !!db,
    store:    STORE_ID
  });
});

// ──────────────────────────────────────────
// 9. 유틸
// ──────────────────────────────────────────
function send(clientId, data) {
  const info = clients.get(clientId);
  if (info?.ws?.readyState === WebSocket.OPEN) {
    info.ws.send(JSON.stringify(data));
  }
}

// type이 있으면 해당 타입에만, excludeId는 제외
function broadcast(data, targetType = null, excludeId = null) {
  const msg = JSON.stringify(data);
  clients.forEach((info, id) => {
    if (id === excludeId) return;
    if (targetType && info.type !== targetType) return;
    if (info.ws.readyState === WebSocket.OPEN) {
      info.ws.send(msg);
    }
  });
}

function formatTime(timestamp) {
  if (!timestamp) return '-';
  const d = timestamp.toDate ? timestamp.toDate() : new Date(timestamp);
  return d.toLocaleString('ko-KR', { month: 'long', day: 'numeric', hour: '2-digit', minute: '2-digit' });
}

// ──────────────────────────────────────────
// 10. 서버 시작
// ──────────────────────────────────────────
server.listen(PORT, () => {
  console.log('');
  console.log('╔══════════════════════════════════════════╗');
  console.log('║       FORE Bridge Server 시작            ║');
  console.log(`║  HTTP  : http://localhost:${PORT}           ║`);
  console.log(`║  WS    : ws://localhost:${PORT}             ║`);
  console.log(`║  Store : ${STORE_ID}                  ║`);
  console.log('╚══════════════════════════════════════════╝');
  console.log('');
  startFirestoreListener();
});

// 예외 처리 - 서버 죽지 않게
process.on('uncaughtException',  e => console.error('[오류]', e.message));
process.on('unhandledRejection', e => console.error('[Promise 오류]', e));