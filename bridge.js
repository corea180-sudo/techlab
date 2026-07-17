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
const _offTimers = {};                 // storeId → 5분 장애 판정 타이머 (서버에서 판정, 브라우저 무관)
const _alertState = {};                // storeId → 현재 장애확정 상태 (블랙박스 이벤트 전환 판정용)
const _presenceState = {};             // storeId → 마지막 쓴 online (중복쓰기 방지 + stale 자가치유)
const GUARDIAN_SECRET = process.env.GUARDIAN_SECRET || '';   // 가디언 이벤트 쓰기 공유 시크릿 (Render env)
const WD_GRACE_MS = 5 * 60 * 1000;     // 연결 끊김 후 이 시간 미복구 시 장애 확정 (일시적 끊김 무시)
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
    const sid = info?.storeId || STORE_ID;
    // 룸 에이전트면 offline 처리
    if (info?.type === 'room_agent' && info.roomId) {
      broadcast({ type: 'agent_offline', roomId: info.roomId }, 'admin');
    }
    clients.delete(clientId);
    // presence: 이 매장의 마지막 연결이 끊겼으면 offline 표시
    if (info && storeConnCount(sid) === 0) {
      updatePresence(sid, false);
      // 5분 장애 판정 — 서버에서 직접. 5분 뒤에도 0연결이면 장애 확정(alert.active)
      if (_offTimers[sid]) clearTimeout(_offTimers[sid]);
      _offTimers[sid] = setTimeout(() => {
        delete _offTimers[sid];
        if (storeConnCount(sid) === 0) markStoreAlert(sid, true);  // 여전히 끊김 → 장애 확정
      }, WD_GRACE_MS);
    }
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
      info.type   = msg.clientType;
      info.roomId = msg.roomId || null;
      info.hostname = msg.hostname || '';
      info.ip       = msg.ip || '';
      info.mac      = msg.mac || '';
      const _prevStoreId = info.storeId;         // 매장전환 감지용 (직전 등록 storeId, 첫 등록이면 undefined)
      info.storeId  = msg.storeId || STORE_ID;   // 신원: 클라이언트가 보낸 매장ID, 없으면 store_001 fallback (기존 호환)
      console.log(`[WS] #${clientId} 등록: ${info.type}${info.roomId ? ' ('+info.roomId+')' : ''} [${info.storeId}]`);
      // presence: 등록될 때마다 online 보장 (중복은 updatePresence 내부 dedup) → stale-false 자가치유
      updatePresence(info.storeId, true);
      // 매장전환(같은 PC가 002→001 등 캐시 다중ID) → 옛 매장 연결이 0이 되면 그 매장 online 즉시 정정 (유령 방지, 근본)
      if (_prevStoreId && _prevStoreId !== info.storeId && storeConnCount(_prevStoreId) === 0) {
        updatePresence(_prevStoreId, false);
      }
      // 재연결 → 5분 타이머 취소 + 장애 해제 (복구되면 경보 멈춤)
      if (_offTimers[info.storeId]) { clearTimeout(_offTimers[info.storeId]); delete _offTimers[info.storeId]; }
      markStoreAlert(info.storeId, false);
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
    // 🔥 2026-04-25: 결제수단 정보 저장 (현금/카드 분류용)
    if (Array.isArray(payload.payments))        update.payments     = payload.payments;
    if (payload.paymentMethod === 'cash' || payload.paymentMethod === 'card') {
      update.paymentMethod = payload.paymentMethod;
      update.method        = payload.paymentMethod;  // 호환용 별칭
    }
    if (typeof payload.cashAmount === 'number') update.cashAmount   = payload.cashAmount;
    if (typeof payload.cardAmount === 'number') update.cardAmount   = payload.cardAmount;
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

// ── 인증 미들웨어 ──────────────────────────
//  브라우저 호출(admin/owner/super/apt): Firebase ID 토큰(Authorization: Bearer) 검증
//  기계 호출(가디언 등): 공유시크릿(X-Guardian-Secret 헤더 또는 body.secret)
//  라이브 안 깨는 2단계 롤아웃: AUTH_ENFORCE≠'1'이면 미인증도 통과시키되 경고 로그만 남김
//   → 실호출이 전부 토큰 붙여 들어오는지 로그로 확인 후, Render env AUTH_ENFORCE=1 로 강제 차단.
const AUTH_ENFORCE = process.env.AUTH_ENFORCE === '1';
async function requireAuth(req, res, next) {
  // 1) 공유시크릿 (기계 호출)
  const hs = req.get('X-Guardian-Secret') || (req.body && req.body.secret);
  if (GUARDIAN_SECRET && hs === GUARDIAN_SECRET) { req.authKind = 'secret'; return next(); }
  // 2) Firebase ID 토큰 (브라우저 로그인 앱)
  const m = (req.get('Authorization') || '').match(/^Bearer (.+)$/i);
  if (m && admin.apps.length) {
    try {
      const dec = await admin.auth().verifyIdToken(m[1]);
      req.authUid = dec.uid; req.authKind = 'token';
      return next();
    } catch (e) { /* 무효/만료 토큰 → 아래 미인증 처리 */ }
  }
  // 3) 미인증
  console.warn(`[auth] 미인증 접근: ${req.method} ${req.path} origin=${req.get('origin') || '-'} enforce=${AUTH_ENFORCE}`);
  if (AUTH_ENFORCE) return res.status(401).json({ error: 'unauthorized' });
  return next();  // 모니터 모드: 통과(경고만) — 롤아웃 중 라이브 보호
}
// 민감 엔드포인트에만 적용 (prefix). /health는 공개 유지, /guardian-event는 자체 시크릿 검사.
//  '/agent' prefix가 /agents·/agent/:id/command 모두 커버, /reservations·/rooms 도 하위 포함.
app.use(['/agents', '/agent', '/reservations', '/rooms'], requireAuth);

// 연결된 에이전트 목록
app.get('/agents', (req, res) => {
  const wantStore = req.query.storeId || null;   // ★매장별 필터 (하위호환: 파라미터 없으면 기존대로 전체)
  const result = {};
  clients.forEach((info, id) => {
    if (info.type === 'room_agent' && info.roomId) {
      if (wantStore && (info.storeId || null) !== wantStore) return;   // 그 매장 것만 반환
      result[info.roomId] = {
        clientId: id,
        storeId:  info.storeId || null,
        hostname: info.hostname,
        ip:       info.ip,
        mac:      info.mac,
        lastSeen: info.lastSeen || Date.now()
      };
    }
  });
  res.json(result);
});

// 🩶 가디언 이벤트 수신 (크래시→재시작·서킷브레이커 저하·종료 등)
//    가디언은 storeId를 모름 → 자기 hostname만 보냄. 같은 PC의 등록 에이전트(hostname 일치)로 storeId 매핑.
//    쓰기 엔드포인트라 공유 시크릿 필수(GUARDIAN_SECRET env). 기록은 2a와 동일한 events.log.
app.post('/guardian-event', (req, res) => {
  const { host, ev, detail, secret } = req.body || {};
  if (!GUARDIAN_SECRET || secret !== GUARDIAN_SECRET) return res.status(403).json({ error: 'forbidden' });
  if (!host || !ev) return res.status(400).json({ error: 'host and ev required' });
  let sid = null;
  clients.forEach(info => {
    if (info.type === 'room_agent' && (info.hostname || '').toLowerCase() === String(host).toLowerCase()) sid = info.storeId || sid;
  });
  if (!sid) return res.status(404).json({ error: 'store not resolved for host', host });
  logGuardianEvent(sid, String(ev).slice(0, 40), detail);
  res.json({ ok: true, storeId: sid, ev });
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

// 🔥 2026-04-25: 즉시 정산 (admin이 룸을 reservation 없이 직접 시작 후 정산하는 케이스)
// 새 reservation을 'done' 상태로 생성. 결제수단 정보 함께 저장.
app.post('/reservations/instant-done', async (req, res) => {
  if (!db) return res.json({ ok: false, error: 'Firebase 미연결' });
  try {
    const p = req.body || {};
    const id = 'inst_' + Date.now() + '_' + Math.floor(Math.random() * 1000);
    const doc = {
      id,
      roomId:    p.roomId || '',
      status:    'done',
      finalPrice: typeof p.finalPrice === 'number' ? p.finalPrice : 0,
      paidAmount: typeof p.paidAmount === 'number' ? p.paidAmount : 0,
      usedMin:    typeof p.usedMin    === 'number' ? p.usedMin    : 0,
      people:     p.people || 1,
      userName:   p.userName || '매장 고객',
      startTime:  p.startTime || Date.now(),
      completedAt: admin.firestore.FieldValue.serverTimestamp(),
      completedSource: p.source || 'pos'
    };
    // 결제수단 정보
    if (Array.isArray(p.payments))               doc.payments      = p.payments;
    if (p.paymentMethod === 'cash' || p.paymentMethod === 'card') {
      doc.paymentMethod = p.paymentMethod;
      doc.method        = p.paymentMethod;
    }
    if (typeof p.cashAmount === 'number')        doc.cashAmount    = p.cashAmount;
    if (typeof p.cardAmount === 'number')        doc.cardAmount    = p.cardAmount;
    await reservationsRef().doc(id).set(doc);
    broadcast({ type: 'reservation_status_changed', reservationId: id, status: 'done' }, 'admin');
    return res.json({ ok: true, id });
  } catch (e) {
    return res.json({ ok: false, error: e.message });
  }
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

// ── presence: 매장 연결 생사 (stores/{sid}/health/presence) ──
// health/heartbeat(곪음·3시간)와 분리된 별도 경로 → 연결 끊김만 실시간 감지
function storeConnCount(storeId) {
  let n = 0;
  clients.forEach(info => { if ((info.storeId || STORE_ID) === storeId) n++; });
  return n;
}

async function updatePresence(storeId, online) {
  if (!db) return;   // 로컬 전용 모드면 skip
  if (_presenceState[storeId] === online) return;   // 이미 그 상태 → 중복쓰기 방지
  _presenceState[storeId] = online;
  try {
    const payload = {
      online,
      updatedAt: admin.firestore.FieldValue.serverTimestamp()
    };
    if (!online) payload.offlineAt = admin.firestore.FieldValue.serverTimestamp();
    await db.collection('stores').doc(storeId)
            .collection('health').doc('presence')
            .set(payload, { merge: true });
    console.log(`[presence] ${storeId} → ${online ? 'ONLINE' : 'OFFLINE'}`);
  } catch (e) {
    _presenceState[storeId] = undefined;   // 실패 → 다음에 재시도 가능
    console.warn(`[presence] ${storeId} 갱신 실패:`, e.message);
  }
}

// 장애 확정/해제를 Firestore에 기록 — 서버가 판정한 "사건"이라 브라우저 새로고침과 무관
// super는 이 alert.active 만 읽어서 경보 (판정은 안 함)
// 🩶 블랙박스 이벤트 로그 — stores/{sid}/health/events.log (최근 14일 + 최근 200건 링버퍼)
//    이벤트 드뭄(진짜 다운/복구 때만) → Firestore 쓰기 사실상 무과금. super·클로드가 읽어 이상유무 판정.
async function logGuardianEvent(storeId, ev, detail) {
  if (!db) return;
  try {
    const entry = { t: Date.now(), ev };
    if (detail != null) entry.detail = String(detail).slice(0, 200);
    const ref = db.collection('stores').doc(storeId).collection('health').doc('events');
    const snap = await ref.get();
    let log = (snap.exists && Array.isArray(snap.data().log)) ? snap.data().log : [];
    log.push(entry);
    const cutoff = Date.now() - 14 * 24 * 60 * 60 * 1000;
    log = log.filter(e => (e.t || 0) >= cutoff).slice(-200);
    await ref.set({ log, updatedAt: admin.firestore.FieldValue.serverTimestamp() }, { merge: true });
    console.log(`[event] ${storeId} → ${ev}`);
  } catch (e) { console.warn(`[event] ${storeId} 기록 실패:`, e.message); }
}

async function markStoreAlert(storeId, active) {
  if (!db) return;
  // 전환 없으면 쓰기·로그 모두 skip (재접속 blip·재배포는 전환 아님, 중복쓰기 방지)
  const _prev = _alertState[storeId] || false;
  if (_prev === active) return;
  _alertState[storeId] = active;
  logGuardianEvent(storeId, active ? 'down' : 'up');
  try {
    const alert = active
      ? { active: true, since: admin.firestore.FieldValue.serverTimestamp() }
      : { active: false };
    await db.collection('stores').doc(storeId)
            .collection('health').doc('presence')
            .set({ alert }, { merge: true });
    console.log(`[alert] ${storeId} → ${active ? 'ACTIVE (장애 확정, 5분 미복구)' : 'CLEAR (복구)'}`);
  } catch (e) {
    console.warn(`[alert] ${storeId} 기록 실패:`, e.message);
  }
}

// 🩶 유령 online 정리 — 이전 브리지 세션(재배포)·매장전환으로 online=true가 굳었지만
//    실제 WS 연결이 0인 매장을 offline로 근본정정. super는 그대로 진실을 읽음(화면 마스킹 아님).
//    부팅 45초 뒤 1회만(에이전트 재접속 대기 후). per-매장 폴링 아님 → 배포당 1회, 무과금.
async function reconcilePhantomPresence() {
  if (!db) return;
  try {
    const storeRefs = await db.collection('stores').listDocuments();
    let fixed = 0;
    for (const sRef of storeRefs) {
      const sid = sRef.id;
      if (storeConnCount(sid) > 0) continue;   // 실제 연결 있으면 진짜 online → 손대지 않음
      const pRef = sRef.collection('health').doc('presence');
      const snap = await pRef.get();
      if (snap.exists && snap.data().online === true) {
        await pRef.set({
          online: false,
          offlineAt: admin.firestore.FieldValue.serverTimestamp(),
          reconciledAt: admin.firestore.FieldValue.serverTimestamp()
        }, { merge: true });
        _presenceState[sid] = false;
        fixed++;
        console.log(`[reconcile] 유령 online 정정 → offline: ${sid}`);
      }
    }
    console.log(`[reconcile] 완료 (실연결 0인 유령 ${fixed}건 offline 정정)`);
  } catch (e) { console.warn('[reconcile] 실패:', e.message); }
}

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
  startAutoCancelOverdue();  // 🔥 2026-04-25: 1시간 초과 미사용 예약 자동 취소
  // 🩶 유령 online 정리 — 재배포/이전세션으로 굳은 online=true를 실연결 0이면 offline로 근본정정 (부팅 45초 뒤 1회)
  setTimeout(reconcilePhantomPresence, 45 * 1000);
});

// 🔥 2026-04-25: 1시간 초과 미사용 예약 자동 취소 (노쇼 정리)
//   - 24시간 무인영업 + 노쇼 케이스 처리
//   - startTime이 1시간 이상 지났는데 status가 여전히 'req' 또는 'app'이면 자동 cancelled
//   - 5분마다 체크. 시작 30초 후 1차 실행
async function autoCancelOverdueReservations() {
  if (!db) return;
  try {
    const now = Date.now();
    const cutoff = now - 60 * 60 * 1000; // 1시간 전
    // status in ('req','app') AND startTime < cutoff
    const snap = await reservationsRef()
      .where('status', 'in', ['req', 'app'])
      .where('startTime', '<', cutoff)
      .get();
    if (snap.empty) return;
    let count = 0;
    const cancelledIds = [];
    for (const docSnap of snap.docs) {
      try {
        await docSnap.ref.update({
          status: 'cancelled',
          cancelReason: 'auto: no-show >1h after startTime',
          autoCancelledAt: admin.firestore.FieldValue.serverTimestamp()
        });
        count++;
        cancelledIds.push(docSnap.id);
      } catch(e) {
        console.warn('[자동취소 실패]', docSnap.id, e.message);
      }
    }
    if (count > 0) {
      console.log(`[자동취소] ${count}건 취소 처리 (1시간 초과 미사용): ${cancelledIds.join(', ')}`);
      // admin들에게 broadcast (룸카드/대시보드 즉시 갱신)
      cancelledIds.forEach(id => {
        broadcast({ type: 'reservation_status_changed', reservationId: id, status: 'cancelled', reason: 'auto-cancel-overdue' }, 'admin');
      });
    }
  } catch(e) {
    console.warn('[자동취소 오류]', e.message);
  }
}

function startAutoCancelOverdue() {
  // 시작 30초 후 1차 실행 (Firebase 연결 안정화 대기)
  setTimeout(autoCancelOverdueReservations, 30 * 1000);
  // 이후 5분마다 반복
  setInterval(autoCancelOverdueReservations, 5 * 60 * 1000);
  console.log('[Auto-Cancel] ✅ 자동 취소 cron 시작 (5분 간격, 1시간 초과 미사용 예약)');
}

// 예외 처리 - 서버 죽지 않게
process.on('uncaughtException',  e => console.error('[오류]', e.message));
process.on('unhandledRejection', e => console.error('[Promise 오류]', e));