/**
 * TechLab Room Agent
 * 룸 PC에 설치 - 부팅 시 자동실행, 브리지 서버에 자동 등록
 *
 * 빌드: npm install && npx pkg agent.js --targets node18-win-x64 --output techlab_agent.exe
 * 자동시작: 윈도우 시작프로그램에 등록 (설치 시 자동 처리)
 */

const os      = require('os');
const http    = require('http');
const dgram   = require('dgram');

// ── 설정 ──────────────────────────────────────────────────────────────────
// 브리지 서버 자동 탐색 (같은 네트워크면 UDP 브로드캐스트로 찾음)
// 못 찾으면 fallback IP 사용
const FALLBACK_BRIDGE = '192.168.0.200'; // 키오스크/카운터 PC IP
const BRIDGE_PORT     = 3000;
const BROADCAST_PORT  = 3001;
const HEARTBEAT_SEC   = 10;

// IP 끝자리 → 룸 번호 매핑 (201=room1 ~ 207=room7)
// DHCP 환경이면 자동 감지로 대체됨
const IP_TO_ROOM = {
  201: 'room1', 202: 'room2', 203: 'room3', 204: 'room4',
  205: 'room5', 206: 'room6', 207: 'room7'
};

// ── 내 네트워크 정보 가져오기 ──────────────────────────────────────────────
function getMyInfo() {
  const ifaces = os.networkInterfaces();
  for (const name of Object.keys(ifaces)) {
    for (const iface of ifaces[name]) {
      if (iface.family === 'IPv4' && !iface.internal) {
        return { ip: iface.address, mac: iface.mac, hostname: os.hostname() };
      }
    }
  }
  return { ip: '0.0.0.0', mac: '00:00:00:00:00:00', hostname: os.hostname() };
}

// IP 끝자리로 룸 번호 추측
function guessRoomId(ip) {
  const last = parseInt(ip.split('.').pop());
  return IP_TO_ROOM[last] || null;
}

// ── UDP 브로드캐스트로 브리지 서버 자동 탐색 ──────────────────────────────
let bridgeIp = FALLBACK_BRIDGE;

function discoverBridge() {
  return new Promise(resolve => {
    const sock = dgram.createSocket('udp4');
    let found = false;

    sock.on('message', (msg, rinfo) => {
      try {
        const data = JSON.parse(msg.toString());
        if (data.type === 'techlab_bridge') {
          console.log('[탐색] 브리지 서버 발견:', rinfo.address);
          bridgeIp = rinfo.address;
          found = true;
          sock.close();
          resolve(rinfo.address);
        }
      } catch(e) {}
    });

    sock.bind(() => {
      sock.setBroadcast(true);
      // 브리지 서버 탐색 요청 브로드캐스트
      const msg = Buffer.from(JSON.stringify({ type: 'find_bridge' }));
      sock.send(msg, 0, msg.length, BROADCAST_PORT, '255.255.255.255');
      console.log('[탐색] 브리지 서버 탐색 중...');
    });

    // 3초 안에 못 찾으면 fallback
    setTimeout(() => {
      if (!found) {
        console.log('[탐색] 탐색 실패, fallback 사용:', FALLBACK_BRIDGE);
        try { sock.close(); } catch(e) {}
        resolve(FALLBACK_BRIDGE);
      }
    }, 3000);
  });
}

// ── 브리지 서버에 등록 요청 ───────────────────────────────────────────────
function registerToServer(info, roomId) {
  const body = JSON.stringify({
    ip:       info.ip,
    mac:      info.mac,
    hostname: info.hostname,
    roomId:   roomId,       // null이면 서버가 자동 배정
    agentVersion: '1.0'
  });

  const options = {
    hostname: bridgeIp,
    port:     BRIDGE_PORT,
    path:     '/agent/register',
    method:   'POST',
    headers:  { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) }
  };

  return new Promise((resolve, reject) => {
    const req = http.request(options, res => {
      let data = '';
      res.on('data', d => data += d);
      res.on('end', () => {
        try {
          const result = JSON.parse(data);
          console.log(`[등록] 완료 → ${result.roomId} (${result.roomName})`);
          resolve(result);
        } catch(e) { reject(e); }
      });
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

// ── 명령 수신 서버 (브리지 → 에이전트) ──────────────────────────────────
function startCommandServer() {
  const server = http.createServer((req, res) => {
    if (req.method !== 'POST') return res.end('{}');
    let body = '';
    req.on('data', d => body += d);
    req.on('end', () => {
      try {
        const cmd = JSON.parse(body);
        console.log('[명령] 수신:', cmd);
        handleCommand(cmd);
        res.end(JSON.stringify({ ok: true }));
      } catch(e) { res.end(JSON.stringify({ ok: false })); }
    });
  });

  server.listen(3002, () => console.log('[에이전트] 명령 수신 대기: port 3002'));
}

function handleCommand(cmd) {
  if (cmd.action === 'wakeup') {
    console.log('[명령] 화면 활성화 / 슬립 해제');
    // 실제 구현 시: child_process로 powercfg, nircmd 등 호출
    // require('child_process').exec('powershell -command "$h = Add-Type ...wake screen..."');
  }
  if (cmd.action === 'shutdown') {
    console.log('[명령] 종료');
  }
  if (cmd.action === 'ping') {
    console.log('[명령] ping - 응답함');
  }
}

// ── 하트비트 (주기적으로 생존 신호) ─────────────────────────────────────
function startHeartbeat(info, roomId) {
  setInterval(() => {
    registerToServer(info, roomId).catch(e => {
      console.warn('[하트비트] 실패, 재탐색...');
      discoverBridge().then(() => registerToServer(info, roomId).catch(() => {}));
    });
  }, HEARTBEAT_SEC * 1000);
}

// ── 메인 ─────────────────────────────────────────────────────────────────
async function main() {
  console.log('=== TechLab Room Agent 시작 ===');
  const info   = getMyInfo();
  const roomId = guessRoomId(info.ip); // IP 끝자리로 추측, null이면 서버가 배정
  console.log(`내 정보: IP=${info.ip} MAC=${info.mac} 추측룸=${roomId || '자동배정'}`);

  // 브리지 서버 탐색
  await discoverBridge();

  // 등록
  try {
    const result = await registerToServer(info, roomId);
    startHeartbeat(info, result.roomId);
  } catch(e) {
    console.error('[등록] 실패:', e.message, '- 재시도...');
    setTimeout(main, 10000);
    return;
  }

  // 명령 수신 서버 시작
  startCommandServer();
}

main();
