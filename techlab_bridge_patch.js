/**
 * techlab_bridge_server.js 에 추가할 코드
 * 기존 파일 하단 server.listen() 위에 붙여넣기
 */

// ── UDP 브로드캐스트 응답 (에이전트가 브리지 탐색할 때 응답) ──────────────
const dgram = require('dgram');
const udpSock = dgram.createSocket('udp4');

udpSock.on('message', (msg, rinfo) => {
  try {
    const data = JSON.parse(msg.toString());
    if (data.type === 'find_bridge') {
      console.log('[UDP] 에이전트 탐색 요청:', rinfo.address);
      const reply = Buffer.from(JSON.stringify({ type: 'techlab_bridge', port: 3000 }));
      udpSock.send(reply, 0, reply.length, rinfo.port, rinfo.address);
    }
  } catch(e) {}
});

udpSock.bind(3001, () => {
  udpSock.setBroadcast(true);
  console.log('[UDP] 에이전트 탐색 응답 대기: port 3001');
});

// ── 에이전트 자동 등록 API ────────────────────────────────────────────────
// IP 끝자리 → 룸 자동 배정 (201=room1 ~ 207=room7)
const IP_ROOM_MAP = { 201:'room1', 202:'room2', 203:'room3', 204:'room4',
                      205:'room5', 206:'room6', 207:'room7' };

// 등록된 에이전트 목록 (메모리 + config 동기화)
const agentRegistry = {};  // { roomId: { ip, mac, hostname, lastSeen } }

app.post('/agent/register', (req, res) => {
  const { ip, mac, hostname, roomId: hintId } = req.body;
  const lastOctet = parseInt(ip.split('.').pop());

  // 룸 배정 우선순위: 1)에이전트 힌트 2)IP끝자리 3)순서대로 자동배정
  let assignedId = hintId
    || IP_ROOM_MAP[lastOctet]
    || autoAssignRoom(ip);

  // config.rooms에 MAC/IP 저장
  if (config.rooms[assignedId]) {
    config.rooms[assignedId].mac = mac;
    config.rooms[assignedId].ip  = ip;
    saveConfig();
  }

  agentRegistry[assignedId] = { ip, mac, hostname, lastSeen: Date.now() };

  console.log(`[에이전트] 등록: ${assignedId} | ${hostname} | ${ip} | ${mac}`);

  // 관리자 페이지/키오스크에 실시간 알림
  broadcast({ type: 'agent_registered', roomId: assignedId, ip, mac, hostname });

  res.json({
    ok: true,
    roomId:   assignedId,
    roomName: config.rooms[assignedId]?.name || assignedId,
    bridgePort: 3000
  });
});

// 순서대로 자동 배정 (아직 등록 안 된 첫 번째 룸)
function autoAssignRoom(ip) {
  const allRooms = Object.keys(config.rooms);
  for (const id of allRooms) {
    if (!agentRegistry[id]) return id;
  }
  return allRooms[0];
}

// 에이전트 목록 조회
app.get('/agents', (req, res) => {
  const now = Date.now();
  const result = {};
  Object.entries(agentRegistry).forEach(([id, info]) => {
    result[id] = { ...info, online: (now - info.lastSeen) < 30000 };
  });
  res.json(result);
});

// 특정 룸 에이전트에 명령 전송 (브리지 → 에이전트)
app.post('/agent/:roomId/command', async (req, res) => {
  const { roomId } = req.params;
  const agent = agentRegistry[roomId];
  if (!agent) return res.status(404).json({ ok: false, error: '에이전트 미등록' });

  try {
    const body = JSON.stringify(req.body);
    await fetch(`http://${agent.ip}:3002`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body
    });
    res.json({ ok: true });
  } catch(e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});
