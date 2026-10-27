// Rising Fury - static file server + WebSocket relay for online play.
// Local:   npm install && node signal_server.js   ->  http://localhost:3000
// Render:  Build Command "npm install", Start Command "node signal_server.js"
//
// Protocol (JSON text frames):
//   client -> server   {t:'host', code}   create a room            -> {t:'hosted'}  | {t:'err', msg}
//   client -> server   {t:'join', code}   join an existing room    -> guest {t:'joined'}, host {t:'peer'} | {t:'err', msg} | {t:'full'}
//   after pairing      every frame from one player is relayed untouched to the other player
//   leaving            host leaves -> guest gets {t:'bye'} and the room is deleted
//                      guest leaves -> host gets {t:'bye'} and the room stays open for a new guest
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const ROOT = path.join(__dirname, 'Game');
const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.gif': 'image/gif',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
  '.mp3': 'audio/mpeg',
  '.ogg': 'audio/ogg',
  '.wav': 'audio/wav',
  '.woff2': 'font/woff2',
  '.txt': 'text/plain; charset=utf-8'
};

/* ---------- static files from ./Game ---------- */
const server = http.createServer((req, res) => {
  if (req.method !== 'GET' && req.method !== 'HEAD') {
    res.writeHead(405, { Allow: 'GET, HEAD' });
    return res.end();
  }
  let p;
  try {
    p = decodeURIComponent(new URL(req.url, 'http://localhost').pathname);
  } catch (_) {
    res.writeHead(400);
    return res.end('Bad request');
  }
  if (p === '/healthz') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    return res.end('ok');
  }
  if (p.endsWith('/')) p += 'index.html';
  const file = path.normalize(path.join(ROOT, p));
  if (file !== ROOT && !file.startsWith(ROOT + path.sep)) {
    res.writeHead(403);
    return res.end('Forbidden');
  }
  fs.stat(file, (err, st) => {
    if (err || !st.isFile()) {
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      return res.end('Not found');
    }
    const ext = path.extname(file).toLowerCase();
    res.writeHead(200, {
      'Content-Type': MIME[ext] || 'application/octet-stream',
      'Content-Length': st.size,
      'Cache-Control': ext === '.html' ? 'no-cache' : 'public, max-age=3600'
    });
    if (req.method === 'HEAD') return res.end();
    fs.createReadStream(file).pipe(res);
  });
});

/* ---------- WebSocket relay ---------- */
const wss = new WebSocketServer({ server, maxPayload: 64 * 1024 });
const rooms = new Map(); // code -> { host: ws, guest: ws|null }
const CODE_RE = /^[A-Z0-9]{4,8}$/;

const send = (ws, obj) => {
  if (ws && ws.readyState === 1) {
    try { ws.send(JSON.stringify(obj)); } catch (_) {}
  }
};

function leave(ws) {
  const code = ws.room;
  if (!code) return;
  ws.room = null;
  const room = rooms.get(code);
  if (!room) return;
  if (room.host === ws) {
    rooms.delete(code);
    if (room.guest) {
      room.guest.room = null;
      send(room.guest, { t: 'bye' });
    }
  } else if (room.guest === ws) {
    room.guest = null;
    send(room.host, { t: 'bye' });
  }
}

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.room = null;
  ws.role = null;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', (data, isBinary) => {
    ws.isAlive = true;
    if (isBinary) return;
    const raw = data.toString();
    if (raw.length > 20000) return;

    // Already paired: relay the frame untouched to the other player.
    if (ws.room) {
      const room = rooms.get(ws.room);
      if (!room) return;
      const other = ws.role === 'host' ? room.guest : room.host;
      if (other && other.readyState === 1) other.send(raw);
      return;
    }

    // Not in a room yet: only host/join are accepted.
    let m;
    try { m = JSON.parse(raw); } catch (_) { return; }
    if (!m || typeof m !== 'object') return;
    const code = String(m.code || '').toUpperCase();

    if (m.t === 'host') {
      if (!CODE_RE.test(code)) return send(ws, { t: 'err', msg: 'Invalid room code.' });
      if (rooms.has(code)) return send(ws, { t: 'err', msg: 'Room code already in use - try again.' });
      rooms.set(code, { host: ws, guest: null });
      ws.room = code;
      ws.role = 'host';
      send(ws, { t: 'hosted' });
    } else if (m.t === 'join') {
      if (!CODE_RE.test(code)) return send(ws, { t: 'err', msg: 'Invalid room code.' });
      const room = rooms.get(code);
      if (!room) return send(ws, { t: 'err', msg: 'No room found with code ' + code + '.' });
      if (room.guest) return send(ws, { t: 'full' });
      room.guest = ws;
      ws.room = code;
      ws.role = 'guest';
      send(ws, { t: 'joined' });
      send(room.host, { t: 'peer' });
    }
  });

  ws.on('close', () => leave(ws));
  ws.on('error', () => { try { ws.terminate(); } catch (_) {} });
});

// Heartbeat: keeps the connection alive behind Render's proxy and drops dead sockets.
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    try { ws.ping(); } catch (_) {}
  }
}, 25000);

server.listen(PORT, () => console.log('Rising Fury server listening on port ' + PORT));
