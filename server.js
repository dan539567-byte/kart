// שרת חדרים למשחק טורבו קארט.
// תפקידו: ליצור חדרים, לנהל את רשימת השחקנים ולהעביר הודעות בין המארח לשאר השחקנים.
// כל הסימולציה של המשחק רצה אצל המארח, כך שהשרת קל מאוד ולא צורך כמעט משאבים.
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
// אופציונלי: רשימת דומיינים מותרים, מופרדים בפסיק. לדוגמה: https://example.com,https://www.example.com
const ORIGINS = (process.env.ALLOWED_ORIGINS || '').split(',').map(s => s.trim()).filter(Boolean);
const MAX_PLAYERS = 6;
const PUBLIC = path.join(__dirname, 'public');
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.png': 'image/png', '.ico': 'image/x-icon', '.json': 'application/json' };

// שרת סטטי קטן: מאפשר להריץ גם את המשחק עצמו מאותו שרת (תיקיית public)
const server = http.createServer((req, res) => {
  if (req.url === '/health') { res.writeHead(200); res.end('ok'); return; }
  let p = decodeURIComponent((req.url || '/').split('?')[0]);
  if (p === '/') p = '/index.html';
  const file = path.normalize(path.join(PUBLIC, p));
  if (!file.startsWith(PUBLIC)) { res.writeHead(403); res.end(); return; }
  fs.readFile(file, (err, data) => {
    if (err) { res.writeHead(404); res.end('not found'); return; }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(file)] || 'application/octet-stream' });
    res.end(data);
  });
});

const wss = new WebSocketServer({
  server,
  maxPayload: 16 * 1024,
  verifyClient: (info) => !ORIGINS.length || !info.origin || ORIGINS.includes(info.origin),
});

const rooms = new Map(); // code -> room
let nextId = 1;
const LETTERS = 'ABCDEFGHJKMNPQRSTUVWXYZ';
const newCode = () => {
  for (let t = 0; t < 100; t++) {
    let c = '';
    for (let i = 0; i < 4; i++) c += LETTERS[Math.floor(Math.random() * LETTERS.length)];
    if (!rooms.has(c)) return c;
  }
  return null;
};
const clean = (s) => String(s || '').replace(/[<>&"'`\u0000-\u001f]/g, '').trim().slice(0, 14) || 'שחקן';
const send = (ws, o) => { if (ws.readyState === 1) ws.send(typeof o === 'string' ? o : JSON.stringify(o)); };

function roster(room) {
  const list = [...room.players.values()].map(p => ({ id: p.id, name: p.name, c: p.c, v: p.v, host: p.ws === room.host }));
  const msg = JSON.stringify({ t: 'roster', list, cfg: room.cfg });
  for (const p of room.players.values()) send(p.ws, msg);
}

function leave(ws) {
  const room = rooms.get(ws.room);
  if (!room) return;
  room.players.delete(ws.id);
  ws.room = null;
  if (ws === room.host) {
    for (const p of room.players.values()) { send(p.ws, { t: 'closed' }); p.ws.room = null; }
    rooms.delete(room.code);
  } else {
    roster(room);
  }
}

wss.on('connection', (ws) => {
  ws.id = nextId++;
  ws.alive = true;
  ws.room = null;
  ws.lastMsg = 0;
  ws.on('pong', () => { ws.alive = true; });

  ws.on('message', (raw) => {
    let m;
    try { m = JSON.parse(raw.toString()); } catch (e) { return; }
    if (!m || typeof m.t !== 'string') return;
    const room = rooms.get(ws.room);

    if (m.t === 'create' && !room) {
      const code = newCode();
      if (!code) return send(ws, { t: 'err', msg: 'השרת עמוס, נסה שוב' });
      const r = { code, host: ws, players: new Map(), cfg: { map: 0, mode: 0 }, started: false };
      rooms.set(code, r);
      r.players.set(ws.id, { id: ws.id, ws, name: clean(m.name), c: 0, v: 0 });
      ws.room = code;
      send(ws, { t: 'joined', id: ws.id, room: code, host: true });
      roster(r);
      return;
    }
    if (m.t === 'join' && !room) {
      const r = rooms.get(String(m.room || '').toUpperCase());
      if (!r) return send(ws, { t: 'err', msg: 'חדר לא נמצא' });
      if (r.players.size >= MAX_PLAYERS) return send(ws, { t: 'err', msg: 'החדר מלא' });
      if (r.started) return send(ws, { t: 'err', msg: 'המירוץ כבר התחיל' });
      r.players.set(ws.id, { id: ws.id, ws, name: clean(m.name), c: 0, v: 0 });
      ws.room = r.code;
      send(ws, { t: 'joined', id: ws.id, room: r.code, host: false });
      roster(r);
      return;
    }
    if (!room) return;
    const me = room.players.get(ws.id);
    const isHost = ws === room.host;

    if (m.t === 'pick') {
      const c = m.c | 0, v = m.v | 0;
      if (c >= 0 && c < 10) me.c = c;
      if (v >= 0 && v < 5) me.v = v;
      roster(room);
    } else if (m.t === 'cfg' && isHost) {
      const map = m.map | 0;
      room.cfg = { map: map >= 0 && map < 5 ? map : 0, mode: m.mode === 1 ? 1 : 0 };
      roster(room);
    } else if ((m.t === 'in' || m.t === 'item') && !isHost) {
      send(room.host, { t: m.t, from: ws.id, i: m.i | 0 });
    } else if (isHost && (m.t === 'start' || m.t === 'snap' || m.t === 'results' || m.t === 'lobby')) {
      if (m.t === 'start') room.started = true;
      if (m.t === 'lobby') room.started = false;
      const out = raw.toString();
      for (const p of room.players.values()) if (p.ws !== ws) send(p.ws, out);
    }
  });

  ws.on('close', () => leave(ws));
  ws.on('error', () => {});
});

// ניתוק חיבורים מתים
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.alive) { ws.terminate(); continue; }
    ws.alive = false;
    try { ws.ping(); } catch (e) {}
  }
}, 30000);

server.listen(PORT, () => console.log('Turbo Kart server on port ' + PORT));
