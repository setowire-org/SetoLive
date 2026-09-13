const path = require('path');
const fs = require('fs');
const http = require('http');
const { WebSocketServer } = require('ws');
const crypto = require('crypto');

function getArg(name, def) {
  const idx = process.argv.indexOf(`--${name}`);
  if (idx !== -1 && process.argv[idx + 1]) return process.argv[idx + 1];
  return def;
}

const PORT = parseInt(getArg('port', process.env.PORT || '3000'), 10);
const MAX_SESSIONS = parseInt(getArg('max-sessions', '500'), 10);

function log(...args) {
  console.log('[server]', ...args);
}

const sessions = new Map();

function sendToBrowser(session, obj) {
  if (session.ws && session.ws.readyState === 1) {
    try {
      session.ws.send(JSON.stringify(obj));
    } catch (e) {
      log('error sending to session', session.id.slice(0, 6), '-', e.message);
    }
  }
}

function getSession(id) {
  return sessions.get(id);
}

function iAmInitiator(myId, otherId) {
  return myId < otherId;
}

function tryRandomMatch(session) {
  if (session.currentPartner) {
    sendToBrowser(session, { type: 'already-matched' });
    return;
  }
  session.wantsMatch = true;

  const candidates = Array.from(sessions.values()).filter((s) => {
    if (s.id === session.id) return false;
    if (!s.wantsMatch) return false;
    if (s.currentPartner) return false;
    if (session.pairedWith.has(s.id)) return false;
    return true;
  });

  if (candidates.length === 0) {
    sendToBrowser(session, { type: 'no-peers-yet' });
    return;
  }

  const partner = candidates[Math.floor(Math.random() * candidates.length)];

  session.currentPartner = partner.id;
  partner.currentPartner = session.id;
  remember(session, partner.id);
  remember(partner, session.id);

  log(`paired: ${session.id.slice(0, 6)} <-> ${partner.id.slice(0, 6)} (active sessions: ${sessions.size})`);

  sendToBrowser(session, {
    type: 'matched',
    peerId: partner.id.slice(0, 8),
    initiator: iAmInitiator(session.id, partner.id),
  });
  sendToBrowser(partner, {
    type: 'matched',
    peerId: session.id.slice(0, 8),
    initiator: iAmInitiator(partner.id, session.id),
  });
}

const MAX_REMEMBERED_PARTNERS = 20;
function remember(session, partnerId) {
  session.pairedWith.add(partnerId);
  if (session.pairedWith.size > MAX_REMEMBERED_PARTNERS) {
    const oldest = session.pairedWith.values().next().value;
    session.pairedWith.delete(oldest);
  }
}

function leaveCurrentPartner(session) {
  if (session.currentPartner) {
    const partner = getSession(session.currentPartner);
    if (partner) {
      partner.currentPartner = null;
      sendToBrowser(partner, { type: 'partner-left' });
    }
  }
  session.currentPartner = null;
  session.wantsMatch = false;
}

const PUBLIC_DIR = __dirname;

const MIME_TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
};

function serveStaticFile(req, res) {
  let urlPath = decodeURIComponent(req.url.split('?')[0]);
  if (urlPath === '/') urlPath = '/index.html';

  const ALLOWED = ['/index.html'];
  if (!ALLOWED.includes(urlPath)) {
    res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('Not found');
    return;
  }

  const filePath = path.join(PUBLIC_DIR, urlPath);

  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end('Not found');
      return;
    }
    const ext = path.extname(filePath);
    res.writeHead(200, { 'Content-Type': MIME_TYPES[ext] || 'application/octet-stream' });
    res.end(data);
  });
}

const server = http.createServer((req, res) => {
  serveStaticFile(req, res);
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
  if (sessions.size >= MAX_SESSIONS) {
    log('session limit reached, refusing new connection');
    ws.send(JSON.stringify({ type: 'server-full' }));
    ws.close();
    return;
  }

  const session = {
    id: crypto.randomBytes(8).toString('hex'),
    ws,
    currentPartner: null,
    wantsMatch: false,
    pairedWith: new Set(),
    isAlive: true,
  };
  sessions.set(session.id, session);
  log('new tab connected, session', session.id.slice(0, 6), `(total: ${sessions.size})`);

  ws.on('pong', () => { session.isAlive = true; });

  ws.send(JSON.stringify({ type: 'ready', sessionId: session.id }));

  ws.on('message', (raw) => {
    const now = Date.now();
    session.msgTimestamps = (session.msgTimestamps || []).filter((t) => now - t < 1000);
    session.msgTimestamps.push(now);
    if (session.msgTimestamps.length > 30) {
      return;
    }

    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch (e) {
      return;
    }

    try {
      if (msg.type === 'find-random') {
        tryRandomMatch(session);
        return;
      }

      if (msg.type === 'signal') {
        if (session.currentPartner) {
          const partner = getSession(session.currentPartner);
          if (partner) {
            sendToBrowser(partner, { type: 'signal', payload: msg.payload });
          }
        }
        return;
      }

      if (msg.type === 'leave') {
        leaveCurrentPartner(session);
        return;
      }
    } catch (e) {
      log('error processing message from session', session.id.slice(0, 6), '-', e.message);
    }
  });

  ws.on('close', () => {
    log('tab disconnected, session', session.id.slice(0, 6));
    leaveCurrentPartner(session);
    sessions.delete(session.id);
  });
});

server.listen(PORT, () => {
  log(`page available at http://localhost:${PORT}`);
  log(`open this same URL in another tab (or another device on the network) to test`);
});

const HEARTBEAT_INTERVAL_MS = 30000;
setInterval(() => {
  for (const session of sessions.values()) {
    if (session.isAlive === false) {
      log('unresponsive session, terminating:', session.id.slice(0, 6));
      leaveCurrentPartner(session);
      sessions.delete(session.id);
      try { session.ws.terminate(); } catch (e) {}
      continue;
    }
    session.isAlive = false;
    try { session.ws.ping(); } catch (e) {}
  }
}, HEARTBEAT_INTERVAL_MS);
