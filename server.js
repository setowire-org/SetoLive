const path = require('path');
const fs = require('fs');
const http = require('http');
const { WebSocketServer } = require('ws');
const crypto = require('crypto');
const Swarm = require('./setowire');

function getArg(name, def) {
  const idx = process.argv.indexOf(`--${name}`);
  if (idx !== -1 && process.argv[idx + 1]) return process.argv[idx + 1];
  return def;
}

const PORT = parseInt(getArg('port', process.env.PORT || '3000'), 10);
const MAX_SESSIONS = parseInt(getArg('max-sessions', '500'), 10);

function log(...args) {
  console.log('[setolive]', ...args);
}

const SWARM_TOPIC = crypto.createHash('sha256').update('setolive-pairing-v1').digest();

const bootstrap = (process.env.SETOLIVE_BOOTSTRAP || '')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);

const swarm = new Swarm({
  seed: process.env.SETOLIVE_SEED || undefined,
  bootstrap,
});

let swarmReady = false;

swarm.join(SWARM_TOPIC, { announce: true, lookup: true }).ready().then(() => {
  swarmReady = true;
  log(`swarm ready, local id: ${swarm._id.slice(0, 8)}, nat: ${swarm.natType}`);
  broadcastFreeSessions();
}).catch((e) => {
  log('swarm did not become ready (falling back to local-only mode):', e && e.message);
});

const remoteFreeSessions = new Map();
const REMOTE_SESSION_TTL_MS = 15000;

function cleanupRemoteFree() {
  const now = Date.now();
  for (const [rid, info] of remoteFreeSessions) {
    if (now - info.seenAt > REMOTE_SESSION_TTL_MS) remoteFreeSessions.delete(rid);
  }
}

function swarmSend(peer, obj) {
  try {
    peer.write(Buffer.from(JSON.stringify(obj)));
  } catch (e) {
    log('error sending over swarm:', e.message);
  }
}

function broadcastFreeSessions() {
  if (!swarmReady) return;
  const ids = Array.from(sessions.values())
    .filter((s) => s.wantsMatch && !s.currentPartner)
    .map((s) => s.id);
  if (ids.length === 0) return;
  try {
    swarm.broadcast(Buffer.from(JSON.stringify({ kind: 'free-sessions', ids })));
  } catch (e) {
    log('error broadcasting free sessions:', e.message);
  }
}

setInterval(() => {
  cleanupRemoteFree();
  broadcastFreeSessions();
}, 4000);

const pendingClaims = new Map();

swarm.on('data', (data, peer) => {
  let msg;
  try {
    msg = JSON.parse(data.toString());
  } catch (e) {
    return;
  }

  try {
    if (msg.kind === 'free-sessions' && Array.isArray(msg.ids)) {
      const now = Date.now();
      for (const rid of msg.ids) {
        if (sessions.has(rid)) continue;
        remoteFreeSessions.set(rid, { peerSwarmId: peer.id, seenAt: now });
      }
      return;
    }

    if (msg.kind === 'claim') {
      const local = getSession(msg.sessionId);
      const accepted = !!local && local.wantsMatch && !local.currentPartner;
      if (accepted) {
        const reservation = 'pending-remote:' + msg.requestId;
        local.currentPartner = reservation;
        local._pendingClaimPeer = peer.id;
        setTimeout(() => {
          if (local.currentPartner === reservation) {
            local.currentPartner = null;
            delete local._pendingClaimPeer;
          }
        }, 4000);
      }
      swarmSend(peer, {
        kind: 'claim-ack',
        requestId: msg.requestId,
        accepted,
        localSessionId: msg.sessionId,
      });
      return;
    }

    if (msg.kind === 'claim-ack') {
      const pending = pendingClaims.get(msg.requestId);
      if (!pending) return;
      pendingClaims.delete(msg.requestId);
      clearTimeout(pending.timer);

      const { session } = pending;
      if (!session || session.currentPartner) return;

      if (!msg.accepted) {
        tryRandomMatch(session);
        return;
      }

      session.currentPartner = { remote: true, peerSwarmId: peer.id, remoteSessionId: msg.localSessionId };
      remember(session, msg.localSessionId);
      remoteFreeSessions.delete(msg.localSessionId);

      log(`paired (cross-instance): ${session.id.slice(0, 6)} <-> ${msg.localSessionId.slice(0, 6)} via peer ${peer.id.slice(0, 8)}`);

      sendToBrowser(session, {
        type: 'matched',
        peerId: msg.localSessionId.slice(0, 8),
        initiator: iAmInitiator(session.id, msg.localSessionId),
      });

      swarmSend(peer, {
        kind: 'claim-confirm',
        requestId: msg.requestId,
        localSessionId: msg.localSessionId,
        remoteSessionId: session.id,
      });
      return;
    }

    if (msg.kind === 'claim-confirm') {
      const local = getSession(msg.localSessionId);
      if (!local) return;
      if (local.currentPartner !== 'pending-remote:' + msg.requestId) return;

      local.currentPartner = { remote: true, peerSwarmId: peer.id, remoteSessionId: msg.remoteSessionId };
      delete local._pendingClaimPeer;
      remember(local, msg.remoteSessionId);

      log(`paired (cross-instance, confirmed): ${local.id.slice(0, 6)} <-> ${msg.remoteSessionId.slice(0, 6)} via peer ${peer.id.slice(0, 8)}`);

      sendToBrowser(local, {
        type: 'matched',
        peerId: msg.remoteSessionId.slice(0, 8),
        initiator: iAmInitiator(local.id, msg.remoteSessionId),
      });
      return;
    }

    if (msg.kind === 'signal') {
      const local = getSession(msg.toSessionId);
      if (local) {
        sendToBrowser(local, { type: 'signal', payload: msg.payload });
      }
      return;
    }

    if (msg.kind === 'partner-left') {
      const local = getSession(msg.toSessionId);
      if (local) {
        local.currentPartner = null;
        local.wantsMatch = false;
        sendToBrowser(local, { type: 'partner-left' });
      }
      return;
    }
  } catch (e) {
    log('error processing swarm message:', e.message);
  }
});

swarm.on('disconnect', (peerSwarmId) => {
  for (const session of sessions.values()) {
    if (
      session.currentPartner &&
      typeof session.currentPartner === 'object' &&
      session.currentPartner.remote &&
      session.currentPartner.peerSwarmId === peerSwarmId
    ) {
      session.currentPartner = null;
      session.wantsMatch = false;
      sendToBrowser(session, { type: 'partner-left' });
    }
  }
});

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

  const localCandidates = Array.from(sessions.values()).filter((s) => {
    if (s.id === session.id) return false;
    if (!s.wantsMatch) return false;
    if (s.currentPartner) return false;
    if (session.pairedWith.has(s.id)) return false;
    return true;
  });

  if (localCandidates.length > 0) {
    const partner = localCandidates[Math.floor(Math.random() * localCandidates.length)];

    session.currentPartner = partner.id;
    partner.currentPartner = session.id;
    remember(session, partner.id);
    remember(partner, session.id);

    log(`paired (local): ${session.id.slice(0, 6)} <-> ${partner.id.slice(0, 6)} (active sessions here: ${sessions.size})`);

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
    return;
  }

  cleanupRemoteFree();
  const remoteCandidates = Array.from(remoteFreeSessions.entries()).filter(
    ([rid]) => !session.pairedWith.has(rid)
  );

  if (remoteCandidates.length > 0 && swarmReady) {
    const [remoteSessionId, info] = remoteCandidates[Math.floor(Math.random() * remoteCandidates.length)];
    const peer = swarm.peers.find((p) => p.id === info.peerSwarmId);

    if (peer) {
      const requestId = crypto.randomBytes(8).toString('hex');
      const timer = setTimeout(() => {
        pendingClaims.delete(requestId);
        if (!session.currentPartner) tryRandomMatch(session);
      }, 3000);

      pendingClaims.set(requestId, { session, timer });
      swarmSend(peer, { kind: 'claim', sessionId: remoteSessionId, requestId });
      return;
    }
  }

  sendToBrowser(session, { type: 'no-peers-yet' });
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
  const cp = session.currentPartner;
  if (cp) {
    if (typeof cp === 'object' && cp.remote) {
      const peer = swarm.peers.find((p) => p.id === cp.peerSwarmId);
      if (peer) swarmSend(peer, { kind: 'partner-left', toSessionId: cp.remoteSessionId });
    } else if (typeof cp === 'string' && cp.startsWith('pending-remote:')) {
    } else if (typeof cp === 'string') {
      const partner = getSession(cp);
      if (partner) {
        partner.currentPartner = null;
        partner.wantsMatch = false;
        sendToBrowser(partner, { type: 'partner-left' });
      }
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
  log('new tab connected, session', session.id.slice(0, 6), `(total on this instance: ${sessions.size})`);

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
        const cp = session.currentPartner;
        if (cp) {
          if (typeof cp === 'object' && cp.remote) {
            const peer = swarm.peers.find((p) => p.id === cp.peerSwarmId);
            if (peer) {
              swarmSend(peer, {
                kind: 'signal',
                toSessionId: cp.remoteSessionId,
                fromSessionId: session.id,
                payload: msg.payload,
              });
            }
          } else if (typeof cp === 'string') {
            const partner = getSession(cp);
            if (partner) sendToBrowser(partner, { type: 'signal', payload: msg.payload });
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
  log('open this same URL in another tab (or another device on the network) to test');
  log('cross-instance pairing via P2P swarm: ' + (swarmReady ? 'already ready' : 'connecting...'));
});

const HEARTBEAT_INTERVAL_MS = 30000;
setInterval(() => {
  for (const session of sessions.values()) {
    if (session.isAlive === false) {
      log('unresponsive session, closing:', session.id.slice(0, 6));
      leaveCurrentPartner(session);
      sessions.delete(session.id);
      try { session.ws.terminate(); } catch (e) {}
      continue;
    }
    session.isAlive = false;
    try { session.ws.ping(); } catch (e) {}
  }
}, HEARTBEAT_INTERVAL_MS);

function shutdown() {
  log('shutting down...');
  for (const session of sessions.values()) {
    leaveCurrentPartner(session);
  }
  swarm.destroy().finally(() => process.exit(0));
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
          
