const http = require('http');
const crypto = require('crypto');
const Swarm = require('setowire');

const PORT = parseInt(process.env.PORT || '3001', 10);

// --- exatamente o exemplo do README, sem nada a mais ---
const swarm = new Swarm();
const topic = crypto.createHash('sha256').update('minitest-topic-v1').digest();

const log = [];
function addLog(line) {
  const entry = `[${new Date().toISOString().slice(11, 19)}] ${line}`;
  console.log(entry);
  log.push(entry);
  if (log.length > 200) log.shift();
}

swarm.join(topic, { announce: true, lookup: true });

swarm.on('connection', (peer) => {
  addLog(`CONNECTION event - peer ${String(peer.id).slice(0, 12)}`);
  peer.write(Buffer.from('hello from ' + swarm._id.slice(0, 8)));
});

swarm.on('data', (data, peer) => {
  addLog(`DATA recebido de ${String(peer?.id).slice(0, 12)}: ${data.toString().slice(0, 100)}`);
});

swarm.on('disconnect', (peerId) => {
  addLog(`DISCONNECT - peer ${String(peerId).slice(0, 12)}`);
});

swarm.on('nat', () => {
  addLog(`NAT event - natType agora: ${swarm.natType}, ext: ${swarm.publicAddress}`);
});

addLog('processo iniciado, entrando no swarm...');

// --- só um HTTP simples pra visualizar o estado, nada mais ---
const server = http.createServer((req, res) => {
  const peersInfo = swarm.peers.map(p => ({
    id: String(p.id).slice(0, 16),
    open: p._open,
  }));

  const html = `<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>minitest</title>
<style>
  body{font-family:monospace;background:#111;color:#ddd;padding:16px;font-size:13px}
  .big{font-size:16px;color:#5f5;margin-bottom:12px}
  pre{background:#000;padding:10px;border:1px solid #333;white-space:pre-wrap;max-height:70vh;overflow-y:auto}
</style></head>
<body>
<div class="big">
  meu id: ${swarm._id.slice(0, 16)} |
  natType: ${swarm.natType} |
  ext: ${swarm.publicAddress || 'nenhum ainda'} |
  peers conectados agora: ${swarm.peers.length}
</div>
<div>peers: ${JSON.stringify(peersInfo)}</div>
<h3>log</h3>
<pre>${log.join('\n')}</pre>
<script>setTimeout(() => location.reload(), 3000);</script>
</body></html>`;

  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
  res.end(html);
});

server.listen(PORT, () => {
  addLog(`http rodando em http://localhost:${PORT}`);
});
                                 
