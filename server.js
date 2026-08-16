// matrix-chat relay
//
// Extremely small WebSocket server whose only job is to pair exactly two
// authenticated clients and forward messages between them. No storage, no
// history, nothing logged beyond connect/disconnect. If a third client
// tries to join with the right token, it is rejected until a seat frees up.

const http = require('http');
const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;
const TOKEN = process.env.CHAT_TOKEN;

if (!TOKEN) {
  console.error('CHAT_TOKEN environment variable is not set. Refusing to start.');
  process.exit(1);
}

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('matrix-chat relay: ok\n');
});

const wss = new WebSocket.Server({ server });

/** @type {WebSocket[]} */
let clients = [];

function send(ws, obj) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

function broadcastToOthers(sender, obj) {
  for (const c of clients) {
    if (c !== sender) send(c, obj);
  }
}

wss.on('connection', (ws) => {
  ws.authed = false;
  ws.name = null;

  const authTimeout = setTimeout(() => {
    if (!ws.authed) ws.close();
  }, 10_000);

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    if (!ws.authed) {
      if (msg.type === 'auth' && msg.token === TOKEN) {
        if (clients.length >= 2) {
          send(ws, { type: 'error', text: 'Room is full.' });
          ws.close();
          return;
        }
        clearTimeout(authTimeout);
        ws.authed = true;
        ws.name = String(msg.name || 'anon').slice(0, 32);
        const peerOnline = clients.length > 0;
        clients.push(ws);
        send(ws, { type: 'auth_ok', peerOnline });
        broadcastToOthers(ws, { type: 'system', text: `${ws.name} has connected.` });
      } else {
        send(ws, { type: 'error', text: 'Bad token.' });
        ws.close();
      }
      return;
    }

    if (msg.type === 'msg') {
      const text = String(msg.text || '').slice(0, 2000);
      if (text.length === 0) return;
      broadcastToOthers(ws, { type: 'msg', from: ws.name, text });
    }
  });

  ws.on('close', () => {
    clearTimeout(authTimeout);
    clients = clients.filter((c) => c !== ws);
    if (ws.authed) {
      broadcastToOthers(ws, { type: 'system', text: `${ws.name} has disconnected.` });
    }
  });
});

server.listen(PORT, () => {
  console.log(`relay listening on :${PORT}`);
});
