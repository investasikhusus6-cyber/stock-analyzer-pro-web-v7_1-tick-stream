'use strict';
/**
 * WebSocket tick relay — Finnhub upstream.
 * Browser never sees the API key. Client protocol stays the same as before:
 *   subscribe: { action: "subscribe", symbols: ["EURUSD"], kind: "forex"|"stock" }
 *   tick:      { type: "price", symbol: "EURUSD", price: 1.08, timestamp: 1710000000 }
 *   status:    { type: "status", state: "live"|"wait"|"off", message: "..." }
 */
const http = require('http');
const WebSocket = require('ws');

const PORT = Number(process.env.PORT || 8080);
const API_KEY = (process.env.FINNHUB_API_KEY || process.env.TWELVEDATA_API_KEY || '').trim();
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || '')
  .split(',')
  .map((x) => x.trim())
  .filter(Boolean);

const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'content-type': 'application/json' });
    return res.end(
      JSON.stringify({
        ok: true,
        provider: 'Finnhub WebSocket',
        configured: !!API_KEY,
      })
    );
  }
  res.writeHead(404);
  res.end('Not found');
});

const wss = new WebSocket.Server({ server, path: '/stream' });

let upstream = null;
let upstreamReady = false;
let reconnectTimer = null;

/** client display symbol -> Set(client) */
const subscriptions = new Map();
/** client display symbol -> Finnhub upstream symbol */
const toUpstream = new Map();
/** Finnhub upstream symbol -> client display symbol */
const fromUpstream = new Map();

const clients = new Set();

function send(client, obj) {
  if (client.readyState === WebSocket.OPEN) client.send(JSON.stringify(obj));
}

function broadcastStatus(state, message) {
  clients.forEach((c) => send(c, { type: 'status', state, message }));
}

/**
 * Normalize client input to a display symbol + Finnhub subscribe symbol.
 * Forex majors → OANDA:EUR_USD style. Stocks stay plain (AAPL) or pass through.
 */
function mapSymbol(raw, kind) {
  let s = String(raw || '').trim().toUpperCase();
  if (!s) return null;

  // Already Finnhub-style
  if (s.includes(':')) {
    const display = s.includes('_')
      ? s.split(':').pop().replace('_', '')
      : s.replace(':', '');
    return { display: display || s, upstream: s };
  }

  // EUR/USD or EUR-USD
  s = s.replace(/[-/]/g, '');

  // Metals
  if (s === 'XAUUSD' || s === 'GOLD') {
    return { display: 'XAUUSD', upstream: 'OANDA:XAU_USD' };
  }
  if (s === 'XAGUSD' || s === 'SILVER') {
    return { display: 'XAGUSD', upstream: 'OANDA:XAG_USD' };
  }

  // 6-letter FX pair (EURUSD, USDJPY, …)
  if (/^[A-Z]{6}$/.test(s) && kind !== 'stock') {
    const base = s.slice(0, 3);
    const quote = s.slice(3);
    return { display: s, upstream: 'OANDA:' + base + '_' + quote };
  }

  // Indonesian Yahoo-style → try IDX (may not be on free Finnhub)
  if (s.endsWith('.JK')) {
    const code = s.replace(/\.JK$/i, '');
    return { display: code + ':IDX', upstream: code + '.JK' };
  }
  if (s.endsWith(':IDX')) {
    return { display: s, upstream: s.replace(':IDX', '.JK') };
  }

  // Plain equity ticker
  return { display: s, upstream: s };
}

function allUpstreamSymbols() {
  return [...toUpstream.values()];
}

function subscribeUpstream() {
  if (!upstream || upstream.readyState !== WebSocket.OPEN) return;
  const symbols = allUpstreamSymbols();
  for (const sym of symbols) {
    try {
      upstream.send(JSON.stringify({ type: 'subscribe', symbol: sym }));
    } catch (_) {}
  }
}

function connectUpstream() {
  if (!API_KEY) {
    broadcastStatus('off', 'Relay belum dikonfigurasi: set FINNHUB_API_KEY di Railway.');
    return;
  }
  if (upstream && (upstream.readyState === WebSocket.OPEN || upstream.readyState === WebSocket.CONNECTING)) {
    return;
  }
  upstreamReady = false;
  upstream = new WebSocket('wss://ws.finnhub.io?token=' + encodeURIComponent(API_KEY));

  upstream.on('open', () => {
    upstreamReady = true;
    broadcastStatus('wait', 'Terhubung ke Finnhub; menunggu tick.');
    subscribeUpstream();
  });

  upstream.on('message', (raw) => {
    let m;
    try {
      m = JSON.parse(raw.toString());
    } catch {
      return;
    }

    // Finnhub trade stream
    if (m.type === 'trade' && Array.isArray(m.data)) {
      for (const t of m.data) {
        if (!t || !Number.isFinite(Number(t.p))) continue;
        const upSym = String(t.s || '');
        const display = fromUpstream.get(upSym) || upSym;
        const price = Number(t.p);
        const ts = t.t ? Math.floor(Number(t.t) / 1000) : Math.floor(Date.now() / 1000);
        const tick = { type: 'price', symbol: display, price, timestamp: ts };
        const watchers = subscriptions.get(display);
        if (watchers && watchers.size) {
          watchers.forEach((c) => send(c, tick));
        } else {
          clients.forEach((c) => send(c, tick));
        }
      }
      return;
    }

    if (m.type === 'ping') {
      try {
        upstream.send(JSON.stringify({ type: 'pong' }));
      } catch (_) {}
      return;
    }

    if (m.type === 'error' || m.msg) {
      broadcastStatus(
        'off',
        'Finnhub: ' + (m.msg || m.message || 'langganan ditolak. Cek simbol, kuota, atau paket free.')
      );
    }
  });

  upstream.on('close', () => {
    upstreamReady = false;
    upstream = null;
    broadcastStatus('off', 'Koneksi Finnhub terputus; mencoba ulang…');
    clearTimeout(reconnectTimer);
    reconnectTimer = setTimeout(connectUpstream, 3000);
  });

  upstream.on('error', () => {
    broadcastStatus('off', 'Tidak dapat terhubung ke Finnhub. Periksa API key dan jaringan.');
  });
}

wss.on('connection', (client, req) => {
  const origin = req.headers.origin;
  if (ALLOWED_ORIGINS.length && origin && !ALLOWED_ORIGINS.includes(origin)) {
    client.close(1008, 'Origin not allowed');
    return;
  }

  client.symbols = new Set();
  clients.add(client);
  send(client, { type: 'status', state: 'wait', message: 'Relay tersambung; menyiapkan feed Finnhub.' });

  client.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }

    if (msg.action === 'subscribe') {
      const requested = Array.isArray(msg.symbols) ? msg.symbols : [];
      const kind = msg.kind || 'forex';
      for (const rawSymbol of requested.slice(0, 20)) {
        const mapped = mapSymbol(rawSymbol, kind);
        if (!mapped) continue;
        const display = mapped.display;
        const up = mapped.upstream;
        if (!subscriptions.has(display)) subscriptions.set(display, new Set());
        subscriptions.get(display).add(client);
        client.symbols.add(display);
        toUpstream.set(display, up);
        fromUpstream.set(up, display);
      }
      connectUpstream();
      if (upstreamReady) subscribeUpstream();
    } else if (msg.action === 'unsubscribe') {
      for (const rawSymbol of msg.symbols || []) {
        const mapped = mapSymbol(rawSymbol, msg.kind || 'forex');
        if (!mapped) continue;
        const display = mapped.display;
        const up = mapped.upstream;
        subscriptions.get(display)?.delete(client);
        client.symbols.delete(display);
        if (!subscriptions.get(display)?.size) {
          subscriptions.delete(display);
          toUpstream.delete(display);
          fromUpstream.delete(up);
          if (upstream && upstream.readyState === WebSocket.OPEN) {
            try {
              upstream.send(JSON.stringify({ type: 'unsubscribe', symbol: up }));
            } catch (_) {}
          }
        }
      }
    }
  });

  client.on('close', () => {
    clients.delete(client);
    for (const display of client.symbols) {
      subscriptions.get(display)?.delete(client);
      if (!subscriptions.get(display)?.size) {
        const up = toUpstream.get(display);
        subscriptions.delete(display);
        toUpstream.delete(display);
        if (up) fromUpstream.delete(up);
        if (upstream && upstream.readyState === WebSocket.OPEN && up) {
          try {
            upstream.send(JSON.stringify({ type: 'unsubscribe', symbol: up }));
          } catch (_) {}
        }
      }
    }
  });
});

setInterval(() => {
  if (upstream && upstream.readyState === WebSocket.OPEN) {
    try {
      upstream.send(JSON.stringify({ type: 'ping' }));
    } catch (_) {}
  }
}, 25000).unref();

server.listen(PORT, '0.0.0.0', () => {
  console.log('Finnhub tick relay listening on ' + PORT);
});
