'use strict';
const http = require('http');
const WebSocket = require('ws');
const PORT = Number(process.env.PORT || 8080);
const API_KEY = process.env.TWELVEDATA_API_KEY;
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || '').split(',').map(x => x.trim()).filter(Boolean);
const server = http.createServer((req, res) => {
  if (req.url === '/health') { res.writeHead(200, {'content-type':'application/json'}); return res.end(JSON.stringify({ok:true, provider:'Twelve Data WebSocket', configured:!!API_KEY})); }
  res.writeHead(404); res.end('Not found');
});
const wss = new WebSocket.Server({server, path:'/stream'});
let upstream = null, upstreamReady = false, reconnectTimer = null;
const clients = new Set();
const subscriptions = new Map(); // symbol -> Set(client)
function send(client, obj) { if (client.readyState === WebSocket.OPEN) client.send(JSON.stringify(obj)); }
function normalizeSymbol(s) {
  s = String(s || '').trim().toUpperCase();
  // Twelve Data uses EUR/USD for FX and e.g. BBCA:IDX for Indonesian listings (if plan supports it).
  if (/^[A-Z]{6}$/.test(s) && !['XAUUSD','XAGUSD'].includes(s)) s = s.slice(0,3)+'/'+s.slice(3);
  if (s === 'XAUUSD') s = 'XAU/USD';
  if (s === 'XAGUSD') s = 'XAG/USD';
  return s;
}
function allSymbols() { return [...subscriptions.keys()].filter(s => subscriptions.get(s)?.size); }
function subscribeUpstream() {
  if (!upstream || upstream.readyState !== WebSocket.OPEN) return;
  const symbols = allSymbols();
  if (symbols.length) upstream.send(JSON.stringify({action:'subscribe', params:{symbols:symbols.join(',')}}));
}
function connectUpstream() {
  if (!API_KEY) { clients.forEach(c => send(c,{type:'status',state:'off',message:'Relay belum dikonfigurasi: set TWELVEDATA_API_KEY di Railway.'})); return; }
  if (upstream && (upstream.readyState === WebSocket.OPEN || upstream.readyState === WebSocket.CONNECTING)) return;
  upstreamReady = false;
  upstream = new WebSocket('wss://ws.twelvedata.com/v1/quotes/price?apikey='+encodeURIComponent(API_KEY));
  upstream.on('open', () => { upstreamReady = true; clients.forEach(c=>send(c,{type:'status',state:'wait',message:'Terhubung ke Twelve Data; menunggu tick.'})); subscribeUpstream(); });
  upstream.on('message', raw => {
    let m; try { m = JSON.parse(raw.toString()); } catch { return; }
    if (m.event === 'price' || m.type === 'price' || m.price !== undefined && m.symbol) {
      const symbol = normalizeSymbol(m.symbol || m.meta?.symbol);
      const tick = {type:'price', symbol, price:Number(m.price), timestamp:Number(m.timestamp || m.time_stamp || Date.now()/1000), volume:m.day_volume ?? null};
      const listeners = subscriptions.get(symbol);
      if (listeners) listeners.forEach(c => send(c,tick));
      // Keep an alternate symbol key if provider preserves a different slash convention.
      return;
    }
    if (m.event === 'subscribe-status' || m.status) {
      clients.forEach(c=>send(c,{type:'status',state:'wait',message:'Status langganan diterima dari penyedia data.'}));
    }
    if (m.event === 'heartbeat') { try { upstream.send(JSON.stringify({action:'heartbeat'})); } catch {} }
    if (m.status === 'error' || m.code) clients.forEach(c=>send(c,{type:'status',state:'off',message:'Penyedia data menolak langganan. Periksa paket, simbol, dan kuota WebSocket.'}));
  });
  upstream.on('close', () => { upstreamReady = false; upstream = null; clients.forEach(c=>send(c,{type:'status',state:'off',message:'Koneksi penyedia data terputus; mencoba ulang.'})); clearTimeout(reconnectTimer); reconnectTimer=setTimeout(connectUpstream,3000); });
  upstream.on('error', () => { clients.forEach(c=>send(c,{type:'status',state:'off',message:'Tidak dapat terhubung ke Twelve Data.'})); });
}
wss.on('connection', (client, req) => {
  const origin = req.headers.origin;
  if (ALLOWED_ORIGINS.length && origin && !ALLOWED_ORIGINS.includes(origin)) { client.close(1008,'Origin not allowed'); return; }
  client.symbols = new Set(); clients.add(client);
  send(client,{type:'status',state:'wait',message:'Relay tersambung; menyiapkan feed harga.'});
  client.on('message', raw => {
    let msg; try { msg = JSON.parse(raw.toString()); } catch { return; }
    if (msg.action === 'subscribe') {
      const requested = Array.isArray(msg.symbols) ? msg.symbols : [];
      for (const rawSymbol of requested.slice(0,20)) {
        const symbol = normalizeSymbol(rawSymbol); if (!symbol || symbol.length > 24) continue;
        if (!subscriptions.has(symbol)) subscriptions.set(symbol,new Set());
        subscriptions.get(symbol).add(client); client.symbols.add(symbol);
      }
      connectUpstream(); subscribeUpstream();
    } else if (msg.action === 'unsubscribe') {
      for (const rawSymbol of (msg.symbols || [])) { const symbol=normalizeSymbol(rawSymbol); subscriptions.get(symbol)?.delete(client); client.symbols.delete(symbol); }
      subscribeUpstream();
    }
  });
  client.on('close', () => { clients.delete(client); for (const s of client.symbols) { subscriptions.get(s)?.delete(client); if (!subscriptions.get(s)?.size) subscriptions.delete(s); } subscribeUpstream(); });
});
setInterval(() => { if (upstream && upstream.readyState === WebSocket.OPEN) { try { upstream.send(JSON.stringify({action:'heartbeat'})); } catch {} } },10000).unref();
server.listen(PORT,'0.0.0.0',()=>console.log(`Tick relay listening on ${PORT}`));
