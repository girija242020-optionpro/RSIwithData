import express from 'express';
import cors from 'cors';
import { WebSocketServer, WebSocket } from 'ws';
import webpush from 'web-push';
import { authenticator } from 'otplib';
import http from 'http';

const PORT = Number(process.env.PORT || 10000);
const HOST = process.env.HOST || '0.0.0.0';
const ORIGIN = process.env.CORS_ORIGIN || '*';
const CLIENT_ID = process.env.DHAN_CLIENT_ID || '';
const PIN = process.env.DHAN_PIN || '';
const TOTP_SECRET = process.env.DHAN_TOTP_SECRET || '';
let accessToken = process.env.DHAN_ACCESS_TOKEN || '';
let tokenRefreshTimer = null;
let bootRetryTimer = null;
let bootInProgress = false;
const TOKEN_RETRY_MS = Math.max(125000, Number(process.env.TOKEN_RETRY_MS || 130000));
const TOKEN_REFRESH_MARGIN_MS = Math.max(120000, Number(process.env.TOKEN_REFRESH_MARGIN_MS || 600000));
const NIFTY_ID = String(process.env.NIFTY_SECURITY_ID || '13');
const NIFTY_SEGMENT = process.env.NIFTY_SEGMENT || 'IDX_I';
const CHAIN_REFRESH_MS = Math.max(3000, Number(process.env.CHAIN_REFRESH_MS || 3200));
const DEPTH_CONTRACTS = Math.min(20, Math.max(2, Number(process.env.DEPTH_CONTRACTS || 20)));
const STALE_AFTER_MS = Number(process.env.STALE_AFTER_MS || 5000);
const MAX_CLIENTS = Number(process.env.MAX_CLIENTS || 30);
const TICK_BUFFER_SIZE = Number(process.env.TICK_BUFFER_SIZE || 20000);

const app = express();
app.use(cors({ origin: ORIGIN === '*' ? true : ORIGIN }));
app.use(express.json({ limit: '1mb' }));

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

const clients = new Set();
const ticks = [];
const lastTickByKey = new Map();
const depthByKey = new Map();
const chainSnapshots = new Map();
const subscriptions = new Map();
const pushSubscriptions = new Map();

const state = {
  startedAt: Date.now(),
  feedConnected: false,
  depthConnected: false,
  lastTickAt: 0,
  spot: null,
  expiry: null,
  chainUpdatedAt: 0,
  chainRows: [],
  flow: null,
  optionError: null,
  tokenExpiry: null,
  feedPackets: 0,
  depthPackets: 0,
  clientCount: 0
};

if (process.env.VAPID_PUBLIC_KEY && process.env.VAPID_PRIVATE_KEY && process.env.VAPID_SUBJECT) {
  webpush.setVapidDetails(process.env.VAPID_SUBJECT, process.env.VAPID_PUBLIC_KEY, process.env.VAPID_PRIVATE_KEY);
}

function num(v, d = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : d;
}
function clamp(v, a, b) { return Math.max(a, Math.min(b, v)); }
function mean(arr) { return arr.length ? arr.reduce((a,b)=>a+b,0)/arr.length : 0; }
function sum(arr) { return arr.reduce((a,b)=>a+b,0); }
function pct(a,b) { return b ? ((a-b)/Math.abs(b))*100 : 0; }
function nowIso() { return new Date().toISOString(); }
function broadcast(message) {
  const raw = JSON.stringify(message);
  for (const ws of clients) if (ws.readyState === WebSocket.OPEN) ws.send(raw);
}

async function dhanFetch(path, body = null) {
  if (!accessToken) throw new Error('Dhan access token is not configured');
  const r = await fetch(`https://api.dhan.co/v2${path}`, {
    method: body === null ? 'GET' : 'POST',
    headers: { 'Content-Type':'application/json', 'access-token':accessToken, 'client-id':CLIENT_ID },
    body: body === null ? undefined : JSON.stringify(body)
  });
  const text = await r.text();
  let data; try { data = JSON.parse(text); } catch { data = { raw:text }; }
  if (!r.ok) throw new Error(`${path} ${r.status}: ${JSON.stringify(data).slice(0,500)}`);
  return data;
}

function tokenExpiryMs() {
  const t = state.tokenExpiry ? Date.parse(state.tokenExpiry) : 0;
  return Number.isFinite(t) ? t : 0;
}

function scheduleTokenRefresh() {
  if (tokenRefreshTimer) clearTimeout(tokenRefreshTimer);
  const exp = tokenExpiryMs();
  if (!exp) return;
  const delay = Math.max(120000, exp - Date.now() - TOKEN_REFRESH_MARGIN_MS);
  tokenRefreshTimer = setTimeout(async () => {
    try {
      accessToken = '';
      await ensureToken(true);
      console.log('DHAN: access token refreshed automatically');
      if (bootRetryTimer) { clearTimeout(bootRetryTimer); bootRetryTimer = null; }
    } catch (e) {
      console.error('DHAN token refresh failed:', e.message);
      tokenRefreshTimer = setTimeout(() => ensureToken(true).catch(err => console.error('DHAN token refresh retry failed:', err.message)), TOKEN_RETRY_MS);
    }
  }, delay);
}

async function ensureToken(force = false) {
  const exp = tokenExpiryMs();
  if (!force && accessToken && (!exp || exp - Date.now() > TOKEN_REFRESH_MARGIN_MS)) return accessToken;
  if (!CLIENT_ID || !PIN || !TOTP_SECRET) throw new Error('Configure DHAN_ACCESS_TOKEN or DHAN_CLIENT_ID + DHAN_PIN + DHAN_TOTP_SECRET');
  const totp = authenticator.generate(TOTP_SECRET.replace(/\s+/g, ''));
  const url = `https://auth.dhan.co/app/generateAccessToken?dhanClientId=${encodeURIComponent(CLIENT_ID)}&pin=${encodeURIComponent(PIN)}&totp=${encodeURIComponent(totp)}`;
  const r = await fetch(url, { method:'POST' });
  const text = await r.text();
  let data; try { data = JSON.parse(text); } catch { data = { raw:text }; }
  if (!r.ok || !data.accessToken) {
    const msg = JSON.stringify(data).slice(0,500);
    const err = new Error(`Dhan token generation failed: ${msg}`);
    err.dhanData = data;
    throw err;
  }
  accessToken = data.accessToken;
  state.tokenExpiry = data.expiryTime || null;
  scheduleTokenRefresh();
  return accessToken;
}

async function loadExpiry() {
  await ensureToken();
  const data = await dhanFetch('/optionchain/expirylist', { UnderlyingScrip:Number(NIFTY_ID), UnderlyingSeg:NIFTY_SEGMENT });
  const list = data?.data || [];
  const today = new Date().toISOString().slice(0,10);
  const future = list.filter(x => String(x) >= today).sort();
  state.expiry = future[0] || list.sort().at(-1) || null;
  return state.expiry;
}

function normalizeOption(strike, side, row) {
  if (!row) return null;
  const g = row.greeks || {};
  return {
    strike:num(strike), side,
    securityId:String(row.security_id || ''),
    ltp:num(row.last_price), previousClose:num(row.previous_close_price),
    oi:num(row.oi), previousOi:num(row.previous_oi), volume:num(row.volume), previousVolume:num(row.previous_volume),
    averagePrice:num(row.average_price), bid:num(row.top_bid_price), ask:num(row.top_ask_price),
    bidQty:num(row.top_bid_quantity), askQty:num(row.top_ask_quantity),
    iv:num(row.implied_volatility), delta:num(g.delta), gamma:num(g.gamma), theta:num(g.theta), vega:num(g.vega)
  };
}

function enrich(rows) {
  return rows.map(r => {
    const key = `${r.side}:${r.strike}`;
    const prev = chainSnapshots.get(key);
    const dOi = prev ? r.oi - prev.oi : r.oi - r.previousOi;
    const dVol = prev ? r.volume - prev.volume : r.volume - r.previousVolume;
    const dPrem = prev ? r.ltp - prev.ltp : 0;
    const dIv = prev ? r.iv - prev.iv : 0;
    const dGamma = prev ? r.gamma - prev.gamma : 0;
    const dTheta = prev ? r.theta - prev.theta : 0;
    const spreadPct = r.bid > 0 && r.ask > 0 ? ((r.ask-r.bid)/r.ltp)*100 : 0;
    const depth = depthByKey.get(r.securityId);
    return { ...r, dOi, dVol, dPrem, dIv, dGamma, dTheta, spreadPct, depth:depth || null };
  });
}

function flowAnalytics(rows, spot) {
  if (!rows.length || !spot) return null;
  const strikes = [...new Set(rows.map(r=>r.strike))].sort((a,b)=>a-b);
  const atm = strikes.reduce((best,s)=>Math.abs(s-spot)<Math.abs(best-spot)?s:best,strikes[0]);
  const window = rows.filter(r => Math.abs(r.strike-atm) <= 150);
  const ce = window.filter(r=>r.side==='CE');
  const pe = window.filter(r=>r.side==='PE');
  const depthRows = window.map(r=>r.depth).filter(Boolean);
  const bidQty = sum(depthRows.map(d=>num(d.bidQty20)));
  const askQty = sum(depthRows.map(d=>num(d.askQty20)));
  const depthImbalance = (bidQty+askQty) ? (bidQty-askQty)/(bidQty+askQty) : 0;
  const callDOI = sum(ce.map(r=>r.dOi));
  const putDOI = sum(pe.map(r=>r.dOi));
  const callDVol = sum(ce.map(r=>r.dVol));
  const putDVol = sum(pe.map(r=>r.dVol));
  const callDPrem = mean(ce.map(r=>r.dPrem));
  const putDPrem = mean(pe.map(r=>r.dPrem));
  const callIV = mean(ce.map(r=>r.iv));
  const putIV = mean(pe.map(r=>r.iv));
  const callDIV = mean(ce.map(r=>r.dIv));
  const putDIV = mean(pe.map(r=>r.dIv));
  const callGamma = mean(ce.map(r=>r.gamma));
  const putGamma = mean(pe.map(r=>r.gamma));
  const callTheta = mean(ce.map(r=>r.theta));
  const putTheta = mean(pe.map(r=>r.theta));
  const callPremVol = mean(ce.filter(r=>r.dVol>0).map(r=>r.dPrem));
  const putPremVol = mean(pe.filter(r=>r.dVol>0).map(r=>r.dPrem));
  const callSupportProxy = clamp(((-callDOI/Math.max(1,Math.abs(callDOI))) + (putDOI/Math.max(1,Math.abs(putDOI))))*0.5,-1,1);
  const premiumBreadthCall = ce.length ? ce.filter(r=>r.dPrem>0 && r.dVol>0).length/ce.length : 0;
  const premiumBreadthPut = pe.length ? pe.filter(r=>r.dPrem>0 && r.dVol>0).length/pe.length : 0;
  const liquidity = clamp(depthImbalance,-1,1);
  const flow = {
    atm, windowStrikes:[atm-150,atm+150],
    call:{dOi:callDOI,dVol:callDVol,dPrem:callDPrem,iv:callIV,dIv:callDIV,gamma:callGamma,theta:callTheta,premiumBreadth:premiumBreadthCall,premiumOnVolume:callPremVol},
    put:{dOi:putDOI,dVol:putDVol,dPrem:putDPrem,iv:putIV,dIv:putDIV,gamma:putGamma,theta:putTheta,premiumBreadth:premiumBreadthPut,premiumOnVolume:putPremVol},
    depth:{bidQty20:bidQty,askQty20:askQty,imbalance:liquidity},
    positioning:{callDoi:callDOI,putDoi:putDOI,putVsCallDoi:putDOI-callDOI},
    interpretation:{
      callWritingProxy: callDOI>0 && callDPrem<=0,
      putWritingProxy: putDOI>0 && putDPrem<=0,
      callUnwindProxy: callDOI<0 && callDPrem>0,
      putUnwindProxy: putDOI<0 && putDPrem>0
    }
  };
  return flow;
}

async function refreshChain() {
  try {
    await ensureToken();
    if (!state.expiry) await loadExpiry();
    const data = await dhanFetch('/optionchain', { UnderlyingScrip:Number(NIFTY_ID), UnderlyingSeg:NIFTY_SEGMENT, Expiry:state.expiry });
    const oc = data?.data?.oc || {};
    state.spot = num(data?.data?.last_price, state.spot);
    const rows=[];
    for (const [strike, obj] of Object.entries(oc)) {
      const ce=normalizeOption(strike,'CE',obj.ce); const pe=normalizeOption(strike,'PE',obj.pe);
      if (ce) rows.push(ce); if (pe) rows.push(pe);
    }
    const enriched = enrich(rows);
    for (const r of rows) chainSnapshots.set(`${r.side}:${r.strike}`, r);
    state.chainRows = enriched;
    state.flow = flowAnalytics(enriched,state.spot);
    state.chainUpdatedAt=Date.now();
    await syncDepth(enriched);
    broadcast({type:'chain',data:{updatedAt:state.chainUpdatedAt,spot:state.spot,expiry:state.expiry,rows:enriched,flow:state.flow}});
  } catch (e) {
    state.optionError=e.message;
    broadcast({type:'error',scope:'option-chain',message:e.message});
  }
}

let marketWs=null;
let depthWs=null;
let reconnectTimer=null;
let depthReconnectTimer=null;

function parseMarketPacket(buf) {
  const b=Buffer.from(buf); if (b.length<8) return null;
  const code=b.readUInt8(0), len=b.readUInt16LE(1), segment=b.readUInt8(3), securityId=b.readInt32LE(4);
  const key=`${segment}:${securityId}`;
  if (code===8 && b.length>=162) {
    return {type:'tick',feedCode:code,segment,securityId:String(securityId),ltp:b.readFloatLE(8),ltq:b.readInt16LE(12),ltt:b.readInt32LE(14),atp:b.readFloatLE(18),volume:b.readInt32LE(22),sellQty:b.readInt32LE(26),buyQty:b.readInt32LE(30),open:b.readFloatLE(34),close:b.readFloatLE(38),high:b.readFloatLE(42),low:b.readFloatLE(46),bidQty:[b.readInt32LE(62),b.readInt32LE(82),b.readInt32LE(102),b.readInt32LE(122),b.readInt32LE(142)],askQty:[b.readInt32LE(66),b.readInt32LE(86),b.readInt32LE(106),b.readInt32LE(126),b.readInt32LE(146)],bidPrice:[b.readFloatLE(74),b.readFloatLE(94),b.readFloatLE(114),b.readFloatLE(134),b.readFloatLE(154)],askPrice:[b.readFloatLE(78),b.readFloatLE(98),b.readFloatLE(118),b.readFloatLE(138),b.readFloatLE(158)],key};
  }
  if (code===5 && b.length>=13) return {type:'oi',segment,securityId:String(securityId),oi:b.readInt32LE(8),key};
  if (code===2 && b.length>=12) return {type:'index',segment,securityId:String(securityId),ltp:b.readFloatLE(8),key};
  return null;
}

function connectMarket() {
  if (!accessToken || !CLIENT_ID) return;
  const url=`wss://api-feed.dhan.co?version=2&token=${encodeURIComponent(accessToken)}&clientId=${encodeURIComponent(CLIENT_ID)}&authType=2`;
  marketWs=new WebSocket(url);
  marketWs.binaryType='arraybuffer';
  marketWs.on('open',()=>{ state.feedConnected=true; broadcast({type:'status',data:{feedConnected:true}}); subscribeMarket(); });
  marketWs.on('message',data=>{ state.feedPackets++; const p=parseMarketPacket(data); if(!p) return; if(p.type==='tick'||p.type==='index'){ lastTickByKey.set(p.key,p); state.lastTickAt=Date.now(); const sid=String(p.securityId); if(sid===NIFTY_ID){state.spot=p.ltp; lastTickByKey.set(`spot:${NIFTY_ID}`,p);} if(p.type==='tick'){ticks.push({...p,receivedAt:Date.now()}); if(ticks.length>TICK_BUFFER_SIZE) ticks.splice(0,ticks.length-TICK_BUFFER_SIZE);} broadcast({type:'tick',data:{...p,receivedAt:Date.now()}}); } });
  marketWs.on('close',()=>{state.feedConnected=false; broadcast({type:'status',data:{feedConnected:false}}); clearTimeout(reconnectTimer); reconnectTimer=setTimeout(connectMarket,3000);});
  marketWs.on('error',()=>{});
}
function sendMarketSubscriptions(list) {
  if(!marketWs || marketWs.readyState!==WebSocket.OPEN || !list.length) return;
  for(let i=0;i<list.length;i+=100){ const part=list.slice(i,i+100); marketWs.send(JSON.stringify({RequestCode:21,InstrumentCount:part.length,InstrumentList:part})); }
}
function subscribeMarket(){
  const list=[{ExchangeSegment:NIFTY_SEGMENT,SecurityId:NIFTY_ID}];
  for(const r of state.chainRows) if(r.securityId) list.push({ExchangeSegment:'NSE_FNO',SecurityId:r.securityId});
  const uniq=[]; const seen=new Set(); for(const x of list){const k=x.ExchangeSegment+':'+x.SecurityId;if(!seen.has(k)){seen.add(k);uniq.push(x)}}
  sendMarketSubscriptions(uniq);
}

function parseDepth(buf) {
  const b=Buffer.from(buf); let off=0; const packets=[];
  while(off+12<=b.length){ const len=b.readUInt16LE(off); const code=b.readUInt8(off+2); const seg=b.readUInt8(off+3); const sid=String(b.readInt32LE(off+4)); const size=Math.max(12,len); if(off+size>b.length) break; if((code===41||code===51)&&size>=332){ let bidQty20=0,askQty20=0,bestBid=0,bestAsk=0; for(let i=0;i<20;i++){const p=off+12+i*16;const price=b.readDoubleLE(p);const qty=b.readUInt32LE(p+8);if(code===41){bidQty20+=qty;if(i===0)bestBid=price}else{askQty20+=qty;if(i===0)bestAsk=price}} packets.push({segment:seg,securityId:sid,side:code===41?'BID':'ASK',bidQty20,askQty20,bestBid,bestAsk});} off+=size; }
  return packets;
}
function mergeDepth(p){ const old=depthByKey.get(p.securityId)||{bidQty20:0,askQty20:0}; const x={...old,...p}; x.imbalance=(x.bidQty20+x.askQty20)?(x.bidQty20-x.askQty20)/(x.bidQty20+x.askQty20):0; depthByKey.set(p.securityId,x); }
async function syncDepth(rows){
  if(!accessToken||!CLIENT_ID)return;
  const candidates=[...rows].sort((a,b)=>Math.abs(a.strike-state.spot)-Math.abs(b.strike-state.spot)).slice(0,DEPTH_CONTRACTS);
  const instruments=candidates.map(r=>({ExchangeSegment:'NSE_FNO',SecurityId:r.securityId}));
  subscriptions.clear(); for(const x of instruments) subscriptions.set(`NSE_FNO:${x.SecurityId}`,x);
  if(depthWs && depthWs.readyState===WebSocket.OPEN){depthWs.send(JSON.stringify({RequestCode:23,InstrumentCount:instruments.length,InstrumentList:instruments}));return;}
  connectDepth(instruments);
}
function connectDepth(instruments){
  const url=`wss://depth-api-feed.dhan.co/twentydepth?token=${encodeURIComponent(accessToken)}&clientId=${encodeURIComponent(CLIENT_ID)}&authType=2`;
  depthWs=new WebSocket(url); depthWs.binaryType='arraybuffer';
  depthWs.on('open',()=>{state.depthConnected=true;broadcast({type:'status',data:{depthConnected:true}});if(instruments.length)depthWs.send(JSON.stringify({RequestCode:23,InstrumentCount:instruments.length,InstrumentList:instruments}));});
  depthWs.on('message',data=>{state.depthPackets++;for(const p of parseDepth(data))mergeDepth(p);});
  depthWs.on('close',()=>{state.depthConnected=false;broadcast({type:'status',data:{depthConnected:false}});clearTimeout(depthReconnectTimer);depthReconnectTimer=setTimeout(()=>connectDepth([...subscriptions.values()]),3000);});
  depthWs.on('error',()=>{});
}

function optionRowsForUi(){return state.chainRows.map(r=>({strike:r.strike,side:r.side,securityId:r.securityId,ltp:r.ltp,oi:r.oi,dOi:r.dOi,volume:r.volume,dVol:r.dVol,iv:r.iv,dIv:r.dIv,gamma:r.gamma,theta:r.theta,bid:r.bid,ask:r.ask,depth:r.depth}));}

app.get('/',(_,res)=>res.type('text').send('NIFTY Flow Evidence Backend ONLINE'));
app.get('/api/health',(_,res)=>res.json({ok:true,feedLive:state.feedConnected&&Date.now()-state.lastTickAt<STALE_AFTER_MS,lastTickAgeMs:state.lastTickAt?Date.now()-state.lastTickAt:null,depthConnected:state.depthConnected,chainUpdatedAt:state.chainUpdatedAt,clients:clients.size,packets:state.feedPackets,depthPackets:state.depthPackets,expiry:state.expiry,uptimeSec:Math.floor((Date.now()-state.startedAt)/1000),tokenConfigured:!!accessToken}));
app.get('/api/config',(_,res)=>res.json({backend:'NIFTY Flow Evidence',version:'1.0.0',features:['tick-by-tick','option-chain','OI','delta-OI','volume','delta-volume','IV','gamma','theta','20-level-depth','flow-meter','push'],defaults:{index:'NIFTY',timeframe:'5m',rsi1:5,rsi2:9,sma1:14,sma2:14,dema:14,oversold:30,middle:50,overbought:70},vapidPublicKey:process.env.VAPID_PUBLIC_KEY||null}));
app.get('/api/state',(_,res)=>res.json({spot:state.spot,expiry:state.expiry,feedConnected:state.feedConnected,lastTickAt:state.lastTickAt,chainUpdatedAt:state.chainUpdatedAt,flow:state.flow,optionError:state.optionError}));
app.get('/api/ticks',(req,res)=>{const n=Math.min(5000,Number(req.query.n||500));res.json(ticks.slice(-n));});
app.get('/api/option-chain',(_,res)=>res.json({spot:state.spot,expiry:state.expiry,updatedAt:state.chainUpdatedAt,rows:optionRowsForUi(),flow:state.flow,error:state.optionError}));
app.get('/api/analytics',(_,res)=>res.json({spot:state.spot,expiry:state.expiry,updatedAt:state.chainUpdatedAt,flow:state.flow,rows:optionRowsForUi()}));
app.get('/api/depth',(_,res)=>res.json([...depthByKey.entries()].map(([securityId,d])=>({securityId,...d}))));
app.get('/api/expiry',async(_,res)=>{try{await ensureToken();await loadExpiry();res.json({expiry:state.expiry})}catch(e){res.status(500).json({error:e.message})}});
app.get('/api/history',async(req,res)=>{try{await ensureToken();const body={securityId:String(req.query.securityId||NIFTY_ID),exchangeSegment:String(req.query.segment||NIFTY_SEGMENT),instrument:'INDEX',fromDate:req.query.fromDate||new Date(Date.now()-7*86400000).toISOString().slice(0,10),toDate:req.query.toDate||new Date().toISOString().slice(0,10)};const d=await dhanFetch('/charts/intraday',body);res.json(d)}catch(e){res.status(500).json({error:e.message})}});
app.post('/api/push/subscribe',(req,res)=>{const s=req.body;if(!s?.endpoint)return res.status(400).json({error:'endpoint required'});pushSubscriptions.set(s.endpoint,s);res.json({ok:true,count:pushSubscriptions.size})});
app.post('/api/push/test',async(_,res)=>{if(!process.env.VAPID_PUBLIC_KEY||!process.env.VAPID_PRIVATE_KEY)return res.status(400).json({ok:false,error:'VAPID not configured'});let sent=0,failed=0;for(const s of pushSubscriptions.values()){try{await webpush.sendNotification(s,JSON.stringify({title:'Flow Terminal Test',body:'Push is working.',tag:'flow-test'}));sent++}catch{failed++}}res.json({ok:true,sent,failed,count:pushSubscriptions.size})});
app.get('/api/push/public-key',(_,res)=>res.json({publicKey:process.env.VAPID_PUBLIC_KEY||null}));

wss.on('connection',(ws)=>{if(clients.size>=MAX_CLIENTS){ws.close(1013,'Too many clients');return;}clients.add(ws);state.clientCount=clients.size;ws.send(JSON.stringify({type:'hello',data:{version:'1.0.0',features:['tick','chain','flow','depth','push'],spot:state.spot,expiry:state.expiry}}));ws.send(JSON.stringify({type:'status',data:{feedConnected:state.feedConnected,depthConnected:state.depthConnected}}));if(state.chainUpdatedAt)ws.send(JSON.stringify({type:'chain',data:{updatedAt:state.chainUpdatedAt,spot:state.spot,expiry:state.expiry,rows:state.chainRows,flow:state.flow}}));ws.on('close',()=>{clients.delete(ws);state.clientCount=clients.size});});

async function boot(){
  if (bootInProgress) return;
  bootInProgress = true;
  try {
    await ensureToken();
    await loadExpiry();
    connectMarket();
    connectDepth([]);
    setTimeout(refreshChain,500);
    setInterval(refreshChain,CHAIN_REFRESH_MS);
    console.log(`DHAN: authenticated; token expiry=${state.tokenExpiry || 'unknown'}`);
  } catch (e) {
    console.error('BOOT:', e.message);
    console.error(`BOOT: next authentication retry in ${Math.round(TOKEN_RETRY_MS/1000)} seconds (Dhan token generation is rate-limited).`);
    if (bootRetryTimer) clearTimeout(bootRetryTimer);
    bootRetryTimer = setTimeout(() => { bootRetryTimer = null; bootInProgress = false; boot(); }, TOKEN_RETRY_MS);
    return;
  } finally {
    if (!bootRetryTimer) bootInProgress = false;
  }
}
server.listen(PORT,HOST,()=>{console.log(`NIFTY Flow Evidence Backend listening on ${HOST}:${PORT}`);boot();});
