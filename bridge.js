// Bridge: gives the page the same small API it had inside Claude (db, user, assets, room),
// backed by Supabase. A device enters the couple's code once, then picks who it is.
import { createClient } from './vendor/supabase.js';

const URL_ = 'https://rfetjwjjfxfdcrjpnjiw.supabase.co';
const ANON = 'sb_publishable_9GWwzZv3VQmPDMc_GJvlfw_2-3_J9vZ';
const LS = { key: 'bhb.key', me: 'bhb.me' };
const ls = {
  get: k => { try { return localStorage.getItem(k); } catch { return null; } },
  set: (k, v) => { try { localStorage.setItem(k, v); } catch {} },
  del: k => { try { localStorage.removeItem(k); } catch {} },
};
window.__mediaUrl = id => `${URL_}/storage/v1/object/public/media/${encodeURIComponent(id)}`;

let sb = null, me = null, key = null, channel = null;
let readyResolve; const ready = new Promise(r => (readyResolve = r));

const client = k => createClient(URL_, ANON, {
  global: { headers: { 'x-couple-key': k } },
  auth: { persistSession: false, autoRefreshToken: false },
  realtime: { params: { eventsPerSecond: 20 } },
});
async function sha(s) {
  const b = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(s));
  return [...new Uint8Array(b)].map(x => x.toString(16).padStart(2, '0')).join('');
}

/* ---------------------------------------------------------------- */
/*  Document store with live updates                                */
/* ---------------------------------------------------------------- */
const listeners = new Map(); // coll -> Set(fn)
const cache = new Map();     // coll -> [{id,data}]
const timers = new Map();
async function fetchColl(coll) {
  const { data, error } = await sb.from('docs').select('id,data').eq('coll', coll).limit(3000);
  if (error) throw error;
  cache.set(coll, data || []);
  return data || [];
}
function refresh(coll) {
  clearTimeout(timers.get(coll));
  timers.set(coll, setTimeout(async () => {
    try { await fetchColl(coll); (listeners.get(coll) || new Set()).forEach(fn => fn()); } catch (e) { console.warn(e); }
  }, 120));
}
function changed(coll) { refresh(coll); if (channel) channel.send({ type: 'broadcast', event: 'chg', payload: { coll } }); }
function listen(coll, fn) {
  if (!listeners.has(coll)) listeners.set(coll, new Set());
  listeners.get(coll).add(fn);
  if (cache.has(coll)) setTimeout(fn); else refresh(coll);
  return () => listeners.get(coll).delete(fn);
}
setInterval(() => { for (const c of listeners.keys()) refresh(c); }, 45000);
document.addEventListener('visibilitychange', () => { if (!document.hidden) for (const c of listeners.keys()) refresh(c); });

const split = path => { const p = String(path).split('/'); if (p.length !== 2) throw new TypeError('path'); return p; };
const snapDoc = (id, data) => ({ id, exists: !!data, data: () => data || undefined, metadata: { fromCache: false, hasPendingWrites: false } });
const err = e => ({ code: (e && /row-level|permission|42501/i.test(e.message || e.code || '')) ? 'invalid_argument' : 'unavailable', message: (e && e.message) || 'error' });
const merge = (a, b) => { const o = { ...(a || {}) }; for (const k in b) { const v = b[k]; o[k] = v && typeof v === 'object' && !Array.isArray(v) && o[k] && typeof o[k] === 'object' && !Array.isArray(o[k]) ? merge(o[k], v) : v; } return o; };
const newId = () => crypto.randomUUID().replace(/-/g, '').slice(0, 20);

function docRef(path) {
  const [coll, id] = split(path);
  return {
    id, path,
    async get() { const rows = await fetchColl(coll); const r = rows.find(x => x.id === id); return snapDoc(id, r && r.data); },
    async set(data) { const { error } = await sb.from('docs').upsert({ coll, id, data, updated_at: new Date().toISOString() }); if (error) throw err(error); changed(coll); },
    async update(data) {
      const { data: rows, error: e1 } = await sb.from('docs').select('data').eq('coll', coll).eq('id', id).limit(1);
      if (e1) throw err(e1); if (!rows || !rows.length) throw { code: 'invalid_argument', message: 'missing' };
      const { error } = await sb.from('docs').update({ data: merge(rows[0].data, data), updated_at: new Date().toISOString() }).eq('coll', coll).eq('id', id);
      if (error) throw err(error); changed(coll);
    },
    async delete() { const { error } = await sb.from('docs').delete().eq('coll', coll).eq('id', id); if (error) throw err(error); changed(coll); },
    onSnapshot(next, onErr) { return listen(coll, () => { const r = (cache.get(coll) || []).find(x => x.id === id); next(snapDoc(id, r && r.data)); }); },
  };
}
function query(coll, o = {}) {
  const run = () => {
    let rows = (cache.get(coll) || []).slice();
    if (o.f) rows.sort((a, b) => { const x = a.data[o.f], y = b.data[o.f]; const c = x === y ? 0 : x === undefined ? 1 : y === undefined ? -1 : x < y ? -1 : 1; return o.d === 'desc' ? -c : c; });
    if (o.n) rows = rows.slice(0, o.n);
    const docs = rows.map(r => snapDoc(r.id, r.data));
    return { docs, size: docs.length, empty: !docs.length, docChanges: () => [], metadata: { fromCache: false, hasPendingWrites: false } };
  };
  return {
    path: coll,
    where() { return query(coll, o); },
    orderBy(f, d) { return query(coll, { ...o, f, d }); },
    limit(n) { return query(coll, { ...o, n }); },
    async get() { await fetchColl(coll); return run(); },
    onSnapshot(next) { return listen(coll, () => next(run())); },
    doc(id) { return docRef(coll + '/' + (id || newId())); },
    async add(data) { const r = docRef(coll + '/' + newId()); await r.set(data); return r; },
  };
}
const db = { doc: docRef, collection: c => query(String(c)) };

/* ---------------------------------------------------------------- */
/*  Photos and videos                                                */
/* ---------------------------------------------------------------- */
const extOf = blob => { const t = (blob.type || '').toLowerCase(); const m = { 'image/jpeg': 'jpg', 'image/png': 'png', 'image/webp': 'webp', 'image/gif': 'gif', 'image/heic': 'heic', 'video/mp4': 'mp4', 'video/quicktime': 'mov', 'video/webm': 'webm', 'video/x-m4v': 'm4v', 'video/3gpp': '3gp' }; return m[t] || (t.startsWith('video') ? 'mp4' : 'jpg'); };
async function media(body) {
  const r = await fetch(`${URL_}/functions/v1/media`, { method: 'POST', headers: { 'Content-Type': 'application/json', 'x-couple-key': key, apikey: ANON }, body: JSON.stringify(body) });
  const j = await r.json().catch(() => ({})); if (!r.ok) throw { code: 'unavailable', message: j.error || 'upload' }; return j;
}
const assets = {
  async upload(blob) {
    if (blob.size > 25 * 1024 * 1024) throw { code: 'quota_exceeded', message: 'too big' };
    const { path, token } = await media({ action: 'sign', ext: extOf(blob) });
    const { error } = await sb.storage.from('media').uploadToSignedUrl(path, token, blob, { contentType: blob.type || undefined });
    if (error) throw { code: 'unavailable', message: error.message };
    return { id: path, url: window.__mediaUrl(path), sizeBytes: blob.size, contentType: blob.type };
  },
  async delete(id) { await media({ action: 'delete', path: id }); },
  async list() { return { assets: [], usage: {} }; },
};

/* ---------------------------------------------------------------- */
/*  Being here together: presence + moments                          */
/* ---------------------------------------------------------------- */
const tab = Math.random().toString(36).slice(2, 8);
const peerFns = [], topicFns = {};
let myPres = {}, presT = null, lastPeers = [];
function peersNow() {
  if (!channel) return [];
  const st = channel.presenceState(); const out = [];
  for (const k in st) { const m = (st[k] || [])[0] || {}; out.push({ peer: k, by: m.by || null, isMe: m.by === me, sameTab: k === me + ':' + tab, kind: 'viewer', guest: false, presence: m, updatedAt: Date.now() }); }
  return out;
}
const tellPeers = () => { lastPeers = peersNow(); peerFns.forEach(fn => { try { fn({ peers: lastPeers, joined: [], left: [], updated: [] }); } catch {} }); };
const room = {
  onPeers(fn) { peerFns.push(fn); setTimeout(() => fn({ peers: lastPeers, joined: [], left: [], updated: [] })); return () => {}; },
  on(topic, fn) { (topicFns[topic] = topicFns[topic] || []).push(fn); return () => {}; },
  async emit(topic, data) { if (channel) await channel.send({ type: 'broadcast', event: topic, payload: { by: me, data } }); },
  async presence(patch) {
    for (const k in patch) { if (patch[k] === null) delete myPres[k]; else myPres[k] = patch[k]; }
    if (presT) return; presT = setTimeout(() => { presT = null; if (channel) channel.track({ ...myPres, by: me }).catch(() => {}); }, 120);
  },
  peers: () => lastPeers,
  connected: () => !!channel,
};
async function joinRoom(hash) {
  channel = sb.channel('c-' + hash.slice(0, 24), { config: { presence: { key: me + ':' + tab }, broadcast: { self: false } } });
  channel.on('broadcast', { event: 'chg' }, ({ payload }) => { if (payload && payload.coll) refresh(String(payload.coll)); });
  for (const t of ['touch', 'miss', 'count', 'draw']) channel.on('broadcast', { event: t }, ({ payload }) => {
    const p = payload || {}; (topicFns[t] || []).forEach(fn => { try { fn({ topic: t, data: p.data, by: p.by || null, isMe: p.by === me, sameTab: false, kind: 'viewer', guest: false, peer: '' }); } catch {} });
  });
  channel.on('presence', { event: 'sync' }, tellPeers);
  channel.subscribe(s => { if (s === 'SUBSCRIBED') channel.track({ by: me }).catch(() => {}); });
}

const user = { id: async () => me, me: async () => ({ id: me }), can: async () => true, canEdit: () => true, isOwner: () => false, profiles: async () => ({}) };
window.claude = {
  use: async name => { await ready; return { db, user, assets, room }[name] || null; },
};

/* ---------------------------------------------------------------- */
/*  Entry: the couple's code, then "who are you?"                    */
/* ---------------------------------------------------------------- */
const css = `.gate{position:fixed;inset:0;z-index:20;display:grid;place-items:center;padding:20px;background:rgba(3,5,4,.72);backdrop-filter:blur(14px);-webkit-backdrop-filter:blur(14px);color:#f5efe2;font-family:Manrope,system-ui,sans-serif}
.gate .g{width:min(420px,100%);text-align:center}
.gate h2{font:400 clamp(44px,12vw,64px)/1 Antonio,Oswald,"Arial Narrow",sans-serif;margin-bottom:12px}
.gate p{color:rgba(245,239,226,.65);font-size:15px;line-height:1.55;margin-bottom:22px}
.gate input{width:100%;height:52px;border-radius:14px;border:1px solid rgba(245,239,226,.25);background:rgba(245,239,226,.06);color:#f5efe2;font:500 18px Manrope,sans-serif;text-align:center;letter-spacing:.08em;outline:none}
.gate input:focus{border-color:rgba(245,239,226,.6)}
.gate button{height:50px;padding:0 26px;border-radius:99px;background:#f5efe2;color:#151510;font:600 15px Manrope,sans-serif;border:0;cursor:pointer;margin-top:14px;width:100%}
.gate .who{display:flex;flex-direction:column;gap:10px;margin-bottom:12px}
.gate .who button{margin:0;background:rgba(245,239,226,.08);color:#f5efe2;border:1px solid rgba(245,239,226,.25)}
.gate .err{color:#ffb4b4;font-size:14px;min-height:1.3em;margin-top:10px}`;
function gate(html) {
  let g = document.querySelector('.gate');
  if (!g) { const s = document.createElement('style'); s.textContent = css; document.head.append(s); g = document.createElement('div'); g.className = 'gate'; document.body.append(g); }
  g.innerHTML = `<div class="g">${html}</div>`; return g;
}
const closeGate = () => { const g = document.querySelector('.gate'); if (g) g.remove(); };

async function tryKey(k) { const c = client(k); const { data, error } = await c.rpc('check_key'); if (error) throw error; return data === true ? c : null; }

function askCode(msg) {
  const g = gate(`<h2>Бид хоёрын булан</h2><p>Зөвхөн бид хоёрт.<br>Бидний нууц кодыг оруулна уу.</p><form><input name="c" autocomplete="off" autocapitalize="characters" placeholder="Нууц код"><div class="err">${msg || ''}</div><button>Орох</button></form>`);
  const f = g.querySelector('form'); setTimeout(() => f.c.focus(), 50);
  f.onsubmit = async e => {
    e.preventDefault(); const k = f.c.value.trim().toUpperCase(); if (!k) return;
    f.querySelector('.err').textContent = 'Шалгаж байна…';
    try { const c = await tryKey(k); if (!c) { f.querySelector('.err').textContent = 'Код буруу байна'; return; } ls.set(LS.key, k); start(k, c); }
    catch { f.querySelector('.err').textContent = 'Холбогдож чадсангүй. Интернэтээ шалгаад дахин оролдоорой.'; }
  };
}
async function askWho() {
  const { data } = await sb.from('docs').select('id,data').eq('coll', 'people').limit(20);
  const people = (data || []), full = people.length >= 2;
  return new Promise(res => {
    const list = people.map(p => `<button type="button" data-id="${p.id.replace(/[^\w-]/g, '')}">${(p.data && p.data.name ? String(p.data.name) : 'Нэргүй').replace(/[<>&"]/g, '')}</button>`).join('');
    const form = full ? '' : `<form><input name="n" maxlength="40" placeholder="Миний нэр"><button>Үргэлжлүүлэх</button><div class="err"></div></form>`;
    const hint = full ? 'Бид хоёр аль хэдийн энд байна. Өөрийгөө сонго.<br>Нэрээ дараа нь «Тохиргоо» хэсгээс сольж болно.' : people.length ? 'Өөрийгөө сонго, эсвэл шинээр нэрээ бич.' : 'Нэрээ бичээрэй. Хайрт чинь энэ нэрийг харна.';
    const g = gate(`<h2>Чи хэн бэ?</h2><p>${hint}</p><div class="who">${list}</div>${form}`);
    g.querySelectorAll('.who button').forEach(b => b.onclick = () => res(b.dataset.id));
    const f = g.querySelector('form'); if (!f) return;
    let busy = false;
    f.onsubmit = async e => {
      e.preventDefault(); if (busy) return; const n = f.n.value.trim().slice(0, 40); if (!n) return;
      busy = true; f.querySelector('button').disabled = true;
      const id = 'u' + newId();
      const { error } = await sb.from('docs').insert({ coll: 'people', id, data: { name: n, tz: Intl.DateTimeFormat().resolvedOptions().timeZone || 'Asia/Shanghai', at: Date.now() } });
      if (error) { busy = false; f.querySelector('button').disabled = false; f.querySelector('.err').textContent = /two people|P0001/.test(error.message + error.code) ? 'Энд зөвхөн хоёр хүн байж болно. Дээрээс өөрийгөө сонго.' : 'Хадгалж чадсангүй, дахин оролдоорой.'; return; }
      res(id);
    };
  });
}
async function start(k, c) {
  key = k; sb = c;
  me = ls.get(LS.me);
  if (me) { const { data } = await sb.from('docs').select('id').eq('coll', 'people').eq('id', me).limit(1); if (!data || !data.length) me = null; }
  if (!me) { me = await askWho(); ls.set(LS.me, me); }
  closeGate();
  await joinRoom(await sha(k));
  readyResolve();
  addLogout();
}
function addLogout() {
  const f = document.getElementById('setForm'); if (!f || document.getElementById('logoutBtn')) return;
  const b = document.createElement('button'); b.type = 'button'; b.id = 'logoutBtn'; b.className = 'btn ghost'; b.textContent = 'Энэ төхөөрөмжөөс гарах'; b.style.marginTop = '24px';
  b.onclick = () => { ls.del(LS.key); ls.del(LS.me); location.reload(); };
  f.parentNode.append(b);
}

(async () => {
  const k = ls.get(LS.key);
  if (!k) return askCode();
  try { const c = await tryKey(k); if (c) return start(k, c); ls.del(LS.key); askCode('Код өөрчлөгдсөн байна. Шинэ кодоо оруулна уу.'); }
  catch { gate('<h2>Бид хоёрын булан</h2><p>Холбогдож чадсангүй.<br>Интернэтээ шалгаад дахин нээгээрэй.</p><button onclick="location.reload()">Дахин оролдох</button>'); }
})();
