/* ═══════════════════════════════════════════════════════════════════════
   BICI / ELDORA — AUTHORITATIVE GAME SERVER  (v1)
   Real-time monster + player + combat sync over WebSockets.
   The SERVER owns monster positions, HP and deaths, so every player in a
   room sees exactly the same thing. Clients send their position + "I hit
   monster X for N"; the server resolves it and broadcasts the truth.

   Run:  npm install   then   node server.js
   Port: process.env.PORT || 8787
   ═══════════════════════════════════════════════════════════════════════ */
const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8787;
const SERVER_VERSION = 'v19-towntrees';   // bump on each deploy so clients can confirm what's live
const PROTOCOL=2;   // bump when clients MUST refresh; client compares against its EXPECTED_PROTO
// ── optional Firebase token verification (set FIREBASE_SERVICE_ACCOUNT env to enable) ──
let adminAuth = null, adminDb = null;
const DB_URL = process.env.FIREBASE_DB_URL || 'https://eldora-world-default-rtdb.firebaseio.com';
try {
  if (process.env.FIREBASE_SERVICE_ACCOUNT) {
    const admin = require('firebase-admin');
    admin.initializeApp({ credential: admin.credential.cert(JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT)), databaseURL: DB_URL });
    adminAuth = admin.auth();
    adminDb = admin.database();          // the server now owns all leaderboard writes
    console.log('[auth] Firebase ENABLED — token verification + authoritative score writes.');
  } else {
    console.log('\n========================================================');
    console.log('  WARNING: FIREBASE_SERVICE_ACCOUNT is NOT set.');
    console.log('  -> Tokens are NOT verified and SCORES WILL NOT SAVE.');
    console.log('  -> Set FIREBASE_SERVICE_ACCOUNT (the service-account JSON)');
    console.log('     in the host environment to enable the authoritative');
    console.log('     leaderboard and identity checks.');
    console.log('========================================================\n');
  }
} catch (e) { console.error('[auth] admin init failed (running open):', e.message); }

// ── authoritative leaderboard: the ONLY thing allowed to write score paths in Firebase ──
// (clients are read-only on these paths via the database rules)
const clampI = (v,lo,hi)=>{ v=Math.round(Number(v)||0); return v<lo?lo : v>hi?hi : v; };
const clampF = (v,lo,hi)=>{ v=Number(v)||0; return v<lo?lo : v>hi?hi : v; };
const cleanName = s => String(s||'?').replace(/[<>"'\\\r\n]/g,'').trim().slice(0,16) || '?';
const cleanUid  = s => String(s||'').replace(/[^a-zA-Z0-9_-]/g,'').slice(0,40);
// each game maps a validated metric payload → the exact board rows the server will write.
// scores are RECOMPUTED here from clamped metrics so the client can't just post a giant number.
const SCORE_GAMES = {
  dragonspire: (b)=>{ const lvl=clampI(b.lvl,1,60), m=clampI(b.m,0,99999), done=b.done?1:0, seed=clampI(b.seed,0,10),
      t=Math.round((Number(b.t)||0)*10)/10, points=clampI(b.points,0,9999999), coins=clampI(b.coins,0,9999999), deaths=clampI(b.deaths,0,99999);
      // Town Hall rank: LEVEL reached/finished dominates, then TIME (lower=better), then DEATHS (each ~2s). Distance is NOT used here.
      const Lp=Math.min(lvl,60)+done, timeScore=Math.max(0,1000000-Math.round((t+deaths*2)*10));
      const score=Lp*10000000+timeScore;
      return [
        // in-game Dragonspire board keeps distance (m) for the climber HoF:
        { path:'scores/dragonspire-climb', k:(seed>0?'_m'+seed:''), val:{lvl,m,t:Math.max(0,Math.min(9e6,t)),points,coins,deaths,done,seed}, better:(n,o)=>!o||n.lvl>(o.lvl||0)||(n.lvl===(o.lvl||0)&&n.m>(o.m||0)) },
        // Town Hall board: level → time → deaths (no distance):
        { path:'scores/dragonspire', k:(seed>0?'_m'+seed:''), val:{score,lvl,t:Math.max(0,Math.min(9e6,t)),deaths,done,seed}, better:(n,o)=>!o||n.score>(o.score||0) },
      ]; },
  // HARDCORE — own board, also stores hearts left at the finish (0 = died)
  'dragonspire-hc': (b)=>{ const lvl=clampI(b.lvl,1,60), m=clampI(b.m,0,99999), done=b.done?1:0, seed=clampI(b.seed,0,10),
      t=Math.round((Number(b.t)||0)*10)/10, points=clampI(b.points,0,9999999), coins=clampI(b.coins,0,9999999), deaths=clampI(b.deaths,0,99999), lives=clampI(b.lives,0,3);
      return [ { path:'scores/dragonspire-hc', k:(seed>0?'_m'+seed:''), val:{lvl,m,t:Math.max(0,Math.min(9e6,t)),points,coins,deaths,done,seed,lives}, better:(n,o)=>!o||n.lvl>(o.lvl||0)||(n.lvl===(o.lvl||0)&&n.m>(o.m||0)) } ]; },
  // EASY (kids) — own board
  'dragonspire-ez': (b)=>{ const lvl=clampI(b.lvl,1,60), m=clampI(b.m,0,99999), done=b.done?1:0, seed=clampI(b.seed,0,10),
      t=Math.round((Number(b.t)||0)*10)/10, points=clampI(b.points,0,9999999), coins=clampI(b.coins,0,9999999), deaths=clampI(b.deaths,0,99999);
      return [ { path:'scores/dragonspire-ez', k:(seed>0?'_m'+seed:''), val:{lvl,m,t:Math.max(0,Math.min(9e6,t)),points,coins,deaths,done,seed}, better:(n,o)=>!o||n.lvl>(o.lvl||0)||(n.lvl===(o.lvl||0)&&n.m>(o.m||0)) } ]; },
  crossing: (b)=>{ const stage=clampI(b.stage,1,10), done=b.done?1:0, t=clampF(b.time,0,99999), deaths=clampI(b.deaths,0,99999), seed=clampI(b.seed,0,10);
      const score=(stage+done)*1e7 + Math.max(0, 1e6 - Math.round((t + deaths*2)*10));
      return [ { path:'scores/crossing', k:(seed>0?'_m'+seed:''), val:{score,stage,time:Math.round(t*10)/10,deaths,done,seed}, better:(n,o)=>!o||n.score>(o.score||0) } ]; },
  'crossing-hc': (b)=>{ const stage=clampI(b.stage,1,10), done=b.done?1:0, t=clampF(b.time,0,99999), deaths=clampI(b.deaths,0,99999), seed=clampI(b.seed,0,10);
      const score=(stage+done)*1e7 + Math.max(0, 1e6 - Math.round((t + deaths*2)*10));
      return [ { path:'scores/crossing-hc', k:(seed>0?'_m'+seed:''), val:{score,stage,time:Math.round(t*10)/10,deaths,done,seed}, better:(n,o)=>!o||n.score>(o.score||0) } ]; },
  'crossing-ez': (b)=>{ const stage=clampI(b.stage,1,10), done=b.done?1:0, t=clampF(b.time,0,99999), deaths=clampI(b.deaths,0,99999), seed=clampI(b.seed,0,10);
      const score=(stage+done)*1e7 + Math.max(0, 1e6 - Math.round((t + deaths*2)*10));
      return [ { path:'scores/crossing-ez', k:(seed>0?'_m'+seed:''), val:{score,stage,time:Math.round(t*10)/10,deaths,done,seed}, better:(n,o)=>!o||n.score>(o.score||0) } ]; },
  bears: (b)=>{
      const won   = b.won ? 1 : 0;
      const wave  = clampI(b.wave,  0, 30);
      const t     = clampI(b.t,     0, 86400);   // survival seconds
      const kills = clampI(b.kills, 0, 9999999);
      const bossK = clampI(b.bossK, 0, 99999);
      // Town Hall rank: COMPLETED >> WAVES SURVIVED >> TOTAL KILLS
      const score = won*1e10                 // 1) completed (any win beats any loss)
                  + wave*1e6                 // 2) waves survived
                  + Math.min(999999, kills); // 3) total kills (tiebreaker)
      return [ { path:'scores/bears', val:{ score, wave, won, kills, bossK, t }, better:(n,o)=>!o || n.score > (o.score||0) } ];
    },
  elycidash: (b)=>{ const rounds=clampI(b.rounds,0,10), done=b.done?1:0, t=clampF(b.t,0,99999),
      berries=clampI(b.berries,0,99999), combo=clampI(b.combo,0,9999);
      // rounds reached/finished dominate (top band), then faster time, then berries & combo as small bonuses
      const score=(Math.min(rounds,10)+done)*1e7 + Math.max(0, 1e6 - Math.round(t*10)) + berries*10 + combo*30;
      return [ { path:'scores/elycidash', val:{score,rounds,t:Math.round(t*10)/10,berries,combo,done}, better:(n,o)=>!o||n.score>(o.score||0) } ];
    },
};
async function submitScore(body){
  const spec = SCORE_GAMES[String(body.game||'')];
  if (!spec) return { ok:false, error:'unknown game' };
  if (!adminDb) return { ok:false, error:'scores unavailable (server has no Firebase credential)' };
  // identity: when auth is enabled it is REQUIRED and verified — no impersonation, no client-uid fallback
  let uid;
  if (adminAuth){
    if (!body.token) return { ok:false, error:'auth required' };
    try { uid = (await adminAuth.verifyIdToken(String(body.token))).uid; }
    catch(e){ return { ok:false, error:'bad token' }; }
  } else {
    uid = cleanUid(body.uid);   // only reachable in dev mode with no Firebase credential (and adminDb is null, so scores aren't saved anyway)
  }
  if (!uid) return { ok:false, error:'bad uid' };
  const name = cleanName(body.name);
  const rows = spec(body);
  let wrote = 0;
  for (const row of rows){
    const ref = adminDb.ref('bici/'+row.path+'/'+(uid+(row.k||'')));
    const snap = await ref.once('value'); const cur = snap.val();
    const next = Object.assign({ n:name, uid, d:Date.now() }, row.val);
    if (row.better(next, cur)){ await ref.set(next); wrote++; }
  }
  return { ok:true, wrote };
}
const TICK_HZ = 20;                 // server simulation rate
const NET_HZ = 15;                  // state broadcast rate
const PLAYER_TIMEOUT = 600000;      // keep players while their socket is open (ws close handles real disconnects) — no vanishing
// ── anti-cheat / abuse limits (tunable) ──
const DMG_CAP   = 150;   // max damage accepted per hit (stops 9999 one-shots)
const HIT_COOL  = 130;   // ms between accepted hits from one player (stops machine-gun hits)
const HIT_RANGE = 220;   // player must be within this many px of the monster to damage it
const MSG_PER_S = 60;    // max messages/sec per connection before we ignore the flood
const MAX_MSG_BYTES = 4000;
const MAX_PLAYERS_PER_ROOM = 60;
const DUEL_HP = 100;       // fixed, fair HP for friendly duels (server-owned)
const DUEL_LEN = 60000;    // 60s → draw
// server-owned per-level damage ceiling: a player can never hit harder than their level allows
function maxHitFor(lvl){ return 14 + Math.max(0, Math.min(60, (+lvl||1))) * 5; } // L1≈19, L20≈114, hard cap ~314

// ── monster stat table (by kind) — tune freely ──
const MOB = {
  gnome:  { hp: 26, spd: 70,  dmg: 2,  aggro: 240, atk: 44, cd: 1.3 },
  imp:    { hp: 30, spd: 95,  dmg: 3,  aggro: 280, atk: 46, cd: 1.1 },
  kobold: { hp: 34, spd: 80,  dmg: 4,  aggro: 250, atk: 48, cd: 1.2 },
  boggart:{ hp: 40, spd: 64,  dmg: 4,  aggro: 230, atk: 50, cd: 1.4 },
  redcap: { hp: 44, spd: 90,  dmg: 4,  aggro: 280, atk: 48, cd: 1.2 },
  gremlin:{ hp: 30, spd: 110, dmg: 3,  aggro: 300, atk: 44, cd: 1.0 },
  sprite: { hp: 22, spd: 120, dmg: 2,  aggro: 300, atk: 42, cd: 0.9 },
  wisp:   { hp: 24, spd: 105, dmg: 2,  aggro: 300, atk: 42, cd: 1.0 },
  slime:  { hp: 36, spd: 50,  dmg: 2,  aggro: 200, atk: 46, cd: 1.4 },
  wolf:   { hp: 38, spd: 130, dmg: 4,  aggro: 320, atk: 46, cd: 1.0 },
};
// which kinds (and how many) spawn in each room type
const byUid = new Map();
const parties = new Map();
const playerParty = new Map();
// ══════════════ GAME INSTANCES — matchmaking for the add-on games ══════════════
// Each add-on game (crossing / elycidash / dragonspire) is split into instances of at most
// INST_CAP human players. When an instance fills, the next player opens a new one. A party
// ALWAYS lands in one instance together: when the first member joins we reserve slots for the
// rest, and a party of N never squeezes into an instance with fewer than N free slots.
const MM_GAMES = new Set(['crossing','crossing-hc','crossing-ez','elycidash','dragonspire','dragonspire-hc','dragonspire-ez']);   // dragonspire = the climber game
const INST_CAP = 10;                                           // max HUMAN players per instance
const RESERVE_MS = 90000;                                      // hold a party-mate's slot this long
const AFK_MS = +process.env.AFK_MS || 1800000;                                         // 10 min with no movement/action → kicked (frees bandwidth)
const gameInst = new Map();                                    // game -> [ {key, game, members:Set, reserved:Map<uid,exp>} ]
function instList(game){ if(!gameInst.has(game)) gameInst.set(game,[]); return gameInst.get(game); }
function instOcc(I){ let n=I.members.size; const now=Date.now(); for(const e of I.reserved.values()) if(e>now) n++; return n; }
function newInstKey(game){ const used=new Set(instList(game).map(i=>i.key)); let n=1; while(used.has(game+'#'+n)) n++; return game+'#'+n; }
function partyUids(uid){ const pid=playerParty.get(uid); if(!pid) return [uid]; const pt=parties.get(pid); return pt? [...pt.members] : [uid]; }
function inAnyInstance(uid){ for(const list of gameInst.values()) for(const I of list) if(I.members.has(uid)) return true; return false; }
function curSeed(){ return 1 + (Math.floor(Date.now()/1800000) % 10); }   // same rotation the add-on clients use
function mmJoin(game, uid){
  if(!MM_GAMES.has(String(game).split('@')[0])) return null;
  const list=instList(game);
  let I=list.find(x=>x.members.has(uid)); if(I){ I.reserved.delete(uid); return I; }   // already placed
  const mates=partyUids(uid).filter(u=>u!==uid && byUid.has(u));
  I=list.find(x=>x.members.has(uid)||x.reserved.has(uid)||mates.some(u=>x.members.has(u)||x.reserved.has(u)));  // a party-mate is already here → stay together
  if(!I){ const need=1+mates.length; I=list.find(x=>INST_CAP-instOcc(x) >= need) || null; }  // else an instance with room for the WHOLE party
  if(!I){ I={ key:newInstKey(game), game, members:new Set(), reserved:new Map() }; list.push(I); }  // else a fresh instance
  I.members.add(uid); I.reserved.delete(uid);
  const now=Date.now();
  for(const u of mates){ if(!I.members.has(u)) I.reserved.set(u, now+RESERVE_MS); }   // reserve the rest of the party
  return I;
}
function mmLeave(game, uid){
  const base=String(game).split('@')[0];
  for(const g of [...gameInst.keys()]){ if(g!==base && g.split('@')[0]!==base) continue;
    const list=gameInst.get(g); if(!list) continue;
    for(const I of list){ I.members.delete(uid); I.reserved.delete(uid); }
    gameInst.set(g, list.filter(I=>I.members.size>0 || instOcc(I)>0));
  }
}
function mmLeaveAll(uid){ for(const game of [...gameInst.keys()]) mmLeave(game, uid); }
function mmSweep(){   // expire stale reservations, drop empty instances, AFK-kick idle overworld sockets
  const now=Date.now();
  for(const [game,list] of gameInst){
    for(const I of list){ for(const [u,exp] of [...I.reserved]) if(exp<=now || !byUid.has(u)) I.reserved.delete(u); }
    gameInst.set(game, list.filter(I=>I.members.size>0 || I.reserved.size>0));
  }
  wss.clients.forEach(ws=>{ if(ws.readyState!==1 || !ws.uid) return;
    if(inAnyInstance(ws.uid)) return;                                  // actively in a game instance → never AFK
    if(now-(ws.lastAct||now) > AFK_MS){ try{send(ws,{t:'afk'});}catch(e){} try{ws.close();}catch(e){} }   // idle too long → free the bandwidth
  });
}
const ROOM_SPAWN = {
  'wild':       { kinds: ['gnome','imp','kobold','boggart'], n: 6 },
  // 'glades' (the EAST gate) removed — like westgate it runs client-side Warden waves, so the server
  // no longer spawns its own monsters there (those were the invisible-to-the-player attackers).
  // 'westgate' removed: the gate is driven by the client-side Warden waves, so the server no longer
  // spawns its own (invisible-to-the-player) monsters there.
  'siege':      { kinds: ['redcap','boggart','gremlin'], n: 8 },
  'skyland:1':  { kinds: ['gnome','boggart','sprite','wisp'], n: 6 },
  'skyland:2':  { kinds: ['imp','redcap','gremlin','kobold'], n: 6 },
};
function spawnCfg(roomKey){
  roomKey = String(roomKey||'').split('#')[0];
  if (ROOM_SPAWN[roomKey]) return ROOM_SPAWN[roomKey];
  if (roomKey && roomKey.indexOf('dungeon') === 0) return { kinds: ['kobold','redcap','boggart','wolf'], n: 10 };
  return null; // towns / interiors: no monsters
}

const TOWN = { cut:{}, ores:[ {id:'tore0', up:0}, {id:'tore1', up:0} ] };   // shared: chopped town trees (id->cutMs) + 2 iron ores (up=respawn ms)
const TOWN_REGROW = 900000;   // town trees regrow after 15 min, for everyone at once
const ORE_REGROW  = 120000;   // a shared iron ore respawns 2 min after it's mined
const rooms = new Map(); // roomKey -> { players:Map, mons:[], geo:{w,h,tile}, seq }
function getRoom(key){
  if (!rooms.has(key)) rooms.set(key, { players: new Map(), mons: [], geo: null, seq: 1 });
  return rooms.get(key);
}
function rnd(a,b){ return a + Math.random()*(b-a); }

function ensureMonsters(room, key){
  const cfg = spawnCfg(key);
  if (!cfg || !room.geo) { room.mons = []; return; }
  if (room.mons.length) return; // already spawned
  const { w, h, tile } = room.geo;
  for (let i=0;i<cfg.n;i++){
    const kind = cfg.kinds[(Math.random()*cfg.kinds.length)|0];
    const st = MOB[kind] || MOB.gnome;
    room.mons.push({
      id: room.seq++, k: kind,
      x: rnd(3, w-3)*tile, y: rnd(3, h-3)*tile,
      hp: st.hp, mh: st.hp, dead: false, respawn: 0,
      acd: 0, tgt: null,
      hx: rnd(3, w-3)*tile, hy: rnd(3, h-3)*tile // wander home
    });
  }
}

const wss = new WebSocketServer({ noServer: true });
const server = http.createServer((req,res)=>{
  const cors = { 'access-control-allow-origin':'*', 'access-control-allow-methods':'POST, OPTIONS', 'access-control-allow-headers':'content-type' };
  if (req.method==='OPTIONS'){ res.writeHead(204, cors); res.end(); return; }
  if (req.method==='POST' && req.url==='/submit-score'){
    let data=''; let tooBig=false;
    req.on('data',c=>{ data+=c; if (data.length>4096){ tooBig=true; req.destroy(); } });
    req.on('end', async ()=>{
      res.writeHead(200, Object.assign({'content-type':'application/json'}, cors));
      if (tooBig){ res.end(JSON.stringify({ok:false,error:'payload too large'})); return; }
      let body; try { body=JSON.parse(data||'{}'); } catch(e){ res.end(JSON.stringify({ok:false,error:'bad json'})); return; }
      try { const r=await submitScore(body); res.end(JSON.stringify(r)); }
      catch(e){ console.error('[score] write failed:', e.message); res.end(JSON.stringify({ok:false,error:'server error'})); }
    });
    return;
  }
  // a tiny health page so you can confirm the server is up in a browser
  res.writeHead(200, Object.assign({'content-type':'text/plain'}, cors));
  let n=0; rooms.forEach(r=>n+=r.players.size);
  res.end('BICI authoritative server '+SERVER_VERSION+' OK — '+rooms.size+' rooms, '+n+' players online · scores '+(adminDb?'ON':'OFF'));
});
server.on('upgrade', (req, socket, head)=>{
  wss.handleUpgrade(req, socket, head, ws=>wss.emit('connection', ws, req));
});

function send(ws, obj){ if (ws.readyState===1) ws.send(JSON.stringify(obj)); }

wss.on('connection', (ws)=>{
  ws.alive = true; ws.uid = null; ws.room = null;
  ws.on('message', async (buf)=>{
    if (buf.length > MAX_MSG_BYTES) return;                       // ignore oversized payloads
    const _t = Date.now();
    if (_t - (ws._win||0) > 1000){ ws._win = _t; ws._cnt = 0; }
    if (++ws._cnt > MSG_PER_S) return;                            // flood: silently drop
    let m; try { m = JSON.parse(buf); } catch(e){ return; }
    // AFK timer: movement & actions keep you alive; idle position heartbeats do not
    if (m.t==='pos'){ if (m.mv || m.act) ws.lastAct=_t; } else if (m.t) ws.lastAct=_t;
    if (m.t === 'join'){
      // verify identity when token-checking is enabled
      let verifiedUid = null;
      if (adminAuth){
        try { const dec = await adminAuth.verifyIdToken(String(m.token||'')); verifiedUid = dec.uid; }
        catch(e){ send(ws, { t:'authfail' }); return; }   // bad/absent token → refuse to join
      }
      // leave previous room
      if (ws.room){ const pr = rooms.get(ws.room); if (pr) pr.players.delete(ws.uid); }
      // when auth is on, the uid is the VERIFIED one (clients can't impersonate); otherwise legacy behaviour
      ws.uid = verifiedUid || (String(m.uid||'').slice(0,40) || ('u'+Math.random().toString(16).slice(2,10)));
      ws.room = String(m.room||'town').slice(0,48);
      ws.name = String(m.name||'?').slice(0,16);
      byUid.set(ws.uid, ws);
      const room = getRoom(ws.room);
      if (m.w && m.h && m.tile) room.geo = { w: Math.max(8,Math.min(200,+m.w)), h: Math.max(8,Math.min(200,+m.h)), tile: Math.max(8,Math.min(128,+m.tile)) };
      if (room.players.size >= MAX_PLAYERS_PER_ROOM && !room.players.has(ws.uid)) { send(ws,{t:'full'}); return; }
      ensureMonsters(room, ws.room);
      room.players.set(ws.uid, {
        ws, uid: ws.uid, n: String(m.name||'?').slice(0,16),
        x:+m.x||0, y:+m.y||0, dir:'down', mv:0, fl:0, act:'',
        hp:+m.hp||30, mh:+m.maxhp||30, av:m.av||{}, pet:m.pet||'',
        lvl: Math.max(1, Math.min(60, +m.lvl||1)), maxhit: maxHitFor(m.lvl),
        duel: null,
        sx:+m.x||0, sy:+m.y||0, last: Date.now(), dead:false
      });
      send(ws, { t:'joined', room: ws.room, ver: SERVER_VERSION, proto: PROTOCOL });
      if (ws.room === 'town'){ const _now=Date.now();
        send(ws, { t:'townstate', cut:Object.keys(TOWN.cut), ores:TOWN.ores.map(o=>({id:o.id, rem:Math.max(0,(o.up-_now))/1000})) }); }
    }
    else if (m.t === 'townchop' && ws.room === 'town'){
      const id = String(m.id||'').slice(0,28); if(!id) return;
      if (!TOWN.cut[id]){ TOWN.cut[id] = _t; broadcast('town', { t:'townchop', id }); }   // record + tell everyone in town
    }
    else if (m.t === 'townmine' && ws.room === 'town'){
      const id = String(m.id||'').slice(0,12); const ore = TOWN.ores.find(o=>o.id===id); if(!ore) return;
      if (ore.up > _t) return;                                   // already depleted
      ore.up = _t + ORE_REGROW; broadcast('town', { t:'oreup', id, rem: ORE_REGROW/1000 });
    }
    else if (m.t === 'pos' && ws.room){
      const room = rooms.get(ws.room); const p = room?.players.get(ws.uid); if (!p) return;
      let nx=+m.x, ny=+m.y;
      if (isFinite(nx) && room.geo) p.x = Math.max(0, Math.min(room.geo.w*room.geo.tile, nx));
      if (isFinite(ny) && room.geo) p.y = Math.max(0, Math.min(room.geo.h*room.geo.tile, ny));
      p.dir=String(m.dir||p.dir).slice(0,6); p.mv=m.mv?1:0; p.fl=m.fl?1:0;
      if (m.act) p.act = m.act+'|'+Date.now();
      if (m.pet!=null) p.pet=m.pet;
      if (m.maxhp) p.mh=+m.maxhp;
      if (m.lvl){ p.lvl=Math.max(1,Math.min(60,+m.lvl)); p.maxhit=maxHitFor(p.lvl); }
      p.last = Date.now();
    }
    else if (m.t === 'hit' && ws.room){               // player claims to have damaged a monster
      const room = rooms.get(ws.room); if (!room) return;
      const p = room.players.get(ws.uid); if (!p) return;
      const t2 = Date.now();
      if (t2 - (p._lastHit||0) < HIT_COOL) return;     // too fast → reject
      p._lastHit = t2;
      const mon = room.mons.find(o=>o.id===m.mid && !o.dead); if (!mon) return;
      if (Math.hypot(p.x-mon.x, p.y-mon.y) > HIT_RANGE) return;   // not actually near it → reject
      mon.hp -= Math.max(0, Math.min(p.maxhit||DMG_CAP, +m.dmg||0)); // capped to what THIS player's level allows
      if (mon.hp <= 0){
        mon.dead = true; mon.hp = 0; mon.respawn = Date.now() + 9000; // respawn after 9s
        broadcast(ws.room, { t:'kill', mid: mon.id, by: ws.uid });
      }
    }
    else if (m.t === 'duel_req' && ws.room){          // challenge another player
      const room = rooms.get(ws.room); if (!room) return;
      const me = room.players.get(ws.uid), tgt = room.players.get(String(m.to));
      if (!me || !tgt || me.duel || tgt.duel) return;
      tgt._inreq = { from: ws.uid, t: Date.now() };
      send(tgt.ws, { t:'duel_incoming', from: ws.uid, fromN: me.n });
    }
    else if (m.t === 'duel_acc' && ws.room){          // accept a challenge from m.to
      const room = rooms.get(ws.room); if (!room) return;
      const me = room.players.get(ws.uid), opp = room.players.get(String(m.to));
      if (!me || !opp || me.duel || opp.duel) return;
      if (!me._inreq || me._inreq.from !== opp.uid || Date.now()-me._inreq.t > 30000) return; // must have a live challenge
      me._inreq = null;
      me.duel  = { opp: opp.uid, myhp: DUEL_HP, ophp: DUEL_HP, endT: Date.now()+DUEL_LEN };
      opp.duel = { opp: me.uid,  myhp: DUEL_HP, ophp: DUEL_HP, endT: me.duel.endT };
      send(me.ws,  { t:'duel_start', opp: opp.uid, oppN: opp.n, hp: DUEL_HP });
      send(opp.ws, { t:'duel_start', opp: me.uid,  oppN: me.n,  hp: DUEL_HP });
      broadcast(ws.room, { t:'duel_event', k:'start', a: me.n, b: opp.n });   // referee + crowd react for everyone
    }
    else if (m.t === 'duel_dec' && ws.room){          // decline
      const room = rooms.get(ws.room); if (!room) return;
      const opp = room.players.get(String(m.to)); const me = room.players.get(ws.uid);
      if (me) me._inreq = null;
      if (opp) send(opp.ws, { t:'duel_declined', by: ws.uid });
    }
    else if (m.t === 'party_invite'){
      const tw = byUid.get(String(m.to)); if (!tw) return;
      send(tw, { t:'party_inv', from: ws.uid, fromN: ws.name||'A hero' });
    }
    else if (m.t === 'party_accept'){
      const inv = byUid.get(String(m.to)); if (!inv) return;
      let pid = playerParty.get(inv.uid);
      if (!pid){ pid = 'p_'+inv.uid; parties.set(pid, { leader: inv.uid, members: new Set([inv.uid]) }); playerParty.set(inv.uid, pid); }
      leaveParty(ws.uid);
      const pt = parties.get(pid); if (!pt) return;
      if (pt.members.size >= 4){ send(ws,{t:'party_full'}); return; }
      pt.members.add(ws.uid); playerParty.set(ws.uid, pid);
      sendParty(pid);
    }
    else if (m.t === 'party_leave'){
      const pid = playerParty.get(ws.uid);
      leaveParty(ws.uid);
      if (pid) sendParty(pid);
      send(ws, { t:'party', id:null, members:[], leader:null });
    }
    else if (m.t === 'party_chat'){
      const pid = playerParty.get(ws.uid); if(!pid) return; const pt=parties.get(pid); if(!pt) return;
      const text=String(m.text||'').slice(0,200); if(!text) return;
      const msg=JSON.stringify({ t:'party_msg', from:ws.uid, fromN:ws.name||'?', text });
      for(const uid of pt.members){ if(uid===ws.uid) continue; const w=byUid.get(uid); if(w&&w.readyState===1) w.send(msg); }
    }
    else if (m.t === 'dm'){
      const tw=byUid.get(String(m.to)); if(!tw) return;
      const text=String(m.text||'').slice(0,200); if(!text) return;
      send(tw, { t:'dm_msg', from:ws.uid, fromN:ws.name||'?', text });
    }
    else if (m.t === 'duel_hit' && ws.room){          // land a hit in an active duel
      const room = rooms.get(ws.room); if (!room) return;
      const me = room.players.get(ws.uid); if (!me || !me.duel) return;
      const opp = room.players.get(me.duel.opp); if (!opp || !opp.duel || opp.duel.opp !== me.uid) return;
      const t4 = Date.now(); if (t4 - (me._lastDuelHit||0) < HIT_COOL) return; me._lastDuelHit = t4;
      if (Math.hypot(me.x-opp.x, me.y-opp.y) > HIT_RANGE) return;        // must be near your opponent
      const dmg = Math.max(0, Math.min(me.maxhit||DMG_CAP, +m.dmg||0));  // capped to level
      opp.duel.myhp = Math.max(0, opp.duel.myhp - dmg);
      me.duel.ophp  = opp.duel.myhp;
      // broadcast fresh HP to both
      send(me.ws,  { t:'duel_state', myhp: me.duel.myhp,  ophp: me.duel.ophp });
      send(opp.ws, { t:'duel_state', myhp: opp.duel.myhp, ophp: opp.duel.ophp });
      if (opp.duel.myhp <= 0) endDuel(room, me, opp, me.uid);
    }
    else if (m.t === 'pvp' && ws.room){               // (legacy) raw player damage
      const room = rooms.get(ws.room); if (!room) return;
      const p = room.players.get(ws.uid); if (!p) return;
      const t3 = Date.now(); if (t3 - (p._lastPvp||0) < HIT_COOL) return; p._lastPvp = t3;
      const tgt = room.players.get(String(m.to)); if (!tgt) return;
      if (Math.hypot(p.x-tgt.x, p.y-tgt.y) > HIT_RANGE) return;
      tgt.hp = Math.max(0, tgt.hp - Math.max(0, Math.min(DMG_CAP, +m.dmg||0)));
      if (tgt.hp <= 0){ broadcast(ws.room, { t:'pdeath', uid: tgt.uid, by: ws.uid }); tgt.hp = tgt.mh; }
    }
    else if (m.t === 'sethp' && ws.room){            // client healed (fountain/potion/ability) — raise our record to match
      const p = rooms.get(ws.room)?.players.get(ws.uid);
      if (p && !p.dead){ if(m.mh) p.mh = Math.max(1, Math.min(9999, +m.mh)); const v = Math.max(0, Math.min(p.mh, +m.hp||0)); if (v > p.hp) p.hp = v; }
    }
    else if (m.t === 'respawn' && ws.room){            // client told us the player respawned
      const p = rooms.get(ws.room)?.players.get(ws.uid); if (p){ p.hp = p.mh; p.dead=false; if(m.x)p.sx=p.x=+m.x; if(m.y)p.sy=p.y=+m.y; }
    }
    else if (m.t === 'mm'){                            // matchmaking: place me (and my party) into an instance of an add-on game
      const game=String(m.game||'');
      const skey = MM_GAMES.has(game) ? (game+'@s'+curSeed()) : game;   // seed-scope: instance-mates always share the map seed
      const I=mmJoin(skey, ws.uid);
      send(ws, { t:'mm', game, instance: I?I.key:null, cap: INST_CAP });
    }
    else if (m.t === 'mm_leave'){                      // I left the add-on game → free my instance slot
      mmLeave(String(m.game||''), ws.uid);
    }
  });
  ws.on('close', ()=>{ if (ws.room){ const r = rooms.get(ws.room); if (r) r.players.delete(ws.uid); } const pid=playerParty.get(ws.uid); leaveParty(ws.uid); if(pid)sendParty(pid); mmLeaveAll(ws.uid); byUid.delete(ws.uid); });
  ws.on('error', ()=>{});
});

// ── simulation ──
let last = Date.now();
setInterval(()=>{
  const now = Date.now(); const dt = Math.min(0.1, (now-last)/1000); last = now;
  rooms.forEach((room, key)=>{
    // drop silent players
    for (const [uid,p] of room.players){ if (now - p.last > PLAYER_TIMEOUT) room.players.delete(uid); }
    if (room.players.size === 0){ room.mons = []; return; } // sleep empty rooms (monsters re-spawn on next join)
    const players = [...room.players.values()];
    for (const mon of room.mons){
      if (mon.dead){ if (now >= mon.respawn){ const st = MOB[mon.k]||MOB.gnome; mon.dead=false; mon.hp=mon.mh=st.hp; mon.x=mon.hx; mon.y=mon.hy; } continue; }
      const st = MOB[mon.k] || MOB.gnome;
      // nearest player
      let near=null, nd=1e9;
      for (const p of players){ if (p.dead) continue; const d=Math.hypot(p.x-mon.x,p.y-mon.y); if (d<nd){ nd=d; near=p; } }
      mon.acd = Math.max(0, mon.acd - dt);
      if (near && nd < st.aggro){
        if (nd > st.atk){ // chase
          mon.x += (near.x-mon.x)/nd * st.spd * dt;
          mon.y += (near.y-mon.y)/nd * st.spd * dt;
        } else if (mon.acd <= 0){ // attack — authoritative damage to the player
          mon.acd = st.cd;
          near.hp = Math.max(0, near.hp - st.dmg);
          send(near.ws, { t:'youhit', dmg: st.dmg, hp: near.hp, by: mon.k, byId: mon.id, mx: Math.round(mon.x), my: Math.round(mon.y) });
          if (near.hp <= 0){ broadcast(key, { t:'pdeath', uid: near.uid, by: mon.k }); near.hp = near.mh; }
        }
      } else { // wander gently toward home
        const d=Math.hypot(mon.hx-mon.x,mon.hy-mon.y);
        if (d>8){ mon.x += (mon.hx-mon.x)/d * st.spd*0.4 * dt; mon.y += (mon.hy-mon.y)/d * st.spd*0.4 * dt; }
        else { mon.hx = mon.x + rnd(-120,120); mon.hy = mon.y + rnd(-120,120); }
      }
    }
    // duel draw timer
    const ended = new Set();
    for (const p of players){ if (p.duel && Date.now() > p.duel.endT && !ended.has(p.uid)){
      const opp = room.players.get(p.duel.opp); ended.add(p.uid); if (opp) ended.add(opp.uid);
      endDuel(room, p, opp, 'draw'); } }
  });
}, 1000/TICK_HZ);

function nameOf(uid){ const w = byUid.get(uid); return w ? (w.name||uid) : uid; }
function sendParty(pid){
  const pt = parties.get(pid); if (!pt) return;
  const members = [...pt.members].map(uid => ({ uid, n: nameOf(uid) }));
  const msg = JSON.stringify({ t:'party', id: pid, leader: pt.leader, members });
  for (const uid of pt.members){ const w = byUid.get(uid); if (w && w.readyState===1) w.send(msg); }
}
function leaveParty(uid){
  const pid = playerParty.get(uid); if (!pid) return;
  const pt = parties.get(pid);
  playerParty.delete(uid);
  if (pt){
    pt.members.delete(uid);
    if (pt.members.size <= 1){
      for (const m of pt.members){ playerParty.delete(m); const w = byUid.get(m); if (w && w.readyState===1) w.send(JSON.stringify({ t:'party', id:null, members:[], leader:null })); }
      parties.delete(pid);
    } else { if (pt.leader === uid) pt.leader = [...pt.members][0]; sendParty(pid); }
  }
}
function endDuel(room, a, b, winnerUid){
  if (a) a.duel = null; if (b) b.duel = null;
  if (a) send(a.ws, { t:'duel_end', winner: winnerUid });
  if (b) send(b.ws, { t:'duel_end', winner: winnerUid });
  const wn = winnerUid==='draw' ? 'draw' : (a && winnerUid===a.uid ? a.n : (b? b.n : 'a champion'));
  if (room){ const s = JSON.stringify({ t:'duel_event', k:'end', winner: wn, a: a&&a.n, b: b&&b.n });
    for (const p of room.players.values()) if (p.ws.readyState===1) p.ws.send(s); }
}
function broadcast(key, obj){
  const room = rooms.get(key); if (!room) return;
  const s = JSON.stringify(obj);
  for (const p of room.players.values()) if (p.ws.readyState===1) p.ws.send(s);
}

// ── state broadcast ──
setInterval(()=>{
  rooms.forEach((room, key)=>{
    if (room.players.size === 0) return;
    const mons = room.mons.map(o=>({ id:o.id, k:o.k, x:Math.round(o.x), y:Math.round(o.y), hp:o.hp, mh:o.mh, dead:o.dead?1:0 }));
    for (const p of room.players.values()){
      const plrs = [];
      for (const q of room.players.values()){
        if (q.uid === p.uid) continue;
        plrs.push({ uid:q.uid, n:q.n, x:Math.round(q.x), y:Math.round(q.y), dir:q.dir, mv:q.mv, fl:q.fl, act:q.act, hp:q.hp, mh:q.mh, av:q.av, pet:q.pet });
      }
      send(p.ws, { t:'state', mons, plrs, hp:p.hp, mh:p.mh });
    }
  });
}, 1000/NET_HZ);

setInterval(()=>{ const now=Date.now(); const grew=[];
  for (const id in TOWN.cut){ if (now - TOWN.cut[id] > TOWN_REGROW){ delete TOWN.cut[id]; grew.push(id); } }
  if (grew.length) broadcast('town', { t:'townregrow', ids:grew });
  for (const ore of TOWN.ores){ if (ore.up && ore.up <= now){ ore.up = 0; broadcast('town', { t:'oreup', id:ore.id, rem:0 }); } }
}, 5000);   // shared town tree regrow + ore respawn
setInterval(mmSweep, +process.env.MM_SWEEP || 15000);   // expire reservations, drop empty instances, AFK-kick idle overworld sockets

server.listen(PORT, ()=>{
  console.log('BICI authoritative server '+SERVER_VERSION+' listening on :'+PORT);
  if(adminDb) console.log('  \u2713 Scores ON \u2014 Firebase token verification + authoritative writes active.');
  else        console.log('  \u26a0  Scores OFF \u2014 set FIREBASE_SERVICE_ACCOUNT to turn them on (see warning above).');
});
