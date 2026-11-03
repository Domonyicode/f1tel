// F1 24 telemetria relay: UDP (alap port 20777) -> böngésző (SSE). Nincs külső függőség.
const dgram = require('dgram');
const http = require('http');
const fs = require('fs');
const path = require('path');
const os = require('os');

const UDP_PORT = +process.env.UDP_PORT || 20777;
const HTTP_PORT = +process.env.HTTP_PORT || 3000;

const state = { connected: false };
const clients = new Set();
let dirty = false;
let lastPacket = 0;

// ---- Time trial: mini-szektorok, gap, szektorok, körlista, mentés pályánként ----
const P = 120; // mintapont körönként (12 / 24 / 40 mini-szektorra osztható)
const NS = [12, 24, 40];
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const TRACKS = ['Melbourne', 'Paul Ricard', 'Shanghai', 'Sakhir', 'Barcelona', 'Monaco', 'Montreal', 'Silverstone',
  'Hockenheim', 'Hungaroring', 'Spa', 'Monza', 'Singapore', 'Suzuka', 'Abu Dhabi', 'Austin', 'Interlagos', 'Spielberg',
  'Szocsi', 'Mexikóváros', 'Baku', 'Sakhir (rövid)', 'Silverstone (rövid)', 'Austin (rövid)', 'Suzuka (rövid)', 'Hanoi',
  'Zandvoort', 'Imola', 'Portimão', 'Dzsidda', 'Miami', 'Las Vegas', 'Losail'];
const trackName = id => TRACKS[id] || (id >= 0 ? `Pálya #${id}` : '');

const tt = { trackLen: 0, trackId: -1, sid: '', lapNum: 0, seq: 0, cur: null, prev: null, best: null,
  prevMs: 0, bestMs: 0, prevSec: null, bestSec: null, prevValid: true, bestSplit: { 12: null, 24: null, 40: null },
  bestSector: [null, null, null], laps: [], lapsRev: 0, pS1: 0, pS2: 0,
  bad: false, d0: null, t0: null, last: null, dirty: true, gamePbMs: 0, gamePbSec: null, lastRecordedKey: null,
  captureNum: 0, lastSeenLapMs: 0, lastSeenCurMs: null, lastSeenDist: null, lastFrame: null };

function ttNewLap() {
  tt.cur = new Array(P + 1).fill(null);
  tt.cur[0] = 0;
  tt.bad = false; tt.d0 = null; tt.t0 = null; tt.pS1 = 0; tt.pS2 = 0;
}
ttNewLap();

const okArr = a => Array.isArray(a) && a.length === P + 1 && a.every(v => typeof v === 'number');
const trackFile = () => path.join(DATA_DIR, `track-${tt.trackId}-${tt.trackLen}.json`);
const archiveDir = () => path.join(DATA_DIR, `track-${tt.trackId}-${tt.trackLen}-laps`);
const activeFile = () => path.join(archiveDir(), 'active.json');
const TELEMETRY_FIELDS = ['worldX','worldY','worldZ','yaw','speed','throttle','steer','brake','gear','rpm','drs','revPct',
  'brakeTemp','tyreSurf','tyreInner','engineTemp','tyrePress','tc','abs','fuelMix',
  'brakeBias','fuel','fuelCap','fuelLaps','compound','tyreAge','ers','ersMode',
  'ersHarv','ersDep','tyreWear','lastLap','curLap','s1','s2','lapDist','pos',
  'lap','pit','sector','invalid','penalties','trackLen','trackName'];
const archive = { samples: [], firstAt: 0, lastSave: 0, pending: false, serial: 0 };
const atomicJSON = (file, data) => {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const tmp = file + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify(data));
  fs.renameSync(tmp, file);
};
const snapshot = (sid, frame) => {
  const obj = { at: Date.now(), sid, frame };
  for (const k of TELEMETRY_FIELDS) if (state[k] !== undefined) obj[k] = state[k];
  return obj;
};
function saveActive(force = false) {
  if (!archive.pending && !force) return;
  if (tt.trackId < 0 || !tt.trackLen) return;
  try {
    atomicJSON(activeFile(), {
      version: 1, status: 'inProgress', trackId: tt.trackId, trackLen: tt.trackLen,
      sessionId: tt.sid, captureNum: tt.captureNum, startedAt: archive.firstAt,
      savedAt: Date.now(), samples: archive.samples,
      timeCurve: tt.cur, invalid: tt.bad, lastKnownTimeMs: state.curLap || 0
    });
    archive.lastSave = Date.now(); archive.pending = false;
  } catch (e) { console.error('Folyamatban lévő kör mentése sikertelen:', e.message); }
}
function beginArchive() {
  // A szerver korábbi leállításakor félbemaradt kör nem tűnhet el.
  try {
    const file = activeFile();
    if (fs.existsSync(file)) {
      const old = JSON.parse(fs.readFileSync(file, 'utf8'));
      if (Array.isArray(old.samples) && old.samples.length) {
        const name = `interrupted-${new Date().toISOString().replace(/[:.]/g,'-')}-${++archive.serial}.json`;
        atomicJSON(path.join(archiveDir(), name), { ...old, status:'interrupted', recoveredAt:Date.now() });
      }
      fs.unlinkSync(file);
    }
  } catch (e) { console.error('Megszakadt kör megőrzése sikertelen:', e.message); }
  archive.samples = []; archive.firstAt = Date.now(); archive.pending = false;
  saveActive(true);
}
function appendArchive(sid, frame) {
  archive.samples.push(snapshot(sid, frame));
  archive.pending = true;
  // Másodpercenként tartósan lementjük az aktuális kört is (áramszünetkor max. ~1 s veszhet el).
  if (Date.now() - archive.lastSave >= 1000) saveActive();
}
function archiveLap(meta, rawCurve, finalCurve) {
  try {
    fs.mkdirSync(archiveDir(), { recursive: true });
    const stamp = new Date(meta.at).toISOString().replace(/[:.]/g, '-');
    const filename = `lap-${stamp}-${String(++archive.serial).padStart(4, '0')}.json`;
    atomicJSON(path.join(archiveDir(), filename), {
      version: 1, status: 'completed', trackId: tt.trackId, trackLen: tt.trackLen,
      trackName: trackName(tt.trackId), ...meta, startedAt: archive.firstAt,
      capturedSamples: archive.samples.length, samples: archive.samples,
      rawTimeCurve: rawCurve, timeCurve: finalCurve,
      // Rögzített értékeket tárolunk, nem pótoljuk ki a hiányzó UDP csomagokat kitalált adatokkal.
      source: 'F1 24 player telemetry (last known decoded values for each lap-data packet)'
    });
    archive.pending = false;
    try { fs.unlinkSync(activeFile()); } catch (_) {}
    return filename;
  } catch (e) { console.error('Kör archiválása sikertelen:', e.message); return null; }
}
function loadArchivedLaps() {
  try {
    const dir = archiveDir();
    const names = fs.readdirSync(dir).filter(f => /^lap-.*\.json$/.test(f)).sort();
    if (!names.length) return;
    const rows = [];
    let prev = null, best = null;
    for (const name of names) {
      let j;
      try { j = JSON.parse(fs.readFileSync(path.join(dir, name), 'utf8')); } catch (_) { continue; }
      if (j.status !== 'completed' || !Number.isFinite(j.ms) || j.ms <= 0) continue;
      rows.push({ n:j.lapNum, ms:j.ms, s:j.sectors, ok:j.valid !== false,
        cmp:j.compound ?? null, age:j.tyreAge ?? null, sid:j.sessionId, at:j.at, file:name });
      prev = j;
      if (j.valid !== false && (!best || j.ms < best.ms)) best = j;
    }
    if (!rows.length) return;
    tt.laps = rows.slice(-200);
    if (prev) {
      tt.prevMs = prev.ms; tt.prevSec = prev.sectors || null; tt.prevValid = prev.valid !== false;
      tt.prev = okArr(prev.timeCurve) ? prev.timeCurve : null;
    }
    if (best && (!tt.bestMs || best.ms < tt.bestMs || (best.ms === tt.bestMs && !tt.best))) {
      tt.bestMs = best.ms; tt.bestSec = best.sectors || null;
      tt.best = okArr(best.timeCurve) ? best.timeCurve : null;
    }
    console.log(`Körarchívum betöltve: ${rows.length} kör (${trackName(tt.trackId)})`);
  } catch (e) { console.error('Körarchívum betöltése sikertelen:', e.message); }
}


function loadTrack() {
  try {
    const j = JSON.parse(fs.readFileSync(trackFile(), 'utf8'));
    if (j.P !== P) return;
    tt.gamePbMs = j.gamePbMs || 0; tt.gamePbSec = j.gamePbSec || null;
    // A köridő akkor is visszatöltődik, ha a teljes telemetria-görbe nem állt rendelkezésre.
    tt.bestMs = Number.isFinite(j.bestMs) && j.bestMs > 0 ? j.bestMs : 0;
    tt.prevMs = Number.isFinite(j.prevMs) && j.prevMs > 0 ? j.prevMs : 0;
    tt.best = okArr(j.best) ? j.best : null;
    tt.prev = okArr(j.prev) ? j.prev : null;
    tt.bestSec = j.bestSec || null; tt.prevSec = j.prevSec || null;
    tt.prevValid = j.prevValid !== false;
    for (const N of NS) if (j.bestSplit && Array.isArray(j.bestSplit[N]) && j.bestSplit[N].length === N) tt.bestSplit[N] = j.bestSplit[N];
    if (Array.isArray(j.bestSector) && j.bestSector.length === 3) tt.bestSector = j.bestSector;
    if (Array.isArray(j.laps)) tt.laps = j.laps.slice(-200);
    console.log(`Mentett pályaadat betöltve: ${trackName(tt.trackId)} (${tt.laps.length} kör)`);
  } catch (e) { /* még nincs mentett adat */ }
}
function saveTrack() {
  try {
    fs.mkdirSync(DATA_DIR, { recursive: true });
    const f = trackFile(), tmp = f + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify({ P, trackId: tt.trackId, trackLen: tt.trackLen, name: trackName(tt.trackId),
      gamePbMs: tt.gamePbMs, gamePbSec: tt.gamePbSec, best: tt.best, bestMs: tt.bestMs, bestSec: tt.bestSec, prev: tt.prev, prevMs: tt.prevMs, prevSec: tt.prevSec, prevValid: tt.prevValid,
      bestSplit: tt.bestSplit, bestSector: tt.bestSector, laps: tt.laps }));
    fs.renameSync(tmp, f);
  } catch (e) { console.error('Mentés sikertelen:', e.message); }
}
function ttSwitchTrack(len, tid) {
  if (tt.trackLen && archive.samples.length) saveActive(true);
  tt.trackLen = len; tt.trackId = tid;
  archive.samples = []; archive.firstAt = 0; archive.pending = false; archive.lastSave = 0;
  tt.prev = tt.best = tt.last = null; tt.prevMs = tt.bestMs = 0; tt.prevSec = tt.bestSec = null; tt.prevValid = true;
  tt.bestSplit = { 12: null, 24: null, 40: null }; tt.bestSector = [null, null, null]; tt.laps = [];
  tt.lapNum = 0; tt.captureNum = 0; tt.lastRecordedKey = null; tt.gamePbMs = 0; tt.gamePbSec = null;
  tt.lastSeenLapMs = 0; tt.lastSeenCurMs = tt.lastSeenDist = tt.lastFrame = null; ttNewLap(); loadTrack(); loadArchivedLaps();
  tt.dirty = true; tt.lapsRev++;
}

const ttComplete = a => { for (let k = 1; k < P; k++) if (a[k] == null) return false; return true; };
const splits = (a, N) => { const st = P / N; return Array.from({ length: N }, (_, m) => a[(m + 1) * st] - a[m * st]); };

function refAt(ref, d, L) { // referencia kör ideje a d távolságnál (ms)
  const x = d / (L / P);
  if (x <= 0) return 0;
  const k = Math.floor(x);
  if (k >= P) return ref[P];
  return ref[k] + (ref[k + 1] - ref[k]) * (x - k);
}

function completeSamples(raw, ms) {
  const a = raw.slice(); a[0] = 0; a[P] = ms;
  const known = [];
  for (let i = 0; i <= P; i++) if (Number.isFinite(a[i]) && a[i] >= 0 && a[i] <= ms) known.push(i);
  // Nem rajzolunk találomra interpolált deltát, ha csak a kör egy töredéke érkezett meg.
  if (known.length < 8 || known[1] > P * 0.1 || known[known.length - 2] < P * 0.85) return null;
  for (let n = 1; n < known.length; n++) {
    const x = known[n-1], y = known[n];
    if (a[y] < a[x]) return null;
    for (let k = x+1; k < y; k++) a[k] = Math.round(a[x] + (a[y]-a[x])*(k-x)/(y-x));
  }
  return a;
}
function commitLap(sid, lapNum, ms) {
  if (!(ms > 10000 && ms < 600000) || !lapNum || tt.lastRecordedKey === `${sid}:${lapNum}`) return;
  tt.lastRecordedKey = `${sid}:${lapNum}`;
  const rawCurve = tt.cur.slice();
  const done = completeSamples(tt.cur, ms);
  const valid = !tt.bad;
  const s1 = tt.pS1, s2 = tt.pS2;
  const sec = s1 > 0 && s2 > 0 && s1+s2 < ms ? [s1,s2,ms-s1-s2] : null;
  const last = { t: done, ms, valid, sec, refPrev: tt.prev, refBest: tt.best,
    refPrevSec: tt.prevSec, refBestSec: tt.bestSec, purple: null, secPurple: null };
  // Az ELŐZŐ kör a tényleges utolsó befejezett kör, érvénytelen kör esetén is.
  // A LEGJOBB rekord viszont csak érvényes körből származhat.
  tt.prevMs = ms; tt.prevSec = sec; tt.prev = done; tt.prevValid = valid;
  if (valid) {
    if (done) {
      last.purple = {};
      for (const N of NS) {
        const sp = splits(done,N), bs = tt.bestSplit[N];
        last.purple[N] = sp.map((v,i) => bs && bs[i] != null && v < bs[i]);
        tt.bestSplit[N] = sp.map((v,i) => bs && bs[i] != null ? Math.min(bs[i],v) : v);
      }
    }
    if (sec) {
      last.secPurple = sec.map((v,i) => tt.bestSector[i] != null && v < tt.bestSector[i]);
      tt.bestSector = sec.map((v,i) => tt.bestSector[i] == null ? v : Math.min(tt.bestSector[i],v));
    }
    if (!tt.bestMs || ms < tt.bestMs) { tt.bestMs = ms; tt.best = done; tt.bestSec = sec; }
  }
  tt.last = last;
  const archivedFile = archiveLap({ at: Date.now(), sessionId:sid, lapNum, ms, valid,
    sectors:sec, compound:state.compound ?? null, tyreAge:state.tyreAge ?? null }, rawCurve, done);
  tt.laps.push({n:lapNum,ms,s:sec,ok:valid,file:archivedFile,cmp:state.compound ?? null,
    age:state.tyreAge ?? null,sid,at:Date.now()});
  if (tt.laps.length > 200) tt.laps.splice(0,tt.laps.length-200);
  tt.lapsRev++; tt.seq++; tt.dirty = true; saveTrack();
}
function ttUpdate(sid, frame) {
  const L = tt.trackLen;
  if (!L || state.lap == null) return;
  const lap = state.lap, d = state.lapDist, t = state.curLap, step = L / P;
  if (!Number.isFinite(d) || !Number.isFinite(t) || t < 0) return;
  if (sid !== tt.sid) {
    // Új próbálkozás/session: a pálya eddigi előző és legjobb köre megmarad.
    tt.sid = sid; tt.lapNum = lap; tt.captureNum = lap || 1;
    ttNewLap(); beginArchive(); tt.lastSeenCurMs = null; tt.lastSeenDist = null;
    tt.lastSeenLapMs = state.lastLap || 0; tt.lastFrame = null;
    tt.lapsRev++; tt.dirty = true; tt.lastRecordedKey = null;
  }
  const prevT = tt.lastSeenCurMs, prevD = tt.lastSeenDist;
  // Az F1 24 Time Trial gyakran ugyanazt a currentLapNum-ot adja minden próbálkozásnál.
  // Ezért a célvonalon áthaladást idő/distance-visszaugrásból is felismerjük.
  const rewind = tt.lastFrame != null && frame + 10 < tt.lastFrame;
  const distWrap = prevD != null && prevD > L * .65 && d < L * .30;
  const timeReset = prevT != null && prevT > 12000 && t + 5000 < prevT;
  const lapAdvance = lap > tt.lapNum && lap <= tt.lapNum + 1;
  const changedLast = state.lastLap > 10000 && state.lastLap !== tt.lastSeenLapMs;
  const crossed = !rewind && (distWrap || (lapAdvance && (t < 20000 || d < L * .25)) ||
    (timeReset && changedLast && d < L * .25));
  const restart = !rewind && timeReset && !crossed;

  if (crossed) {
    const ms = state.lastLap > 10000 ? state.lastLap : (prevT > 10000 ? prevT : 0);
    // A játék utolsó köridő mezője az elsődleges; megőrizzük a teljes kört annak
    // ellenére, hogy a játék körszáma nem növekszik.
    commitLap(sid, tt.captureNum, ms);
    tt.captureNum++;
    ttNewLap(); beginArchive(); tt.dirty = true;
  } else if (restart || rewind) {
    // Restart / flashback nem új befejezett kör.
    // Az eldobott próbálkozás külön JSON fájlba kerül, nem változtatja az előző/legjobb kört.
    if (archive.samples.length) {
      try {
        const f = `aborted-${new Date().toISOString().replace(/[:.]/g,'-')}-${++archive.serial}.json`;
        atomicJSON(path.join(archiveDir(), f), { version:1, status:'aborted',
          trackId:tt.trackId, trackLen:tt.trackLen, sessionId:sid,
          startedAt:archive.firstAt, endedAt:Date.now(), samples:archive.samples,
          rawTimeCurve:tt.cur, reason:rewind ? 'flashback' : 'restart' });
      } catch (e) { console.error('Restart mentése sikertelen:', e.message); }
    }
    ttNewLap(); beginArchive(); tt.dirty = true;
  }
  tt.lapNum = lap;
  tt.lastSeenCurMs = t; tt.lastSeenDist = d; tt.lastSeenLapMs = state.lastLap || 0;
  tt.lastFrame = frame;

  if (state.invalid || state.pit) tt.bad = true;
  if (d >= 0 && d <= L + 20) {
    if (tt.d0 != null && d < tt.d0 - 1) {
      for (let k = 1; k <= P; k++) if (k * step > d) tt.cur[k] = null;
      tt.d0 = d; tt.t0 = t; tt.dirty = true;
    } else if (tt.d0 == null) {
      tt.d0 = d; tt.t0 = t;
    } else if (d - tt.d0 > L * 0.2) {
      // A nagy UDP-kimaradás önmagában nem teszi érvénytelenné a játékban futott kört.
      tt.d0 = d; tt.t0 = t;
    } else if (d > tt.d0) {
      const from = Math.max(1, Math.floor(tt.d0 / step) + 1);
      const to = Math.min(P - 1, Math.floor(d / step));
      for (let k = from; k <= to; k++) {
        tt.cur[k] = Math.round(tt.t0 + (k * step - tt.d0) / (d - tt.d0) * (t - tt.t0));
        tt.dirty = true;
      }
      tt.d0 = d; tt.t0 = t;
    } else { tt.t0 = t; }
  }
  tt.pS1 = state.s1 || 0; tt.pS2 = state.s2 || 0;
  state.trackLen = L;
  state.trackName = trackName(tt.trackId);
  appendArchive(sid, frame);
  const ok = d >= 0 && d <= L && !rewind;
  state.deltaPrev = tt.prev && ok ? t - refAt(tt.prev, d, L) : null;
  state.deltaBest = tt.best && ok ? t - refAt(tt.best, d, L) : null;
}

function ttPayload() {
  return { P, trackLen: tt.trackLen, name: trackName(tt.trackId), sid: tt.sid, lapNum: tt.lapNum, seq: tt.seq,
           cur: tt.cur, prev: tt.prev, best: tt.best, prevMs: tt.prevMs, bestMs: tt.bestMs,
           gamePbMs: tt.gamePbMs, gamePbSec: tt.gamePbSec, prevSec: tt.prevSec, prevValid: tt.prevValid, bestSec: tt.bestSec, bestSplit: tt.bestSplit, bestSector: tt.bestSector, last: tt.last };
}
function lapsPayload() {
  return { rev: tt.lapsRev, sid: tt.sid, name: trackName(tt.trackId), trackLen: tt.trackLen,
           laps: tt.laps.slice(-60), gamePbMs: tt.gamePbMs, bestMs: tt.bestMs, bestSector: tt.bestSector };
}

// -------- Verseny mérnöki panel / 2024-es UDP (külön /race/state végpont) --------
const RACE_CARS = 22;
const race = {
  sessionId: '', playerIndex: -1, trackId: -1, trackLen: 0, cars: Array.from({length:RACE_CARS},(_,index)=>({index})),
  trackPoints: new Map(), trackDirty: false, trackSavedAt: 0, lastTrackSampleAt: 0,
  pitObservation: null, lastPitStatus: null, pitExitPoint: null,
  session: { type: null, totalLaps: 0, safetyCar: null }, updatedAt: 0, lastEvent: null
};
const RACE_CAL_FILE = path.join(DATA_DIR, 'race-pit-calibration.json');
let pitCal = {};
try { const obj = JSON.parse(fs.readFileSync(RACE_CAL_FILE, 'utf8')); if (obj && typeof obj === 'object') pitCal = obj; } catch (_) {}
function raceSession(sid, playerIndex) {
  if (race.sessionId === sid && race.playerIndex === playerIndex) return;
  saveRaceMap();
  race.sessionId = sid; race.playerIndex = playerIndex;
  race.cars = Array.from({length:RACE_CARS},(_,index)=>({index}));
  race.session = {type:null,totalLaps:0,safetyCar:null};
  race.pitObservation = null; race.lastPitStatus = null;
  race.lastEvent = null; race.updatedAt = 0;
  // A pályarajz és a korábbi boxmérések pályánként megmaradnak.
}
const raceMapFilename = () => path.join(DATA_DIR, `track-${race.trackId}-${race.trackLen}-circuit.json`);
function saveRaceMap() {
  if (!race.trackDirty || race.trackId < 0 || !race.trackLen || race.trackPoints.size < 8) return;
  try {
    atomicJSON(raceMapFilename(), {version:1, trackId:race.trackId, trackLen:race.trackLen,
      savedAt:Date.now(), points:Array.from(race.trackPoints.values()).sort((a,b)=>a[0]-b[0])});
    race.trackDirty=false; race.trackSavedAt=Date.now();
  } catch(e) {console.error('Versenypálya mentése sikertelen:',e.message);}
}
function raceTrack(tid, len) {
  if (tid === race.trackId && len === race.trackLen) return;
  saveRaceMap();
  race.trackId=tid; race.trackLen=len; race.trackPoints=new Map(); race.trackDirty=false;
  race.pitExitPoint=null;
  if (tid < 0 || !len) return;
  const put=(d,x,z)=>{
    if (![d,x,z].every(Number.isFinite) || d<0 || d>len+20 || Math.abs(x)>100000 || Math.abs(z)>100000) return;
    const bin = Math.min(Math.floor(len/12), Math.max(0, Math.round(d/12)));
    if (!race.trackPoints.has(bin)) race.trackPoints.set(bin,[+d.toFixed(1),+x.toFixed(2),+z.toFixed(2)]);
  };
  try {const j=JSON.parse(fs.readFileSync(raceMapFilename(),'utf8')); if(Array.isArray(j.points)) for(const [d,x,z] of j.points) put(d,x,z);} catch (_) {}
  // Az előző Time Trial rögzítéseiből is felépítjük a pálya geometriáját.
  if (race.trackPoints.size < 80) {
    try {
      const dir=path.join(DATA_DIR,`track-${tid}-${len}-laps`);
      const names=fs.readdirSync(dir).filter(n=>/^lap-[A-Za-z0-9_-]+\.json$/.test(n)).sort().reverse();
      for(const name of names.slice(0,4)) {
        const j=JSON.parse(fs.readFileSync(path.join(dir,name),'utf8'));
        if(!Array.isArray(j.samples))continue;
        for(const s of j.samples) put(s.lapDist,s.worldX,s.worldZ);
        if(race.trackPoints.size>=80) break;
      }
    }catch(_){}
  }
  race.trackDirty=race.trackPoints.size>=8;
}
function recordRaceTrackPoint() {
  if (race.trackLen<500 || race.trackId<0) return;
  const c=race.cars[race.playerIndex]; if(!c || !Number.isFinite(c.lapDist) || !Number.isFinite(c.x) || !Number.isFinite(c.z))return;
  if (c.lapDist<0 || c.lapDist>race.trackLen+10 || Math.abs(c.x)>100000 || Math.abs(c.z)>100000) return;
  const bin=Math.min(Math.floor(race.trackLen/12),Math.max(0,Math.round(c.lapDist/12)));
  if(!race.trackPoints.has(bin)) {
    race.trackPoints.set(bin,[+c.lapDist.toFixed(1),+c.x.toFixed(2),+c.z.toFixed(2)]);
    race.trackDirty=true;
  }
  if (race.trackDirty && Date.now()-race.trackSavedAt>12000) saveRaceMap();
}
function raceCalKey(){return `${race.trackId}-${race.trackLen}`;}
function raceCalMode(sc){return sc===1?'sc':sc===2?'vsc':'green';}
function raceCalibratedLoss(mode) {
  const values=pitCal[raceCalKey()]?.[mode];
  if(!Array.isArray(values)||!values.length)return null;
  const sorted=values.filter(v=>Number.isFinite(v)&&v>3&&v<90).slice().sort((a,b)=>a-b);
  return sorted.length ? {seconds:sorted[Math.floor(sorted.length/2)],samples:sorted.length} : null;
}
function racePitObserve(car,sessionTime) {
  if (!car || !Number.isFinite(sessionTime))return;
  const prev=race.lastPitStatus, current=car.pitStatus;
  if (current>0 && prev===0 && Number.isFinite(car.totalDist)) {
    race.pitObservation={startTime:sessionTime,startDist:car.totalDist,sc:race.session.safetyCar,sessionId:race.sessionId};
  }
  if(current===0 && prev>0 && race.pitObservation && race.pitObservation.sessionId===race.sessionId){
    const v=race.pitObservation,dt=sessionTime-v.startTime,dist=car.totalDist-v.startDist;
    const lapSeconds=car.lastLapMs>25000?car.lastLapMs/1000:race.trackLen/51;
    const rate=race.trackLen / Math.max(35,lapSeconds);
    // Tényleges pitstop vs. az adott útszakaszon normál versenytempó mellett eltelt idő.
    const loss=dt-dist/Math.max(20,rate);
    const mode=raceCalMode(v.sc);
    if (dt>5 && dt<130 && dist>=0 && dist<race.trackLen && loss>3 && loss<90) {
      const key=raceCalKey(); if(!pitCal[key])pitCal[key]={};
      if(!Array.isArray(pitCal[key][mode]))pitCal[key][mode]=[];
      pitCal[key][mode].push(+loss.toFixed(2));pitCal[key][mode]=pitCal[key][mode].slice(-12);
      try{atomicJSON(RACE_CAL_FILE,pitCal);}catch(e){console.error('Boxmérés mentési hiba:',e.message);}
      race.pitExitPoint={x:car.x,z:car.z,d:car.lapDist};
    }
    race.pitObservation=null;
  }
  if(prev!==current)race.lastPitStatus=current;
}
function readRacePacket(b,id,p,sid,sessionTime) {
  raceSession(sid,p);
  const now=Date.now();race.updatedAt=now;
  if(id===0){ // minden autó 3D helyzete, 60 byte / autó
    for(let i=0;i<RACE_CARS;i++){
      const o=29+i*60,c=race.cars[i];
      const x=b.readFloatLE(o),z=b.readFloatLE(o+8),vx=b.readFloatLE(o+12),vz=b.readFloatLE(o+20);
      if(Number.isFinite(x)&&Number.isFinite(z)&&Math.abs(x)<100000&&Math.abs(z)<100000 && (Math.abs(x)+Math.abs(z)>0.01)){
        c.x=x;c.z=z;c.vx=vx;c.vz=vz;c.motionAt=now;
      }
    }
    recordRaceTrackPoint();
  }else if(id===1){ // Session - 2024, 753 bytes
    const s=race.session;
    s.weather=b.readUInt8(29);s.trackTemp=b.readInt8(30);s.airTemp=b.readInt8(31);
    s.totalLaps=b.readUInt8(32);s.type=b.readUInt8(35);s.pitSpeed=b.readUInt8(42);
    s.paused=b.readUInt8(43)===1;s.safetyCar=b.readUInt8(153);
    s.rainChance=b.readUInt8(155)>0?b.readUInt8(163):null;
    s.pitIdeal=b.readUInt8(682);s.pitLatest=b.readUInt8(683);
    s.gameRejoin=b.readUInt8(684);s.network=b.readUInt8(154)===1;
    const count=Math.min(21,b.readUInt8(47));s.marshal=[];
    for(let i=0;i<count;i++){
      const o=48+i*5;const d=b.readFloatLE(o),f=b.readInt8(o+4);
      if(Number.isFinite(d)&&d>=0&&d<=1)s.marshal.push({fraction:d,flag:f});
    }
    const tid=b.readInt8(36),len=b.readUInt16LE(33);raceTrack(tid,len);
  }else if(id===2){ // kör/pozíció adatok az összes autóról
    for(let i=0;i<RACE_CARS;i++){
      const o=29+i*57,c=race.cars[i];
      c.lastLapMs=b.readUInt32LE(o);c.curLapMs=b.readUInt32LE(o+4);
      c.deltaFrontMs=b.readUInt16LE(o+14)+60000*b.readUInt8(o+16);
      c.deltaLeaderMs=b.readUInt16LE(o+17)+60000*b.readUInt8(o+19);
      const d=b.readFloatLE(o+20),total=b.readFloatLE(o+24);
      if(Number.isFinite(d)) c.lapDist=d;
      if(Number.isFinite(total))c.totalDist=total;
      const scDelta=b.readFloatLE(o+28);if(Number.isFinite(scDelta))c.scDelta=scDelta;
      c.position=b.readUInt8(o+32);c.lap=b.readUInt8(o+33);
      c.pitStatus=b.readUInt8(o+34);c.pitStops=b.readUInt8(o+35);
      c.sector=b.readUInt8(o+36)+1;c.penalties=b.readUInt8(o+38);
      c.warnings=b.readUInt8(o+39);c.cornerWarnings=b.readUInt8(o+40);
      c.driveThrough=b.readUInt8(o+41);c.stopGo=b.readUInt8(o+42);
      c.driverStatus=b.readUInt8(o+44);c.resultStatus=b.readUInt8(o+45);
      c.pitLaneTimerActive=b.readUInt8(o+46)===1;
      c.pitLaneMs=b.readUInt16LE(o+47);c.pitStopMs=b.readUInt16LE(o+49);
      c.servePenalty=b.readUInt8(o+51)===1;
      c.lapAt=now;
    }
    const mine=race.cars[p];racePitObserve(mine,sessionTime);
    recordRaceTrackPoint();
  }else if(id===4){
    const active=Math.min(RACE_CARS,b.readUInt8(29));
    race.activeCount=active;
    for(let i=0;i<active;i++){
      const o=30+i*60,c=race.cars[i];
      c.ai=b.readUInt8(o)===1;c.driverId=b.readUInt8(o+1);
      c.team=b.readUInt8(o+3);c.carNumber=b.readUInt8(o+5);
      c.name=b.subarray(o+7,o+55).toString('utf8').split('\0')[0].trim().slice(0,48);
      c.participantAt=now;
    }
  }else if(id===7){
    for(let i=0;i<RACE_CARS;i++){
      const o=29+i*55,c=race.cars[i];
      c.limiter=b.readUInt8(o+4)===1;
      c.drsAllowed=b.readUInt8(o+22)===1;
      c.compound=b.readUInt8(o+26);
      c.tyreAge=b.readUInt8(o+27);
      c.fiaFlag=b.readInt8(o+28);
      const e=b.readFloatLE(o+37);if(Number.isFinite(e))c.ersPct=Math.max(0,Math.min(100,e/4000000*100));
      if(i===p){race.playerFuelLaps=b.readFloatLE(o+13);}
      c.statusAt=now;
    }
  }else if(id===10){
    const o=29+p*42;
    race.damage={frontLeft:b.readUInt8(o+24),frontRight:b.readUInt8(o+25),rear:b.readUInt8(o+26),
      floor:b.readUInt8(o+27),diffuser:b.readUInt8(o+28),sidepod:b.readUInt8(o+29),
      drsFault:b.readUInt8(o+30)===1,ersFault:b.readUInt8(o+31)===1,
      gearbox:b.readUInt8(o+32),engine:b.readUInt8(o+33)};
  }else if(id===3){
    const code=b.toString('ascii',29,33);
    race.lastEvent={code,at:now};
  }
}
function racePayload() {
  const now=Date.now(),mine=race.cars[race.playerIndex];
  const cars=race.cars.filter(c=>c.position>0 && c.position<=RACE_CARS && c.lapAt && now-c.lapAt<3500 &&
    c.resultStatus!==4&&c.resultStatus!==5&&c.resultStatus!==7).sort((a,b)=>a.position-b.position)
    .map(c=>({index:c.index,name:c.name||('Autó #'+(c.index+1)),team:c.team,number:c.carNumber,
      position:c.position,lap:c.lap,lapDist:c.lapDist,totalDist:c.totalDist,lastLapMs:c.lastLapMs,
      curLapMs:c.curLapMs,deltaFrontMs:c.deltaFrontMs,deltaLeaderMs:c.deltaLeaderMs,
      x:c.motionAt && now-c.motionAt<3500?c.x:null,z:c.motionAt && now-c.motionAt<3500?c.z:null,
      speedMs:Number.isFinite(c.vx)&&Number.isFinite(c.vz)?Math.hypot(c.vx,c.vz):null,
      tyre:c.compound,tyreAge:c.tyreAge,ersPct:c.ersPct,drsAllowed:c.drsAllowed,
      pitStatus:c.pitStatus,pitStops:c.pitStops,penalties:c.penalties,lapAt:c.lapAt}));
  const bestCal={};for(const key of ['green','vsc','sc'])bestCal[key]=raceCalibratedLoss(key);
  return {connected:now-lastPacket<3000,updatedAt:race.updatedAt,trackName:trackName(race.trackId),
    trackId:race.trackId,trackLen:race.trackLen,sessionId:race.sessionId,playerIndex:race.playerIndex,
    session:race.session,damage:race.damage||null,playerFuelLaps:race.playerFuelLaps,
    cars,track:Array.from(race.trackPoints.values()).sort((a,b)=>a[0]-b[0]),
    mapPoints:race.trackPoints.size,calibratedLoss:bestCal,pitExit:race.pitExitPoint,
    lastEvent:race.lastEvent};
}

const udp = dgram.createSocket('udp4');

udp.on('message', (b) => {
  if (b.length < 29 || b.readUInt16LE(0) !== 2024) return;
  const id = b.readUInt8(6);
  const p = b.readUInt8(27); // játékos autó indexe
  if (p > 21) return;
  // Csomaghossz ellenőrzése a mezők olvasása előtt.
  if (id === 0 && b.length < 29 + (p + 1) * 60) return;
  if (id === 1 && b.length < 753) return;
  if (id === 6 && b.length < 29 + (p + 1) * 60) return;
  if (id === 2 && b.length < 1285) return;
  if (id === 7 && b.length < 29 + (p + 1) * 55) return;
  if (id === 10 && b.length < 953) return;
  if (id === 14 && b.length < 101) return;
  if (id === 4 && b.length < 1350) return;
  if (id === 3 && b.length < 33) return;
  if (![0, 1, 2, 3, 4, 6, 7, 10, 14].includes(id)) return;
  lastPacket = Date.now();
  state.lastPacketAt = lastPacket;
  const sid = b.readBigUInt64LE(7).toString(36);
  readRacePacket(b, id, p, sid, b.readFloatLE(15));

  if (id === 0) { // Motion: 3D koordináták és yaw; új JSON köröknél valódi pályarajz
    const o = 29 + p * 60;
    const x = b.readFloatLE(o), y = b.readFloatLE(o + 4), z = b.readFloatLE(o + 8);
    if ([x,y,z].every(Number.isFinite)) { state.worldX = x; state.worldY = y; state.worldZ = z; }
    const yaw = b.readFloatLE(o + 48);
    if (Number.isFinite(yaw)) state.yaw = yaw;
  } else if (id === 14) { // Time Trial: player, personal best, rival (3 x 24 bytes)
    const o = 29 + 24;
    const pb = b.readUInt32LE(o + 2);
    const valid = b.readUInt8(o + 23) === 1;
    if (valid && pb > 0 && pb < 600000 && (tt.gamePbMs !== pb)) {
      tt.gamePbMs = pb;
      tt.gamePbSec = [6,10,14].map(k => b.readUInt32LE(o+k));
      tt.dirty = true; saveTrack();
    }
  } else if (id === 1) { // Session: pályahossz + pálya azonosító
    const len = b.readUInt16LE(33), tid = b.readInt8(36);
    if (len && (len !== tt.trackLen || tid !== tt.trackId)) ttSwitchTrack(len, tid); // pálya (váltás) -> mentett adat betöltése
    state.trackLen = len;
     state.trackName = trackName(tid);
  } else if (id === 6) { // Car Telemetry (60 byte / autó)
    const o = 29 + p * 60;
    state.speed = b.readUInt16LE(o);
    state.throttle = b.readFloatLE(o + 2);
    state.steer = b.readFloatLE(o + 6);
    state.brake = b.readFloatLE(o + 10);
    state.gear = b.readInt8(o + 15);
    state.rpm = b.readUInt16LE(o + 16);
    state.drs = b.readUInt8(o + 18);
    state.revPct = b.readUInt8(o + 19);
    state.brakeTemp = [0, 1, 2, 3].map(i => b.readUInt16LE(o + 22 + i * 2));
    state.tyreSurf = [0, 1, 2, 3].map(i => b.readUInt8(o + 30 + i));
    state.tyreInner = [0, 1, 2, 3].map(i => b.readUInt8(o + 34 + i));
    state.engineTemp = b.readUInt16LE(o + 38);
    state.tyrePress = [0, 1, 2, 3].map(i => b.readFloatLE(o + 40 + i * 4));
  } else if (id === 2) { // Lap Data (57 byte / autó)
    const o = 29 + p * 57;
    state.lastLap = b.readUInt32LE(o);
    state.curLap = b.readUInt32LE(o + 4);
    state.s1 = b.readUInt16LE(o + 8) + b.readUInt8(o + 10) * 60000;
    state.s2 = b.readUInt16LE(o + 11) + b.readUInt8(o + 13) * 60000;
    state.lapDist = b.readFloatLE(o + 20);
    state.pos = b.readUInt8(o + 32);
    state.lap = b.readUInt8(o + 33);
    state.pit = b.readUInt8(o + 34);
    state.sector = b.readUInt8(o + 36) + 1;
    state.invalid = b.readUInt8(o + 37);
    state.penalties = b.readUInt8(o + 38);
    ttUpdate(b.readBigUInt64LE(7).toString(36), b.readUInt32LE(19));
  } else if (id === 7) { // Car Status (55 byte / autó)
    const o = 29 + p * 55;
    state.tc = b.readUInt8(o);
    state.abs = b.readUInt8(o + 1);
    state.fuelMix = b.readUInt8(o + 2);
    state.brakeBias = b.readUInt8(o + 3);
    state.fuel = b.readFloatLE(o + 5);
    state.fuelCap = b.readFloatLE(o + 9);
    state.fuelLaps = b.readFloatLE(o + 13);
    state.compound = b.readUInt8(o + 26);
    state.tyreAge = b.readUInt8(o + 27);
    state.ers = b.readFloatLE(o + 37);
    state.ersMode = b.readUInt8(o + 41);
    state.ersHarv = b.readFloatLE(o + 42) + b.readFloatLE(o + 46);
    state.ersDep = b.readFloatLE(o + 50);
  } else if (id === 10) { // Car Damage (46 byte / autó)
    const o = 29 + p * 42;
    state.tyreWear = [0, 1, 2, 3].map(i => b.readFloatLE(o + i * 4));
  }
  dirty = true;
});

udp.on('error', err => {
  console.error('UDP hiba:', err.message);
});

udp.on('listening', () => {
  const a = udp.address();
  console.log(`UDP figyel: ${a.address}:${a.port}`);
  const ips = Object.values(os.networkInterfaces()).flat()
    .filter(i => i.family === 'IPv4' && !i.internal).map(i => i.address);
  console.log(`Xboxon a UDP IP cím: ${ips.join(' vagy ')}  |  Port: ${UDP_PORT}`);
  console.log(`Weboldal: http://localhost:${HTTP_PORT}`);
});
udp.bind(UDP_PORT, '0.0.0.0');
process.on('SIGINT', () => { saveRaceMap(); saveActive(true); saveTrack(); process.exit(0); });
process.on('SIGTERM', () => { saveRaceMap(); saveActive(true); saveTrack(); process.exit(0); });

// ~30 Hz push a böngészőnek
setInterval(() => {
  const wasConnected = state.connected;
  state.connected = Date.now() - lastPacket < 3000;
  if (!dirty && !clients.size) return;
  const msg = `data: ${JSON.stringify(state)}\n\n`;
  for (const c of clients) c.write(msg);
  dirty = false;
}, 33);

let lapsSentRev = -1;
setInterval(() => {
  if (!clients.size) return;
  if (tt.dirty) {
    tt.dirty = false;
    const msg = `event: tt\ndata: ${JSON.stringify(ttPayload())}\n\n`;
    for (const c of clients) c.write(msg);
  }
  if (lapsSentRev !== tt.lapsRev) {
    lapsSentRev = tt.lapsRev;
    const msg = `event: laps\ndata: ${JSON.stringify(lapsPayload())}\n\n`;
    for (const c of clients) c.write(msg);
  }
}, 200);

// Time Trial pályanézet: külön, takarékos lekérdezés (nem terheli az SSE adatfolyamot).
function mapSamples(samples) {
  if (!Array.isArray(samples)) return [];
  const out = [];
  // Mintegy 600 pont elég a pályakörvonalhoz, a JSON archívumban minden minta megmarad.
  const step = Math.max(1, Math.ceil(samples.length / 650));
  for (let i = 0; i < samples.length; i += step) {
    const v = samples[i];
    if (!v || !Number.isFinite(v.lapDist) || !Number.isFinite(v.worldX) || !Number.isFinite(v.worldZ)) continue;
    if (v.lapDist < 0 || v.lapDist > tt.trackLen + 50) continue;
    out.push([Math.round(v.lapDist * 10) / 10, Math.round(v.worldX * 100) / 100, Math.round(v.worldZ * 100) / 100]);
  }
  return out;
}
function archivedMap(row) {
  if (!row || !row.file || !/^lap-[A-Za-z0-9_-]+\.json$/.test(row.file)) return [];
  try { return mapSamples(JSON.parse(fs.readFileSync(path.join(archiveDir(), row.file), 'utf8')).samples); }
  catch (_) { return []; }
}
function ttMapPayload() {
  const previousRow = tt.laps[tt.laps.length - 1];
  const bestRow = tt.laps.filter(l => l.ok && l.ms > 0).reduce((b,l) => !b || l.ms < b.ms ? l : b, null);
  return { trackId:tt.trackId, trackLen:tt.trackLen, seq:tt.seq, sid:tt.sid,
    active:mapSamples(archive.samples), previous:archivedMap(previousRow), best:archivedMap(bestRow) };
}

http.createServer((req, res) => {
  const route = (req.url || '/').split('?')[0];
  if (route === '/race/state') {
    res.writeHead(200, {'Content-Type':'application/json; charset=utf-8','Cache-Control':'no-store'});
    res.end(JSON.stringify(racePayload()));
  } else if (route === '/tt/map') {
    res.writeHead(200, {'Content-Type':'application/json; charset=utf-8', 'Cache-Control':'no-store'});
    res.end(JSON.stringify(ttMapPayload()));
  } else if (route === '/archive/list') {
    // A helyi archívum indexe; legfeljebb az 1000 legújabb kör.
    try {
      const rows = [];
      if (fs.existsSync(DATA_DIR)) for (const folder of fs.readdirSync(DATA_DIR)) {
        if (!/^track-\d+-\d+-laps$/.test(folder)) continue;
        const dir = path.join(DATA_DIR, folder);
        for (const name of fs.readdirSync(dir).filter(x => /^lap-[A-Za-z0-9_-]+\.json$/.test(x)).sort().reverse().slice(0, 1000)) {
          try { const j = JSON.parse(fs.readFileSync(path.join(dir, name), 'utf8'));
            if (j.status !== 'completed') continue;
            rows.push({key:folder+'/'+name, trackName:j.trackName || trackName(j.trackId), ms:j.ms, at:j.at, valid:j.valid, samples:j.capturedSamples});
          } catch (_) {}
        }
      }
      rows.sort((a,b)=>(b.at||0)-(a.at||0));
      res.writeHead(200, {'Content-Type':'application/json; charset=utf-8','Cache-Control':'no-store'});
      res.end(JSON.stringify(rows.slice(0,1000)));
    } catch(e) {res.writeHead(500);res.end('Archive read failed');}
  } else if (route === '/archive/lap') {
    try {
      const key = new URL(req.url, 'http://localhost').searchParams.get('key') || '';
      if (!/^track-\d+-\d+-laps\/lap-[A-Za-z0-9_-]+\.json$/.test(key)) {res.writeHead(400);return res.end('Invalid key');}
      const file = path.join(DATA_DIR, key);
      const data = fs.readFileSync(file);
      res.writeHead(200, {'Content-Type':'application/json; charset=utf-8','Cache-Control':'no-store'});
      res.end(data);
    } catch(e) {res.writeHead(404);res.end('Lap not found');}
  } else if (route === '/state') {
    res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
    res.end(JSON.stringify({ ...state, connected: Date.now() - lastPacket < 3000, tt: ttPayload(), laps: lapsPayload() }));
  } else if (route === '/events') {
    req.socket.setNoDelay(true);
    req.socket.setTimeout(0);
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache, no-transform',
      Connection: 'keep-alive',
      'X-Accel-Buffering': 'no',
    });
    res.flushHeaders();
    res.write('retry: 1000\n\n');
    res.write(`event: tt\ndata: ${JSON.stringify(ttPayload())}\n\n`);
    res.write(`event: laps\ndata: ${JSON.stringify(lapsPayload())}\n\n`);
    clients.add(res);
    req.on('close', () => clients.delete(res));
     req.on('error', () => clients.delete(res));
  } else if (route === '/' || route === '/index.html') {
    fs.readFile(path.join(__dirname, 'index.html'), (e, d) => {
      if (e) { res.writeHead(500); return res.end('index.html hiányzik'); }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(d);
    });
  } else {
    res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('Nem található.');
  }
}).listen(HTTP_PORT, () => console.log(`HTTP szerver figyel: ${HTTP_PORT}`));
