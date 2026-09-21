const DIRECT_API='https://fapi.binance.com';
// If direct calls to Binance fail (regional block, temporary CORS/network
// issue, rate-limit, etc.) the app automatically falls back to one of these
// public CORS proxies. Order matters: they're tried in sequence.
// If you deploy your own proxy (recommended for reliability — see
// cloudflare-worker.js), put its base URL here and it will be tried first.
const CUSTOM_PROXY_BASE=''; // e.g. 'https://your-worker.your-subdomain.workers.dev'
const CORS_PROXIES=[
  full=>`https://corsproxy.io/?url=${encodeURIComponent(full)}`,
  full=>`https://api.allorigins.win/raw?url=${encodeURIComponent(full)}`
];
const MODES=(CUSTOM_PROXY_BASE?[CUSTOM_PROXY_BASE]:[]).concat(['DIRECT'],CORS_PROXIES);
const TOTAL_MODES=MODES.length;
function buildUrl(mode,path){
  const full=DIRECT_API+path;
  const m=MODES[mode];
  if(m==='DIRECT')return full;
  if(typeof m==='function')return m(full);
  return m+path // custom proxy base acts like a direct passthrough host
}
let activeMode=Number(localStorage.getItem('scannerApiMode')||0);
if(!(Number.isInteger(activeMode)&&activeMode>=0&&activeMode<TOTAL_MODES))activeMode=0;
function modeLabel(m){return MODES[m]==='DIRECT'?'direct':(m===0&&CUSTOM_PROXY_BASE?'custom proxy':`proxy #${m}`)}

const INTERVALS=['1d'];
const UPDATE_INTERVAL=60*1000;
const INITIAL_CONCURRENCY=25;
const SMMA_LENGTH=40;
const M5_LIMIT=500; // ~1.7 days of 5m candles. Was 1500 (weight 10/call);
                     // 500 is weight 2/call, ~5x less API weight -> fits
                     // comfortably under Binance's rate limit in one pass.
const M5_REFRESH_INTERVAL=5*60*1000;

const state={
  coins:new Map(),
  flags:new Map(JSON.parse(localStorage.getItem('scannerFlags')||'[]')),
  sortKey:'volume',
  sortDir:-1,
  refreshInProgress:false,
  m5RefreshInProgress:false
};

const rowsEl=document.getElementById('rows');
const statusText=document.getElementById('statusText');
const statusDot=document.getElementById('statusDot');
const lastUpdate=document.getElementById('lastUpdate');
const sleep=ms=>new Promise(r=>setTimeout(r,ms));

function escapeHtml(s){
  return String(s).replace(/[&<>'"]/g,ch=>({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[ch]))
}
function setStatus(t,type=''){statusText.textContent=t;statusDot.className=`status-dot ${type}`}
async function fetchOnce(mode,path){
  const r=await fetch(buildUrl(mode,path),{cache:'no-store'});
  if(!r.ok){
    const err=new Error(`${r.status} ${r.statusText}`);
    err.status=r.status;
    throw err
  }
  return r.json()
}
async function getJSON(path,retriesPerMode=2){
  let lastErr;
  const attemptsLog=[];
  for(let step=0;step<TOTAL_MODES;step++){
    const mode=(activeMode+step)%TOTAL_MODES;
    let modeErr;
    for(let i=0;i<retriesPerMode;i++){
      try{
        const data=await fetchOnce(mode,path);
        if(mode!==activeMode){
          activeMode=mode;
          localStorage.setItem('scannerApiMode',String(activeMode));
          console.info('Binance scanner: switched connection mode to',modeLabel(mode));
        }
        return data
      }catch(x){
        lastErr=x;modeErr=x;
        console.warn(`Binance scanner: [${modeLabel(mode)}] attempt ${i+1} failed for ${path} —`,x.message||x);
        // Hard client-side errors (403/404/451 geo-block etc.) won't fix
        // themselves on retry — move straight to the next mode instead of
        // burning retries on a route that's actually blocked.
        if(x.status&&x.status!==429&&x.status<500)break;
        await sleep(600*(i+1))
      }
    }
    attemptsLog.push(`${modeLabel(mode)}: ${modeErr?.message||'network error (fetch blocked/failed)'}`)
  }
  const err=lastErr||new Error('Request failed on all connection modes');
  err.summary=attemptsLog.join(' | ');
  throw err
}
const fmtPrice=n=>{
  if(!Number.isFinite(n))return'--';
  const a=Math.abs(n),d=a>=1000?2:a>=1?2:a>=.1?4:a>=.01?6:8;
  return n.toLocaleString('en-US',{minimumFractionDigits:d,maximumFractionDigits:d})
};
const fmtVol=n=>{
  if(!Number.isFinite(n))return'--';
  if(n>=1e9)return(n/1e9).toFixed(2)+'B';
  if(n>=1e6)return(n/1e6).toFixed(1)+'M';
  if(n>=1e3)return(n/1e3).toFixed(1)+'K';
  return n.toFixed(0)
};
const fmtPct=n=>Number.isFinite(n)?`${n>=0?'+':''}${n.toFixed(2)}%`:'--';
function calcPct(open,price){return Number.isFinite(open)&&open!==0&&Number.isFinite(price)?((price-open)/open)*100:null}

async function loadCandle(symbol,interval){
  const d=await getJSON(`/fapi/v1/klines?symbol=${encodeURIComponent(symbol)}&interval=${interval}&limit=1`);
  if(!d?.length)return null;
  const k=d[0];
  return{open:Number(k[1]),openTime:Number(k[0]),closeTime:Number(k[6])}
}
function calculateDailyPercentages(){
  for(const c of state.coins.values())
    c.percent['1d']=c.candles['1d']?calcPct(c.candles['1d'].open,c.price):null
}
async function loadInitialCandles(){
  const tasks=[...state.coins.values()].map(c=>({c,i:'1d'}));
  let p=0,n=0;
  async function worker(){
    while(true){
      const x=p++;if(x>=tasks.length)return;
      const{c,i}=tasks[x];
      try{const k=await loadCandle(c.symbol,i);if(k)c.candles[i]=k}catch(e){console.warn('candle',c.symbol,e)}
      n++;
      if(n%20===0||n===tasks.length){
        setStatus(`Loading daily candle data… ${n}/${tasks.length}`);
        render()
      }
    }
  }
  await Promise.all(Array.from({length:Math.min(INITIAL_CONCURRENCY,tasks.length)},worker))
}

function smmaSeries(closes,len){
  if(closes.length<len)return[];
  const out=new Array(closes.length).fill(null);
  let sum=0;
  for(let i=0;i<len;i++)sum+=closes[i];
  let r=sum/len;
  out[len-1]=r;
  for(let i=len;i<closes.length;i++){
    r=((r*(len-1))+closes[i])/len;
    out[i]=r
  }
  return out
}

function computeSmmaFromClosed(closed){
  if(closed.length<SMMA_LENGTH)return null;
  const closes=closed.map(k=>Number(k[4]));
  const series=smmaSeries(closes,SMMA_LENGTH);
  const last=series.length-1;
  const smma=series[last];
  if(!Number.isFinite(smma))return null;

  let touch=-1;
  for(let i=SMMA_LENGTH-1;i<closed.length;i++){
    const ma=series[i],hi=Number(closed[i][2]),lo=Number(closed[i][3]);
    if(Number.isFinite(ma)&&lo<=ma&&hi>=ma)touch=i
  }

  return{
    smma,
    lastTouchTime:touch>=0?Number(closed[touch][0]):null,
    barsSinceTouch:touch>=0?last-touch:null,
    lastClosedOpenTime:Number(closed[last][0])
  }
}

// Full-window fetch: downloads M5_LIMIT candles. Only used for the initial
// load and as a one-off fallback for a symbol that has no cached window yet
// (e.g. it failed every attempt during the initial load).
async function loadFiveMinuteData(symbol){
  const d=await getJSON(`/fapi/v1/klines?symbol=${encodeURIComponent(symbol)}&interval=5m&limit=${M5_LIMIT}`);
  if(!Array.isArray(d)||d.length<SMMA_LENGTH)return null;
  const now=Date.now();
  const closed=d.filter(k=>Number(k[6])<=now);
  const result=computeSmmaFromClosed(closed);
  return result?{...result,raw:closed}:null
}

// Incremental refresh: downloads only the last few candles (cheap) and
// stitches them onto the window we already have cached in memory, instead
// of re-downloading the full M5_LIMIT history every time. This is what cuts
// bandwidth on every periodic refresh by roughly 100x.
const M5_REFRESH_TAIL=12; // small safety margin in case a refresh cycle is delayed/throttled
async function refreshOneSymbol5m(c){
  if(!c.candles5mRaw||!c.candles5mRaw.length){
    const m=await loadFiveMinuteData(c.symbol);
    if(!m)return false;
    c.candles5mRaw=m.raw;
    c.fiveMinute={smma:m.smma,lastTouchTime:m.lastTouchTime,barsSinceTouch:m.barsSinceTouch,lastClosedOpenTime:m.lastClosedOpenTime};
    return true
  }
  const d=await getJSON(`/fapi/v1/klines?symbol=${encodeURIComponent(c.symbol)}&interval=5m&limit=${M5_REFRESH_TAIL}`);
  if(!Array.isArray(d)||!d.length)return false;
  const now=Date.now();
  const freshClosed=d.filter(k=>Number(k[6])<=now);
  if(!freshClosed.length)return false;

  const freshOpenTimes=new Set(freshClosed.map(k=>Number(k[0])));
  const merged=c.candles5mRaw.filter(k=>!freshOpenTimes.has(Number(k[0]))).concat(freshClosed);
  merged.sort((a,b)=>Number(a[0])-Number(b[0]));
  const trimmed=merged.length>M5_LIMIT?merged.slice(merged.length-M5_LIMIT):merged;

  const result=computeSmmaFromClosed(trimmed);
  if(!result)return false;
  c.candles5mRaw=trimmed;
  c.fiveMinute=result;
  return true
}

async function loadInitialM5(){
  const cs=[...state.coins.values()];
  let pending=cs.slice();

  // Initial API calls can occasionally fail/rate-limit. Retry unresolved
  // symbols instead of leaving them permanently as "No touch yet".
  for(let attempt=1;attempt<=4 && pending.length;attempt++){
    let p=0,n=0;
    const batch=pending.slice();

    async function worker(){
      while(true){
        const x=p++;if(x>=batch.length)return;
        const c=batch[x];
        try{
          const m=await loadFiveMinuteData(c.symbol);
          if(m){
            c.candles5mRaw=m.raw;
            c.fiveMinute={smma:m.smma,lastTouchTime:m.lastTouchTime,barsSinceTouch:m.barsSinceTouch,lastClosedOpenTime:m.lastClosedOpenTime}
          }
        }catch(e){console.warn(`5m initial attempt ${attempt}`,c.symbol,e)}
        n++;
        if(n%10===0||n===batch.length){
          setStatus(`Loading 5m SMMA 40… ${n}/${batch.length} (attempt ${attempt})`);
          render();
        }
      }
    }

    await Promise.all(Array.from(
      {length:Math.min(INITIAL_CONCURRENCY,batch.length)},worker
    ));
    pending=batch.filter(c=>!c.fiveMinute);

    if(pending.length) await sleep(1200*attempt);
  }
}

async function refreshM5(){
  if(state.m5RefreshInProgress)return;
  state.m5RefreshInProgress=true;
  try{
    let pending=[...state.coins.values()];

    // Retry failed symbols during every refresh. Existing valid data is never
    // discarded when a temporary Binance request fails.
    for(let attempt=1;attempt<=3 && pending.length;attempt++){
      let p=0;
      const batch=pending.slice();
      const failed=[];

      async function worker(){
        while(true){
          const x=p++;if(x>=batch.length)return;
          const c=batch[x];
          try{
            const ok=await refreshOneSymbol5m(c);
            if(!ok)failed.push(c)
          }catch(e){console.warn(`5m refresh attempt ${attempt}`,c.symbol,e);failed.push(c)}
        }
      }

      await Promise.all(Array.from(
        {length:Math.min(INITIAL_CONCURRENCY,batch.length)},worker
      ));
      pending=failed;

      if(pending.length) await sleep(1000*attempt);
    }

    if(pending.length)
      console.warn('5m unresolved after retries:',pending.map(c=>c.symbol));
  }finally{state.m5RefreshInProgress=false}
}

function distance(c){
  return c.fiveMinute&&Number.isFinite(c.fiveMinute.smma)&&Number.isFinite(c.price)&&c.price!==0
    ?((c.fiveMinute.smma-c.price)/c.price)*100:null
}
function touchMinutes(c){
  return Number.isFinite(c.fiveMinute?.lastTouchTime)
    ?Math.max(0,Math.floor((Date.now()-c.fiveMinute.lastTouchTime)/60000)):null
}
function distanceCell(c){
  const d=distance(c);
  if(!Number.isFinite(d))return'<div class="metric-box neutral">--</div>';
  return`<div class="metric-box neutral"><span>${d>=0?'+':''}${d.toFixed(2)}%</span></div>`
}
function formatTouchElapsed(ts){
  if(!Number.isFinite(ts))return'--';
  const elapsed=Math.max(0,Date.now()-ts);
  const totalMinutes=Math.floor(elapsed/60000);
  const days=Math.floor(totalMinutes/1440);
  const hours=Math.floor((totalMinutes%1440)/60);
  const minutes=totalMinutes%60;
  if(days>0)return`${days}d ${hours}h ${minutes}m`;
  if(hours>0)return`${hours}h ${minutes}m`;
  return`${minutes}m`;
}
function timeCell(c){
  if(!Number.isFinite(c.fiveMinute?.lastTouchTime))
    return'<div class="metric-box neutral">--</div>';
  return`<div class="metric-box neutral"><span>${formatTouchElapsed(c.fiveMinute.lastTouchTime)}</span></div>`
}
function candleCell(p){
  if(!Number.isFinite(p))return'<div class="cell-box neutral">--</div>';
  const up=p>=0,strong=Math.abs(p)>=1;
  return`<div class="cell-box ${up?'up':'down'} ${strong?'strong':''}"><span class="arrow">${up?'↗':'↘'}</span>${fmtPct(p)}</div>`
}
function flagCell(c){
  const f=state.flags.get(c.symbol)||0;
  const label=f===1?'Red flag':f===2?'Blue flag':f===3?'Green flag':'No flag';
  const cl=f===1?'red':f===2?'blue':f===3?'green':'none';
  return`<div class="flag-wrap"><button class="flag-btn ${cl}" data-flag="${escapeHtml(c.symbol)}" aria-label="${label}" title="${label}">${f?'⚑':'⚐'}</button></div>`
}
function rowHTML(c){
  return`<td class="flag-cell">${flagCell(c)}</td>
  <td class="pair">${escapeHtml(c.symbol.replace('USDT','/USDT'))}</td>
  <td class="price">${fmtPrice(c.price)}</td>
  <td class="volume">${fmtVol(c.volume)}</td>
  <td>${candleCell(c.percent['1d'])}</td>
  <td>${distanceCell(c)}</td>
  <td>${timeCell(c)}</td>`
}
function sortValue(c,k){
  if(k==='flag')return state.flags.get(c.symbol)||0;
  if(k==='symbol')return c.symbol;
  if(k==='price')return c.price;
  if(k==='volume')return c.volume;
  if(k==='1d')return c.percent['1d']??-Infinity;
  if(k==='distance')return distance(c)??-Infinity;
  if(k==='time')return c.fiveMinute?.lastTouchTime??-Infinity;
  return 0
}
function sortedCoins(){
  const a=[...state.coins.values()];
  a.sort((x,y)=>{
    const av=sortValue(x,state.sortKey),bv=sortValue(y,state.sortKey);
    return typeof av==='string'
      ?state.sortDir*av.localeCompare(bv)
      :state.sortDir*((av??0)-(bv??0))
  });
  return a
}
function updateSortHeaders(){
  document.querySelectorAll('th[data-sort]').forEach(th=>{
    th.classList.toggle('sorted-asc',th.dataset.sort===state.sortKey&&state.sortDir===1);
    th.classList.toggle('sorted-desc',th.dataset.sort===state.sortKey&&state.sortDir===-1)
  })
}
function render(){
  const cs=sortedCoins();
  rowsEl.innerHTML=cs.length
    ?cs.map(c=>`<tr>${rowHTML(c)}</tr>`).join('')
    :'<tr><td colspan="7" class="loading">No coins found.</td></tr>';
  updateSortHeaders()
}
document.querySelectorAll('th[data-sort]').forEach(th=>th.addEventListener('click',()=>{
  const k=th.dataset.sort;
  if(state.sortKey===k)state.sortDir*=-1;
  else{state.sortKey=k;state.sortDir=k==='symbol'?1:-1}
  render()
}));
rowsEl.addEventListener('click',e=>{
  const b=e.target.closest('[data-flag]');
  if(!b)return;
  const s=b.dataset.flag,f=state.flags.get(s)||0,n=f>=3?0:f+1;
  if(n)state.flags.set(s,n);else state.flags.delete(s);
  localStorage.setItem('scannerFlags',JSON.stringify([...state.flags.entries()]));
  render()
});

function updateClockLabel(){
  const now=Date.now();
  const next=Math.ceil(now/M5_REFRESH_INTERVAL)*M5_REFRESH_INTERVAL;
  const remain=Math.max(0,next-now);
  const m=Math.floor(remain/60000).toString().padStart(2,'0');
  const s=Math.floor(remain%60000/1000).toString().padStart(2,'0');
  document.getElementById('time5m').textContent=`${m}:${s}`
}
function updateLast(){
  lastUpdate.textContent=new Date().toLocaleTimeString([],{hour:'2-digit',minute:'2-digit',second:'2-digit'})
}

async function refreshMarket(){
  if(state.refreshInProgress)return;
  state.refreshInProgress=true;
  try{
    setStatus('Updating market…');
    const ts=await getJSON('/fapi/v1/ticker/24hr');
    for(const t of ts){
      const c=state.coins.get(t.symbol);
      if(c){c.price=Number(t.lastPrice);c.volume=Number(t.quoteVolume)}
    }
    render();

    const now=Date.now(),tasks=[];
    for(const c of state.coins.values())
      if(!c.candles['1d']||now>=c.candles['1d'].closeTime)tasks.push(c);

    let p=0;
    async function w(){
      while(true){
        const x=p++;if(x>=tasks.length)return;
        const c=tasks[x];
        try{const k=await loadCandle(c.symbol,'1d');if(k)c.candles['1d']=k}catch(e){}
      }
    }
    await Promise.all(Array.from({length:Math.min(INITIAL_CONCURRENCY,tasks.length)},w));
    calculateDailyPercentages();
    render();
    updateLast();
    setStatus(`${state.coins.size} Binance USDT perpetuals • 5m SMMA 40 active${activeMode!==0?' • via '+modeLabel(activeMode):''}`,'ok')
  }catch(e){
    console.error(e);
    setStatus('Update failed • retrying in 3 minutes','error')
  }finally{state.refreshInProgress=false}
}

async function init(){
  try{
    setStatus('Loading Binance USDT perpetuals…');
    const[info,ts]=await Promise.all([
      getJSON('/fapi/v1/exchangeInfo'),
      getJSON('/fapi/v1/ticker/24hr')
    ]);
    const allowed=new Set(info.symbols
      .filter(s=>s.contractType==='PERPETUAL'&&s.quoteAsset==='USDT'&&s.status==='TRADING')
      .map(s=>s.symbol));
    const tm=new Map(ts.map(t=>[t.symbol,t]));

    for(const symbol of allowed){
      const t=tm.get(symbol);
      if(t)state.coins.set(symbol,{
        symbol,
        price:Number(t.lastPrice),
        volume:Number(t.quoteVolume),
        candles:{'1d':null},
        percent:{'1d':null},
        fiveMinute:null,
        candles5mRaw:null
      })
    }

    // Show the table immediately with price/volume filled in — no need to
    // wait for the candle/SMMA passes before the user sees anything.
    render();
    updateLast();
    setStatus(`Loading daily candle data… 0/${state.coins.size}`);

    await loadInitialCandles();
    calculateDailyPercentages();
    render();
    updateLast();
    setStatus(`Loading 5m SMMA 40 data… 0/${state.coins.size}`);

    await loadInitialM5();
    render();
    updateLast();
    setStatus(`${state.coins.size} Binance USDT perpetuals • 5m SMMA 40 active${activeMode!==0?' • via '+modeLabel(activeMode):''}`,'ok');
    updateClockLabel();
    setInterval(updateClockLabel,1000);
    // Keep LAST TOUCH as a live elapsed duration (e.g. 10h 5m).
    setInterval(()=>{ render(); },60000);
    setInterval(refreshMarket,UPDATE_INTERVAL);

    // Refresh SMMA data exactly after the 5m candle-close boundary.
    const scheduleFiveMinuteRefresh=()=>{
      const delay=Math.max(1000,Math.ceil(Date.now()/M5_REFRESH_INTERVAL)*M5_REFRESH_INTERVAL-Date.now()+1200);
      setTimeout(async()=>{
        await refreshM5();
        render();
        updateLast();
        setStatus(`${state.coins.size} Binance USDT perpetuals • 5m SMMA 40 active${activeMode!==0?' • via '+modeLabel(activeMode):''}`,'ok');
        scheduleFiveMinuteRefresh()
      },delay)
    };
    scheduleFiveMinuteRefresh();
  }catch(e){
    console.error(e);
    const detail=e.summary?` — ${e.summary}`:(e.message?` — ${e.message}`:'');
    setStatus(`Could not reach Binance via ${TOTAL_MODES} connection mode(s)${detail}`,'error');
    rowsEl.innerHTML=`<tr><td colspan="7" class="loading">${escapeHtml(e.summary||e.message)}</td></tr>`
  }
}
init();
