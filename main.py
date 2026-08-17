"""LeadFlow - unified inbound lead intake with AI triage.

Endpoints:
  GET  /            contact form (Hebrew) - what a customer sees
  GET  /ops         live view of the running system (demo only)
  POST /api/inquiry receives form submissions (JSON)
  GET  /webhook     Meta webhook verification handshake
  POST /webhook     incoming WhatsApp messages
  GET  /status      JSON: recent events, SLA breaches, retry queue, alerts
  GET  /health      liveness probe
"""

import asyncio
import hashlib
import hmac
import json
import logging
import os
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse

import config  # noqa: F401  loads .env - MUST come before the imports below,
#                             which read os.environ while they are importing
import business_hours
import crm
import intake
import reps
import sla
from pipeline import recent_events
from utils import normalize_phone, valid_email

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("leadflow")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Ownership setup, in order: make sure the custom property exists, then
    # ask the CRM where the round-robin left off. Neither call can raise, so a
    # HubSpot outage at boot delays the feature but never blocks the server.
    await crm.ensure_leadflow_properties()
    await crm.seed_rotation_from_crm()
    log.info("reps in rotation: %s", ", ".join(reps.REPS))
    recovered = intake.recover_in_progress()
    if recovered:
        log.warning("recovered %d interrupted intake item(s)", recovered)

    tasks = [
        asyncio.create_task(intake.processing_loop()),
        asyncio.create_task(sla.sla_loop()),
        asyncio.create_task(sla.silence_loop()),
        asyncio.create_task(crm.retry_loop()),
    ]
    log.info("background controllers started (intake / SLA / silence / retry)")
    yield
    for t in tasks:
        t.cancel()


app = FastAPI(title="LeadFlow", lifespan=lifespan)


# ---------------------------------------------------------------- demo page

PAGE = """<!DOCTYPE html>
<html dir="rtl" lang="he">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>צור קשר</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Assistant:wght@400;500;600&family=Rubik:wght@400;500&display=swap">
<style>
  :root{
    --paper:#f4f3ef; --card:#fff; --sunk:#f1efea;
    --ink:#14130f; --ink2:#6d6a62; --ink3:#a5a199; --rule:#e6e3dc;
    --clay:#ab4326; --ochre:#b8873a; --moss:#4a6a49;
    --body:'Assistant',system-ui,'Segoe UI',Arial,sans-serif;
    --disp:'Rubik','Assistant',system-ui,sans-serif;
  }
  *{box-sizing:border-box}
  html{-webkit-text-size-adjust:100%;overflow-x:hidden}
  body{margin:0;background:var(--paper);color:var(--ink);
    font:400 15px/1.65 var(--body);-webkit-font-smoothing:antialiased}
  .shell{max-width:1080px;margin:0 auto;padding:0 28px 64px}
  :focus-visible{outline:2px solid var(--ink);outline-offset:3px;border-radius:4px}
  .lat{direction:ltr;unicode-bidi:isolate;font-variant-numeric:tabular-nums}

  .top{display:flex;align-items:center;justify-content:space-between;
    gap:14px;flex-wrap:wrap;padding:22px 0}
  .badge{display:inline-flex;align-items:center;gap:9px;background:var(--card);
    border-radius:999px;padding:7px 16px;font-size:13px;color:var(--ink2)}
  .badge b{color:var(--ink);font-weight:600;letter-spacing:-.01em}
  .dot{width:6px;height:6px;border-radius:50%;background:var(--ink);flex:0 0 auto}
  .wa{display:inline-flex;align-items:center;gap:8px;font-size:13.5px;color:var(--ink2)}
  .wa svg{color:var(--moss)}

  .hero{display:grid;grid-template-columns:1fr auto;gap:22px 40px;
    align-items:end;padding:48px 0 34px}
  h1{margin:0;font-family:var(--disp);font-weight:500;
    font-size:clamp(44px,7.6vw,80px);line-height:.96;letter-spacing:-.038em}
  .hero p{margin:0 0 10px;max-width:36ch;color:var(--ink2);font-size:15px;line-height:1.6}

  .duo{display:grid;grid-template-columns:minmax(0,1.55fr) minmax(0,1fr);gap:18px}
  .panel{background:var(--card);border-radius:30px;padding:34px}
  .fields{display:grid;grid-template-columns:1fr 1fr;gap:16px 18px}
  .full{grid-column:1/-1}
  label{display:block;font-size:13px;font-weight:600;margin-bottom:7px}
  .optional{font-weight:400;color:var(--ink3)}
  input,textarea{width:100%;background:var(--sunk);border:1px solid transparent;
    border-radius:12px;padding:13px 15px;color:var(--ink);
    font:400 16px/1.5 var(--body);transition:background .15s,border-color .15s}
  textarea{min-height:124px;resize:vertical}
  ::placeholder{color:var(--ink3)}
  input:focus,textarea:focus{outline:none;background:var(--card);border-color:var(--ink)}
  .cta{margin-top:22px;display:inline-flex;align-items:center;gap:16px;cursor:pointer;
    background:var(--ink);color:#fff;border:0;border-radius:999px;
    padding-block:8px;padding-inline:26px 8px;font:600 15px var(--body);
    transition:opacity .15s}
  .cta:hover{opacity:.86}
  .cta:disabled{opacity:.45;cursor:default}
  .arrow{width:38px;height:38px;border-radius:50%;background:var(--card);color:var(--ink);
    display:grid;place-items:center;flex:0 0 auto}
  .ok{display:none;gap:11px;margin-top:18px;padding:14px 16px;border-radius:14px;
    background:var(--sunk);font-size:14px;line-height:1.5}
  .ok .dot{margin-top:7px;background:var(--ink3)}
  .ok.good .dot{background:var(--moss)}
  .ok.review .dot{background:var(--ochre)}
  .ok.bad .dot{background:var(--clay)}

  /* the one warm surface on the page */
  .promo{position:relative;overflow:hidden;border-radius:30px;padding:34px;color:#fff;
    display:flex;flex-direction:column;gap:26px;
    background:
      radial-gradient(120% 95% at 82% 6%, #d5893f 0%, rgba(213,137,63,0) 58%),
      radial-gradient(95% 85% at 6% 96%, #7d3219 0%, rgba(125,50,25,0) 62%),
      linear-gradient(158deg,#b4502c 0%,#8f3a1f 100%)}
  .promo::after{content:"";position:absolute;inset:0;pointer-events:none;opacity:.10;
    background-image:radial-gradient(#fff .8px,transparent .8px);background-size:13px 13px}
  .promo > *{position:relative;z-index:1}
  .pill{align-self:flex-start;font-size:12.5px;border-radius:999px;padding:5px 14px;
    background:rgba(255,255,255,.16);color:#fff}
  .promo-num{display:block;font-family:var(--disp);font-weight:500;
    font-size:clamp(56px,8vw,76px);line-height:.85;letter-spacing:-.04em}
  .promo-unit{display:block;margin-top:10px;font-size:15px;color:rgba(255,255,255,.86)}
  .promo-note{margin:14px 0 0;font-size:13.5px;line-height:1.6;color:rgba(255,255,255,.78)}
  .promo-foot{margin-top:auto;padding-top:22px;border-top:1px solid rgba(255,255,255,.22);
    font-size:13.5px;line-height:1.7;color:rgba(255,255,255,.86)}
  .promo-foot b{display:block;font-weight:600;color:#fff;font-size:13px;margin-bottom:3px}

  footer{margin-top:44px;padding-top:22px;border-top:1px solid var(--rule);
    display:flex;justify-content:space-between;align-items:center;gap:16px;
    flex-wrap:wrap;font-size:13px;color:var(--ink3)}
  footer a{color:var(--ink2);text-decoration:none;border-bottom:1px solid var(--rule)}
  footer a:hover{color:var(--ink);border-color:var(--ink)}

  @media (max-width:900px){
    .duo{grid-template-columns:1fr}
    .hero{grid-template-columns:1fr;align-items:start}
    .hero p{margin-bottom:0}
    .promo{min-height:280px}
  }
  @media (max-width:600px){
    .shell{padding:0 18px 52px}
    .panel,.promo{padding:26px 22px}
    .fields{grid-template-columns:1fr}
  }
  @media (prefers-reduced-motion:reduce){*{transition:none!important}}
</style>
</head>
<body>
<div class="shell">

  <div class="top">
    <span class="badge"><span class="dot"></span><b>LeadFlow</b></span>
    <span class="wa">
      <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M5 12.5 9.5 17 19 7.5"/></svg>
      הפנייה נשמרת מיד
    </span>
  </div>

  <header class="hero">
    <h1>נשמח<br>לשמוע מכם</h1>
    <p>השאירו דרך לחזור אליכם וכמה מילים על מה שאתם צריכים. הפנייה נשמרת מיד ומועברת לטיפול מסודר.</p>
  </header>

  <div class="duo">
    <section class="panel">
      <div class="fields">
        <div><label for="name">שם מלא</label>
          <input id="name" type="text" autocomplete="name" placeholder="ישראל ישראלי"></div>
        <div><label for="phone">טלפון <span class="optional">(טלפון או אימייל)</span></label>
          <input id="phone" type="tel" inputmode="tel" autocomplete="tel" placeholder="050-1234567"></div>
        <div class="full"><label for="email">אימייל <span class="optional">(אימייל או טלפון)</span></label>
          <input id="email" type="email" inputmode="email" autocomplete="email" placeholder="israel@example.com"></div>
        <div class="full"><label for="text">במה נוכל לעזור?</label>
          <textarea id="text" placeholder="היי, אשמח להצעת מחיר…"></textarea></div>
      </div>
      <button type="button" class="cta" id="cta" onclick="send()">
        <span id="ctatext">שליחת פנייה</span>
        <span class="arrow" aria-hidden="true"><svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M17 17 7 7"/><path d="M17 7H7v10"/></svg></span>
      </button>
      <div class="ok" id="ok" role="status" aria-live="polite"></div>
    </section>

    <aside class="promo">
      <div>
        <span class="pill">ההתחייבות שלנו</span>
        <span class="promo-num lat" id="promonum">4</span>
        <span class="promo-unit">שעות עבודה</span>
        <p class="promo-note">זה הזמן שבתוכו אנחנו שואפים לחזור לכל פנייה. חריגות מזוהות ומוצגות בתצוגת המערכת.</p>
      </div>
      <div class="promo-foot">
        <b>שעות פעילות</b>
        <span id="promohours">ראשון–חמישי, 09:00–18:00</span>
      </div>
    </aside>
  </div>

  <footer>
    <span>LeadFlow · קליטת פניות</span>
    <a href="/ops">תצוגת מערכת (דמו) ←</a>
  </footer>

</div>
<script>
const esc = v => String(v == null ? '' : v).replace(/[&<>"']/g,
  c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
let activeSubmissionId = null;

async function send() {
  const ok = document.getElementById('ok');
  const cta = document.getElementById('cta');
  const ctaText = document.getElementById('ctatext');
  const textEl = document.getElementById('text');
  const phoneEl = document.getElementById('phone');
  const emailEl = document.getElementById('email');
  const say = (state, msg) => {
    ok.className = 'ok ' + state;
    ok.style.display = 'flex';
    ok.innerHTML = '<span class="dot"></span><span>' + msg + '</span>';
  };

  if (!textEl.value.trim()) {
    say('bad', 'כתבו במה אפשר לעזור, ואז שלחו.');
    textEl.focus();
    return;
  }
  if (!phoneEl.value.trim() && !emailEl.value.trim()) {
    say('bad', 'השאירו טלפון או אימייל כדי שנוכל לחזור אליכם.');
    phoneEl.focus();
    return;
  }
  if (emailEl.value.trim() && !emailEl.checkValidity()) {
    say('bad', 'כתובת האימייל אינה תקינה.');
    emailEl.focus();
    return;
  }

  const body = {
    name: document.getElementById('name').value,
    phone: phoneEl.value,
    email: emailEl.value,
    text: textEl.value,
    submission_id: activeSubmissionId ||
      (globalThis.crypto && crypto.randomUUID ? crypto.randomUUID() :
       'web-' + Date.now() + '-' + Math.random().toString(16).slice(2)),
  };
  activeSubmissionId = body.submission_id;
  cta.disabled = true;
  ctaText.textContent = 'שולח…';
  say('', 'שולח את הפנייה…');
  try {
    const r = await fetch('/api/inquiry', { method: 'POST',
      headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body) });
    const d = await r.json();
    if (!r.ok) {
      say('bad', 'הפנייה לא נשלחה: ' + esc(d.error || r.status));
    } else {
      say('good', 'קיבלנו את הפנייה ושמרנו אותה במערכת. נחזור אליכם בהקדם, תודה!');
      textEl.value = '';
      activeSubmissionId = null;
    }
  } catch (e) {
    say('bad', 'שגיאה בשליחה: ' + esc(e));
  } finally {
    cta.disabled = false;
    ctaText.textContent = 'שליחת פנייה';
  }
}

// /status reports working hours the way the server means them
// ("sun,mon,tue,wed,thu 09:00-18:00 Asia/Jerusalem"). That is the right form
// for the ops view, and the wrong form for a customer. Same data, read out in
// Hebrew here - and if it ever stops matching that shape, show it verbatim
// rather than guess.
const DAYS = {sun:'ראשון', mon:'שני', tue:'שלישי', wed:'רביעי',
              thu:'חמישי', fri:'שישי', sat:'שבת'};
function humanHours(raw) {
  const verbatim = '<span class="lat">' + esc(raw) + '</span>';
  const parts = String(raw || '').split(' ');
  if (parts.length < 2) return verbatim;
  const codes = parts[0].split(',');
  const days = codes.map(d => DAYS[d]).filter(Boolean);
  if (days.length !== codes.length) return verbatim;
  if (!/^[0-9]{2}:[0-9]{2}-[0-9]{2}:[0-9]{2}$/.test(parts[1])) return verbatim;
  const span = days.length === 1 ? days[0] : days[0] + '–' + days[days.length - 1];
  // <bdi> so the two clock times keep their order inside the RTL sentence
  return esc(span) + ', <bdi>' + esc(parts[1].replace('-', '–')) + '</bdi>';
}

// The response promise is not decoration - it is the live SLA config.
async function loadPromise() {
  try {
    const r = await fetch('/status');
    const d = await r.json();
    document.getElementById('promonum').textContent = d.sla_hours;
    document.getElementById('promohours').innerHTML = humanHours(d.business_hours);
  } catch (e) {}
}
loadPromise();
</script>
</body>
</html>"""


OPS_PAGE = """<!DOCTYPE html>
<html dir="rtl" lang="he">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>LeadFlow · תצוגת מערכת</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Assistant:wght@400;500;600&family=Rubik:wght@400;500&display=swap">
<style>
  :root{
    --paper:#f4f3ef; --card:#fff; --sunk:#f1efea;
    --ink:#14130f; --ink2:#6d6a62; --ink3:#a5a199; --rule:#e6e3dc;
    --clay:#ab4326; --ochre:#b8873a; --moss:#4a6a49;
    --body:'Assistant',system-ui,'Segoe UI',Arial,sans-serif;
    --disp:'Rubik','Assistant',system-ui,sans-serif;
  }
  *{box-sizing:border-box}
  html{-webkit-text-size-adjust:100%;overflow-x:hidden}
  body{margin:0;background:var(--paper);color:var(--ink);
    font:400 15px/1.65 var(--body);-webkit-font-smoothing:antialiased}
  .shell{width:100%;max-width:1180px;margin:0 auto;padding:0 28px 80px}
  :focus-visible{outline:2px solid var(--ink);outline-offset:3px;border-radius:4px}
  .lat{direction:ltr;unicode-bidi:isolate;font-variant-numeric:tabular-nums}

  .top{display:flex;align-items:center;justify-content:space-between;gap:14px;
    flex-wrap:wrap;padding:22px 0}
  .badge{display:inline-flex;align-items:center;gap:9px;background:var(--card);
    border-radius:999px;padding:7px 16px;font-size:13px;color:var(--ink2)}
  .badge b{color:var(--ink);font-weight:600;letter-spacing:-.01em}
  .dot{width:6px;height:6px;border-radius:50%;background:var(--moss);flex:0 0 auto}
  .badge.off .dot{background:var(--clay)}
  .live .dot{animation:blink 2.6s ease-in-out infinite}
  @keyframes blink{0%,100%{opacity:1}50%{opacity:.25}}

  /* says plainly what this page is, so nobody mistakes it for a product screen */
  .disclosure{display:flex;gap:12px;background:var(--card);border-radius:18px;
    padding:16px 20px;margin-top:6px;font-size:13.5px;line-height:1.6;color:var(--ink2)}
  .disclosure b{color:var(--ink);font-weight:600}
  .disclosure svg{flex:0 0 auto;margin-top:3px;color:var(--ink3)}

  .hero{display:grid;grid-template-columns:1fr auto;gap:22px 40px;
    align-items:end;padding:44px 0 34px}
  .hero > *{min-width:0}
  h1{margin:0;font-family:var(--disp);font-weight:500;
    font-size:clamp(42px,7.4vw,80px);line-height:.96;letter-spacing:-.038em}
  h1 .q{color:var(--clay)}
  .hero p{margin:0 0 10px;max-width:40ch;color:var(--ink2);font-size:15px;line-height:1.6}

  .alerts{background:var(--card);border-radius:30px;padding:2px 30px;margin-bottom:16px}
  .alerts:empty{display:none}
  .alert{display:flex;gap:13px;padding:17px 0;border-top:1px solid var(--rule);
    font-size:14.5px;line-height:1.5}
  .alert:first-child{border-top:0}
  .alert .dot{margin-top:8px}
  .alert.warn .dot{background:var(--ochre)}
  .alert.bad .dot{background:var(--clay)}
  .alert b{font-weight:600}
  .alert span{color:var(--ink2)}

  .ledger{background:var(--card);border-radius:30px;padding:38px 40px 16px}
  .ledger-top{display:flex;justify-content:space-between;align-items:flex-end;
    gap:32px;padding-bottom:30px}
  .lab{margin:0 0 6px;font-size:13px;color:var(--ink2)}
  .big{display:block;font-family:var(--disp);font-weight:500;
    font-size:clamp(60px,9.5vw,96px);line-height:.86;letter-spacing:-.045em;color:var(--clay)}
  .big.clear{color:var(--ink)}
  .ledger-note{max-width:30ch;margin:0 0 8px;font-size:13.5px;line-height:1.55;color:var(--ink2)}

  .rep{border-top:1px solid var(--rule);padding:24px 0}
  .rep-head{display:flex;align-items:baseline;gap:14px}
  .rep-name{font-family:var(--disp);font-weight:500;font-size:26px;letter-spacing:-.024em}
  .rep.none .rep-name{color:var(--clay)}
  .rep-n{margin-inline-start:auto;font-size:13px;color:var(--ink2);white-space:nowrap}
  .leads{list-style:none;margin:16px 0 0;padding:0;display:grid;gap:11px}
  .lead{display:grid;grid-template-columns:minmax(88px,1.1fr) minmax(0,3fr) auto;
    gap:18px;align-items:center}
  .lead-name{font-size:14px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
  .track{height:3px;border-radius:2px;background:var(--rule);overflow:hidden}
  .track i{display:block;height:100%;background:var(--ochre);
    transform-origin:100% 50%;animation:grow .6s cubic-bezier(.2,.8,.25,1) both}
  .lv2 .track i{background:var(--clay)}
  .hrs{font-family:var(--disp);font-size:13.5px;color:var(--ink2);white-space:nowrap}
  .hrs b{font-weight:500;direction:ltr;unicode-bidi:isolate;font-variant-numeric:tabular-nums}
  .lv2 .hrs b{color:var(--clay)}
  .lead.unk .track{background:repeating-linear-gradient(135deg,
    var(--rule) 0 4px,transparent 4px 8px)}
  .lead.unk .hrs{color:var(--ink3)}
  .clear-note{border-top:1px solid var(--rule);padding:24px 0;font-size:15px;color:var(--ink2)}

  .meta{display:grid;grid-template-columns:repeat(3,1fr);gap:30px;
    padding:46px 0;margin-top:10px;border-bottom:1px solid var(--rule)}
  .mcol{text-align:center}
  .mic{width:46px;height:46px;margin:0 auto 15px;border-radius:50%;background:var(--card);
    display:grid;place-items:center;color:var(--ink)}
  .mt{font-weight:600;font-size:15px;margin-bottom:5px}
  .mv{color:var(--ink2);font-size:14px;line-height:1.55;word-break:break-word}

  .sec-head{display:flex;align-items:end;justify-content:space-between;
    gap:24px;padding:52px 0 22px}
  h2{margin:0;font-family:var(--disp);font-weight:500;font-size:clamp(28px,4vw,38px);
    line-height:1;letter-spacing:-.03em}
  .sec-head p{margin:0 0 4px;max-width:36ch;color:var(--ink2);font-size:14px}
  .panel{background:var(--card);border-radius:30px;padding:14px 34px}

  .evt{display:flex;flex-wrap:wrap;align-items:center;gap:9px 14px;
    padding:16px 0;border-top:1px solid var(--rule)}
  .evt:first-child{border-top:0}
  .evt time{font-family:var(--disp);font-size:13px;color:var(--ink3);
    direction:ltr;unicode-bidi:isolate;font-variant-numeric:tabular-nums}
  .src{font-size:13px;color:var(--ink2)}
  .cat{font-weight:600;font-size:15px;min-width:74px}
  .conf{display:inline-flex;align-items:center;gap:8px;font-size:12.5px;color:var(--ink2)}
  .cbar{width:46px;height:3px;border-radius:2px;background:var(--rule);overflow:hidden}
  .cbar i{display:block;height:100%;background:var(--moss)}
  .conf.low .cbar i{background:var(--ochre)}
  .conf b{font-family:var(--disp);font-weight:500;direction:ltr;unicode-bidi:isolate}
  .conf.low b{color:var(--ochre)}
  .rev{font-size:11.5px;border-radius:999px;padding:3px 10px;
    background:rgba(184,135,58,.13);color:#8a6524}
  .crm{margin-inline-start:auto;font-size:13px;color:var(--ink2);
    display:inline-flex;align-items:center;gap:8px}
  .crm .dot{background:var(--moss)}
  .crm.q .dot{background:var(--ochre)}
  .quiet{color:var(--ink2);font-size:14px;line-height:1.6;padding:22px 0}

  footer{margin-top:46px;padding-top:22px;border-top:1px solid var(--rule);
    display:flex;justify-content:space-between;align-items:center;gap:16px;
    flex-wrap:wrap;font-size:13px;color:var(--ink3)}
  footer a{color:var(--ink2);text-decoration:none;border-bottom:1px solid var(--rule)}
  footer a:hover{color:var(--ink);border-color:var(--ink)}

  @keyframes grow{from{transform:scaleX(0)}to{transform:scaleX(1)}}

  @media (max-width:900px){
    .hero{grid-template-columns:1fr;align-items:start}
    .hero p{margin-bottom:0}
    .ledger-top{flex-direction:column;align-items:flex-start;gap:18px}
    .ledger-note{margin-bottom:0;max-width:44ch}
  }
  @media (max-width:640px){
    .shell{padding:0 18px 60px}
    .ledger,.panel{padding:20px 22px}
    .alerts{padding:2px 22px}
    .meta{grid-template-columns:1fr;gap:0;padding:8px 0 0}
    .mcol{display:grid;grid-template-columns:46px 1fr;gap:16px;text-align:start;
      align-items:center;padding:20px 0;border-top:1px solid var(--rule)}
    .mic{margin:0}
    .sec-head{flex-direction:column;align-items:flex-start;gap:10px}
    .lead{grid-template-columns:1fr auto;gap:6px 14px}
    .lead .track{grid-column:1/-1;order:3}
    .crm{margin-inline-start:0;width:100%}
  }
  @media (prefers-reduced-motion:reduce){
    *,*::before,*::after{animation:none!important;transition:none!important}
  }
</style>
</head>
<body>
<div class="shell">

  <div class="top">
    <span class="badge"><span class="dot" style="background:var(--ink)"></span><b>LeadFlow</b></span>
    <span class="badge live" id="live"><span class="dot"></span><span id="livetext">מתעדכן כל 5 שניות</span></span>
  </div>

  <div class="disclosure">
    <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="M12 11v5M12 7.6v.5"/></svg>
    <div><b>העמוד הזה הוא חלון לדמו, לא מסך מוצר.</b>
    בפועל אנשי המכירות עובדים בתוך ה‑CRM ולא נכנסים לשום מסך נוסף, והמנכ״ל מקבל דוח.
    הוא קיים כאן רק כדי שאפשר יהיה לראות מה המערכת עשתה עם כל פנייה.</div>
  </div>

  <header class="hero">
    <h1>מה קורה<br>לכל פנייה<span class="q">?</span></h1>
    <p>פניות מהאתר נשמרות לפני העיבוד, מסווגות אוטומטית ונכתבות ל‑CRM עם שיוך לפי סבב. מתאם WhatsApp קיים בקוד, אך החיבור ל‑Meta עדיין לא אומת בסביבה חיה.</p>
  </header>

  <div class="alerts" id="alerts"></div>

  <section class="ledger" aria-labelledby="big-lab">
    <div class="ledger-top">
      <div>
        <p class="lab" id="big-lab">לידים שממתינים למענה</p>
        <span class="big" id="breachnum">—</span>
      </div>
      <p class="ledger-note" id="ledgernote"></p>
    </div>
    <div id="byrep"><p class="quiet">טוען…</p></div>
  </section>

  <div class="meta" id="repsline"></div>

  <div class="sec-head">
    <h2>מה נכנס למערכת</h2>
    <p>כל פנייה, הסיווג שקיבלה, ציון הביטחון ומה נכתב ל‑CRM.</p>
  </div>
  <section class="panel">
    <div id="events"><p class="quiet">טוען…</p></div>
  </section>

  <footer>
    <span>נתונים חיים מתוך <span class="lat">/status</span></span>
    <a href="/">← לטופס יצירת הקשר</a>
  </footer>

</div>
<script>
const CATS = {new_lead:'ליד חדש', existing_customer:'לקוח קיים',
  support:'תמיכה', spam:'ספאם', other:'אחר'};
const esc = v => String(v == null ? '' : v).replace(/[&<>"']/g,
  c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
// Hebrew reads badly with a bare "1" in front of a plural noun.
const plural = (n, one, many) => n === 1 ? one : esc(n) + ' ' + many;

const SVG = s => '<svg width="19" height="19" viewBox="0 0 24 24" fill="none" ' +
  'stroke="currentColor" stroke-width="1.5" stroke-linecap="round" ' +
  'stroke-linejoin="round">' + s + '</svg>';
const IC_REPS  = SVG('<circle cx="9.5" cy="8" r="3.2"/><path d="M3.6 19v-1.4A3.6 3.6 0 0 1 7.2 14h4.6a3.6 3.6 0 0 1 3.6 3.6V19"/><path d="M16.2 5.4a3.2 3.2 0 0 1 0 5.2"/><path d="M18 14.3a3.6 3.6 0 0 1 2.4 3.3V19"/>');
const IC_HOURS = SVG('<circle cx="12" cy="12" r="8.4"/><path d="M12 7.4V12l3 1.8"/>');
const IC_SLA   = SVG('<circle cx="12" cy="12" r="8.4"/><circle cx="12" cy="12" r="3.3"/>');

// Re-render a block only when its data actually changed, so the bars do not
// replay their entrance animation on every 5-second poll.
const sig = {};
function paint(id, data, html) {
  const s = JSON.stringify(data);
  if (sig[id] === s) return;
  sig[id] = s;
  document.getElementById(id).innerHTML = html();
}

function alertsHtml(d) {
  const out = [];
  if (d.silence_alert && d.silence_alert.active) out.push(['warn', 'התראת דממה',
    'לא נכנסו פניות כבר מעל ' + esc(d.silence_hours) + ' שעות עבודה. ייתכן שהטופס או הוובהוק שבורים.']);
  if (d.sla_breach_count) out.push(['bad', 'חריגה מיעד התגובה',
    plural(d.sla_breach_count, 'ליד אחד ממתין', 'לידים ממתינים') +
    ' למענה מעל ' + esc(d.sla_hours) + ' שעות עבודה.']);
  if (d.retry_queue > 0) out.push(['warn', 'תור ניסיונות חוזרים',
    plural(d.retry_queue, 'כתיבה אחת ל‑CRM ממתינה', 'כתיבות ל‑CRM ממתינות') +
    ' לניסיון נוסף. אף פנייה לא אבדה.']);
  if (d.intake_pending > 0) out.push(['warn', 'ממתין לעיבוד',
    plural(d.intake_pending, 'פנייה אחת נשמרה וממתינה', 'פניות נשמרו וממתינות') +
    ' לסיווג.']);
  if (d.intake_dead > 0) out.push(['bad', 'קליטה דורשת טיפול',
    plural(d.intake_dead, 'פנייה אחת נשמרה אך העיבוד שלה נכשל.',
      'פניות נשמרו אך העיבוד שלהן נכשל.')]);
  if (d.dead_letter > 0) out.push(['bad', 'דורש טיפול ידני',
    plural(d.dead_letter,
      'פנייה אחת מיצתה את כל הניסיונות ולא נכתבה ל‑CRM.',
      'פניות מיצו את כל הניסיונות ולא נכתבו ל‑CRM.')]);
  return out.map(a => '<div class="alert ' + a[0] + '"><span class="dot"></span>' +
    '<div><b>' + a[1] + '</b> <span>' + a[2] + '</span></div></div>').join('');
}

function repsHtml(d) {
  if (!d.sla_breaches.length) return '<p class="clear-note">' +
    'כל הפניות קיבלו מענה בתוך יעד הזמן. אין ליד שממתין מעל ' +
    esc(d.sla_hours) + ' שעות עבודה.</p>';

  let worst = 0;
  d.sla_breaches.forEach(g => g.leads.forEach(l => {
    if (l.hours_overdue > worst) worst = l.hours_overdue;
  }));
  if (!worst) worst = 1;

  return d.sla_breaches.map(g =>
    '<article class="rep' + (g.rep === 'ללא נציג' ? ' none' : '') + '">' +
      '<div class="rep-head"><span class="rep-name">' + esc(g.rep) + '</span>' +
      '<span class="rep-n">' + plural(g.count, 'ליד אחד בחריגה', 'לידים בחריגה') +
      '</span></div>' +
      '<ul class="leads">' + g.leads.map(l => {
        if (l.hours_overdue === null) return '<li class="lead unk">' +
          '<span class="lead-name">' + esc(l.name) + '</span>' +
          '<span class="track"></span><span class="hrs">זמן לא ידוע</span></li>';
        const lv = l.hours_overdue >= d.sla_hours ? ' lv2' : '';
        const w = Math.max(4, Math.round(l.hours_overdue / worst * 100));
        return '<li class="lead' + lv + '">' +
          '<span class="lead-name">' + esc(l.name) + '</span>' +
          '<span class="track"><i style="width:' + w + '%"></i></span>' +
          '<span class="hrs"><b>+' + esc(l.hours_overdue) + '</b> שעות</span></li>';
      }).join('') + '</ul>' +
    '</article>').join('');
}

function metaHtml(d) {
  const col = (icon, title, value) => '<div class="mcol"><span class="mic">' + icon +
    '</span><div><div class="mt">' + title + '</div>' +
    '<div class="mv">' + value + '</div></div></div>';
  return col(IC_REPS, 'נציגים בסבב',
      d.reps.map(esc).join(' · ') + '<br>הבא בתור: ' + esc(d.rotation_next)) +
    col(IC_HOURS, 'שעות עבודה', humanHours(d.business_hours)) +
    col(IC_SLA, 'יעד תגובה', esc(d.sla_hours) + ' שעות עבודה');
}

const DAYS = {sun:'ראשון', mon:'שני', tue:'שלישי', wed:'רביעי',
              thu:'חמישי', fri:'שישי', sat:'שבת'};
function humanHours(raw) {
  const verbatim = '<span class="lat">' + esc(raw) + '</span>';
  const parts = String(raw || '').split(' ');
  if (parts.length < 2) return verbatim;
  const codes = parts[0].split(',');
  const days = codes.map(d => DAYS[d]).filter(Boolean);
  if (days.length !== codes.length ||
      !/^[0-9]{2}:[0-9]{2}-[0-9]{2}:[0-9]{2}$/.test(parts[1])) return verbatim;
  const span = days.length === 1 ? days[0] : days[0] + '–' + days[days.length - 1];
  return esc(span) + ', <bdi>' + esc(parts[1].replace('-', '–')) + '</bdi>';
}

function eventsHtml(d) {
  if (!d.events.length) return '<p class="quiet">עדיין לא נכנסו פניות. שלחו אחת ' +
    '<a href="/">מטופס יצירת הקשר</a> כדי לראות את המסלול המלא — סיווג, ציון ביטחון וכתיבה ל‑CRM.</p>';
  return d.events.map(e => {
    const cat = CATS[e.category] || e.category;
    const queued = String(e.crm || '').indexOf('נכתב') !== 0;
    const conf = Math.max(0, Math.min(100, Number(e.confidence) || 0));
    return '<div class="evt">' +
      '<time>' + esc(e.at) + '</time>' +
      '<span class="src">' + esc(e.source) + '</span>' +
      '<span class="cat">' + esc(cat) + '</span>' +
      '<span class="conf' + (e.needs_review ? ' low' : '') + '">ביטחון ' +
        '<span class="cbar"><i style="width:' + conf + '%"></i></span>' +
        '<b>' + esc(e.confidence) + '</b></span>' +
      (e.needs_review ? '<span class="rev">לבדיקת אדם</span>' : '') +
      '<span class="crm' + (queued ? ' q' : '') + '"><span class="dot"></span>' +
        esc(e.crm) + '</span>' +
    '</div>';
  }).join('');
}

function connection(up) {
  document.getElementById('live').classList.toggle('off', !up);
  document.getElementById('livetext').textContent =
    up ? 'מתעדכן כל 5 שניות' : 'אין חיבור לשרת';
}

async function refresh() {
  try {
    const r = await fetch('/status');
    const d = await r.json();

    paint('alerts', [d.silence_alert.active, d.silence_hours, d.sla_breach_count,
                     d.sla_hours, d.retry_queue, d.dead_letter,
                     d.intake_pending, d.intake_dead], () => alertsHtml(d));

    const num = document.getElementById('breachnum');
    num.textContent = d.sla_breach_count;
    num.classList.toggle('clear', !d.sla_breach_count);
    document.getElementById('ledgernote').textContent =
      'ליד נחשב בחריגה אחרי ' + d.sla_hours +
      ' שעות עבודה בלי מענה. סופרים שעות עבודה בלבד, לא סופי שבוע.';

    paint('byrep', [d.sla_breaches, d.sla_hours], () => repsHtml(d));
    paint('repsline', [d.reps, d.rotation_next, d.business_hours, d.sla_hours],
      () => metaHtml(d));
    paint('events', d.events, () => eventsHtml(d));

    connection(true);
  } catch (e) { connection(false); }
}
refresh(); setInterval(refresh, 5000);
</script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
async def home() -> str:
    """The customer-facing contact form. Nothing internal belongs on this page."""
    return PAGE


@app.get("/ops", response_class=HTMLResponse)
async def ops() -> str:
    """A window into the running system, for the demo only.

    Deliberately a separate URL: the sales team works inside the CRM and the
    owner gets a report. This page exists so a human can watch what the
    pipeline decided, not because the product ships a dashboard.
    """
    return OPS_PAGE


# ---------------------------------------------------------------- intake

@app.post("/api/inquiry")
async def api_inquiry(request: Request) -> JSONResponse:
    data = await request.json()
    text = (data.get("text") or "").strip()
    if not text:
        return JSONResponse({"error": "empty inquiry"}, status_code=400)
    phone = data.get("phone")
    email = data.get("email")
    if not normalize_phone(phone) and not valid_email(email):
        return JSONResponse(
            {"error": "a valid phone number or email is required"},
            status_code=400,
        )
    try:
        accepted = intake.accept_inquiry(
            source="טופס אתר",
            text=text,
            phone=phone,
            email=email,
            name=data.get("name"),
            submission_id=data.get("submission_id"),
        )
    except intake.IdempotencyConflict as exc:
        return JSONResponse({"error": str(exc)}, status_code=409)
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    except Exception as exc:  # noqa: BLE001 - no durable write means no 202
        log.error("intake persistence failed: %s", exc)
        return JSONResponse({"error": "could not safely store inquiry"}, status_code=503)
    return JSONResponse(accepted, status_code=202)


@app.get("/api/inquiry/{submission_id}")
async def inquiry_status(submission_id: str) -> JSONResponse:
    row = intake.get_status(submission_id)
    if row is None:
        return JSONResponse({"error": "not found"}, status_code=404)
    # Deliberately no payload or AI result: this endpoint is unauthenticated.
    return JSONResponse(row)


# ------------------------------------------------------- WhatsApp webhook

@app.get("/webhook")
async def whatsapp_verify(request: Request) -> PlainTextResponse:
    """Meta's one-time verification handshake."""
    params = request.query_params
    if params.get("hub.verify_token") == os.environ.get("WHATSAPP_VERIFY_TOKEN", "leadflow"):
        return PlainTextResponse(params.get("hub.challenge", ""))
    return PlainTextResponse("verification failed", status_code=403)


def verify_meta_signature(raw_body: bytes, header: str | None) -> bool:
    """Check Meta's X-Hub-Signature-256 against the RAW request body.

    Meta signs the exact bytes it sent. Re-serializing the parsed JSON would
    produce different bytes (key order, spacing) and every signature would
    fail, so this must never be handed a re-encoded payload.

    No APP_SECRET set -> accept, so local testing and the demo still work.
    That is a deliberate hole with a loud warning, not an oversight.
    """
    secret = os.environ.get("APP_SECRET")
    if not secret:
        log.warning("APP_SECRET not set - accepting webhook WITHOUT signature "
                    "verification (fine locally, not fine in production)")
        return True
    if not header or not header.startswith("sha256="):
        return False
    expected = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
    # compare_digest, not ==, so the comparison time doesn't leak the signature
    return hmac.compare_digest(expected, header.split("=", 1)[1].strip())


@app.post("/webhook")
async def whatsapp_incoming(request: Request) -> JSONResponse:
    """Acknowledge fast, process async - Meta expects a quick 200."""
    raw_body = await request.body()
    if not verify_meta_signature(raw_body, request.headers.get("X-Hub-Signature-256")):
        log.warning("webhook REJECTED: bad or missing signature (%d bytes)", len(raw_body))
        return JSONResponse({"error": "invalid signature"}, status_code=403)

    try:
        payload = json.loads(raw_body)
    except Exception as e:  # noqa: BLE001 - unparseable body must not 500 to Meta
        log.warning("webhook body was not valid JSON: %s", e)
        return JSONResponse({"status": "ignored"})

    try:
        for entry in payload.get("entry", []):
            for change in entry.get("changes", []):
                for msg in change.get("value", {}).get("messages", []) or []:
                    if msg.get("type") == "text":
                        accepted = intake.accept_inquiry(
                            source="וואטסאפ",
                            text=msg["text"]["body"],
                            phone=msg.get("from"),
                            submission_id=(f"whatsapp:{msg['id']}"
                                           if msg.get("id") else None),
                        )
                        if not accepted["duplicate"]:
                            asyncio.create_task(_ack_whatsapp(msg.get("from")))
    except Exception as e:  # noqa: BLE001 - a malformed payload must not 500 to Meta
        # Returning a failure is intentional: Meta will retry, and the message
        # ID makes that retry idempotent. A 200 here would acknowledge data we
        # did not durably save.
        log.warning("webhook could not be durably stored: %s", e)
        return JSONResponse({"error": "temporary intake failure"}, status_code=503)
    return JSONResponse({"status": "received"})


async def _ack_whatsapp(to: str | None) -> None:
    """Optional auto-acknowledgment, only if send credentials are configured."""
    token = os.environ.get("WHATSAPP_TOKEN")
    phone_id = os.environ.get("WHATSAPP_PHONE_ID")
    if not (token and phone_id and to):
        return
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(
                f"https://graph.facebook.com/v20.0/{phone_id}/messages",
                headers={"Authorization": f"Bearer {token}"},
                json={"messaging_product": "whatsapp", "to": to, "type": "text",
                      "text": {"body": "קיבלנו את פנייתך, נחזור אליך בהקדם. תודה!"}},
            )
    except Exception as e:  # noqa: BLE001
        log.warning("ack send failed (non-critical): %s", e)


# ---------------------------------------------------------------- status

@app.get("/status")
async def status() -> dict:
    return {
        "events": recent_events,
        # Grouped by rep: [{"rep", "count", "leads": [{"name", "hours_overdue"}]}]
        "sla_breaches": sla.current_breaches,
        "sla_breach_count": crm.breach_total(sla.current_breaches),
        "sla_hours": sla.SLA_HOURS,
        # Stated explicitly: "4 hours" means four WORKING hours, and here is
        # the definition being used. The owner should never have to guess.
        "business_hours": business_hours.describe(),
        "silence_alert": sla.silence_alert,
        "silence_hours": sla.SILENCE_HOURS,
        "retry_queue": crm.queue_size(),
        "dead_letter": crm.dead_letter_size(),
        "intake_pending": intake.pending_size(),
        "intake_dead": intake.dead_size(),
        "reps": reps.REPS,
        "rotation_next": reps.REPS[reps.current_position()],
    }


@app.get("/health")
async def health() -> dict:
    return {"ok": True}
