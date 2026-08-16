"""LeadFlow - unified inbound lead intake with AI triage.

Endpoints:
  GET  /            demo contact form (Hebrew) + live system view
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
import reps
import sla
from pipeline import handle_inquiry, recent_events

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("leadflow")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Ownership setup, in order: make sure the custom property exists, then
    # ask the CRM where the round-robin left off. Neither call can raise, so a
    # HubSpot outage at boot delays the feature but never blocks the server.
    await crm.ensure_rep_property()
    await crm.seed_rotation_from_crm()
    log.info("reps in rotation: %s", ", ".join(reps.REPS))

    tasks = [
        asyncio.create_task(sla.sla_loop()),
        asyncio.create_task(sla.silence_loop()),
        asyncio.create_task(crm.retry_loop()),
    ]
    log.info("background controllers started (SLA / silence / retry)")
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
<title>LeadFlow - דמו</title>
<style>
  body { font-family: Arial, sans-serif; background: #f4f5f7; margin: 0; padding: 16px; color: #1a1a1a; }
  .wrap { max-width: 560px; margin: 0 auto; }
  h1 { font-size: 22px; margin: 8px 0 2px; }
  .sub { color: #666; font-size: 14px; margin-bottom: 16px; }
  .card { background: #fff; border-radius: 10px; padding: 16px; margin-bottom: 14px;
          box-shadow: 0 1px 3px rgba(0,0,0,.08); }
  label { display: block; font-size: 14px; margin: 10px 0 4px; }
  input, textarea { width: 100%; box-sizing: border-box; padding: 10px; font-size: 15px;
          border: 1px solid #ccc; border-radius: 8px; font-family: inherit; }
  textarea { min-height: 90px; }
  button { margin-top: 14px; width: 100%; padding: 12px; font-size: 16px; border: 0;
          border-radius: 8px; background: #1f6feb; color: #fff; }
  .ok { background: #e6f6ea; border-radius: 8px; padding: 10px; margin-top: 12px;
        font-size: 14px; display: none; }
  .evt { border-top: 1px solid #eee; padding: 8px 0; font-size: 13px; }
  .tag { display: inline-block; padding: 1px 8px; border-radius: 10px; font-size: 12px;
         background: #eef; margin-left: 6px; }
  .rev { background: #ffe9c7; }
  .rep { border-top: 1px solid #eee; padding: 8px 0; font-size: 13px; }
  .repname { font-weight: bold; }
  .lead { color: #555; font-size: 12px; padding-right: 12px; }
  .late { color: #c0392b; }
  .muted { color: #888; font-size: 12px; }
  .alert { background: #ffe0e0; border-radius: 8px; padding: 10px; font-size: 14px;
           margin-bottom: 10px; }
  h2 { font-size: 16px; margin: 4px 0 8px; }
</style>
</head>
<body>
<div class="wrap">
  <h1>צור קשר</h1>
  <div class="sub">דמו של מערכת קליטת פניות - כל שליחה הופכת לליד אמיתי ב-CRM</div>

  <div class="card">
    <label>שם מלא</label><input id="name" placeholder="ישראל ישראלי">
    <label>טלפון</label><input id="phone" placeholder="050-1234567">
    <label>אימייל</label><input id="email" placeholder="israel@example.com">
    <label>במה נוכל לעזור?</label><textarea id="text" placeholder="היי, אשמח להצעת מחיר..."></textarea>
    <button onclick="send()">שליחה</button>
    <div class="ok" id="ok"></div>
  </div>

  <div class="card">
    <h2>חריגות זמן תגובה לפי נציג</h2>
    <div class="muted" id="repsline"></div>
    <div id="byrep">טוען...</div>
  </div>

  <div class="card">
    <h2>מה קורה במערכת (תצוגה חיה)</h2>
    <div id="alerts"></div>
    <div id="events">טוען...</div>
  </div>
</div>
<script>
async function send() {
  const body = {
    name: document.getElementById('name').value,
    phone: document.getElementById('phone').value,
    email: document.getElementById('email').value,
    text: document.getElementById('text').value,
  };
  const ok = document.getElementById('ok');
  ok.style.display = 'block';
  ok.textContent = 'שולח...';
  try {
    const r = await fetch('/api/inquiry', { method: 'POST',
      headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body) });
    const d = await r.json();
    ok.textContent = d.needs_review
      ? 'הפנייה התקבלה והועברה לבדיקת אדם (המערכת לא הייתה בטוחה בסיווג)'
      : 'הפנייה התקבלה, סווגה ונכתבה ל-CRM. תודה!';
    document.getElementById('text').value = '';
    refresh();
  } catch (e) { ok.textContent = 'שגיאה בשליחה: ' + e; }
}
async function refresh() {
  try {
    const r = await fetch('/status');
    const d = await r.json();
    const alerts = [];
    if (d.silence_alert.active) alerts.push('התראת דממה: לא נכנסו פניות כבר מעל ' + d.silence_hours + ' שעות');
    if (d.sla_breach_count) alerts.push('חריגת זמן תגובה: ' + d.sla_breach_count + ' לידים בלי מענה מעל ' + d.sla_hours + ' שעות עבודה');
    if (d.retry_queue > 0) alerts.push('תור ניסיונות חוזרים: ' + d.retry_queue + ' כתיבות ממתינות (ה-CRM לא זמין?)');
    if (d.dead_letter > 0) alerts.push('דורש טיפול ידני: ' + d.dead_letter + ' פניות מיצו את כל הניסיונות ולא נכתבו ל-CRM');
    document.getElementById('alerts').innerHTML =
      alerts.map(a => '<div class="alert">' + a + '</div>').join('');
    document.getElementById('repsline').textContent =
      'נציגים בסבב: ' + d.reps.join(', ') + ' | הבא בתור: ' + d.rotation_next +
      ' | שעות עבודה: ' + d.business_hours;
    document.getElementById('byrep').innerHTML = d.sla_breaches.length
      ? d.sla_breaches.map(g =>
          '<div class="rep"><span class="repname">' + g.rep + '</span>' +
          ' <span class="tag">' + g.count + ' לידים בחריגה</span>' +
          g.leads.map(l => '<div class="lead">' + l.name +
            ' <span class="late">' +
            (l.hours_overdue === null ? 'זמן לא ידוע' : 'באיחור ' + l.hours_overdue + ' שעות') +
            '</span></div>').join('') +
          '</div>').join('')
      : 'אין כרגע לידים בחריגת זמן תגובה.';
    document.getElementById('events').innerHTML = d.events.length
      ? d.events.map(e =>
          '<div class="evt">' + e.at + ' | ' + e.source +
          ' <span class="tag">' + e.category + '</span>' +
          ' <span class="tag">ביטחון ' + e.confidence + '</span>' +
          (e.needs_review ? ' <span class="tag rev">לבדיקת אדם</span>' : '') +
          ' | ' + e.crm + '</div>').join('')
      : 'עדיין אין פניות. שלח אחת למעלה.';
  } catch (e) {}
}
refresh(); setInterval(refresh, 5000);
</script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
async def home() -> str:
    return PAGE


# ---------------------------------------------------------------- intake

@app.post("/api/inquiry")
async def api_inquiry(request: Request) -> JSONResponse:
    data = await request.json()
    text = (data.get("text") or "").strip()
    if not text:
        return JSONResponse({"error": "empty inquiry"}, status_code=400)
    name_line = f"שם שהוזן בטופס: {data.get('name')}\n" if data.get("name") else ""
    result = await handle_inquiry(
        source="טופס אתר",
        text=name_line + text,
        phone=data.get("phone"),
        email=data.get("email"),
    )
    return JSONResponse({"needs_review": result["needs_review"]})


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
                        asyncio.create_task(handle_inquiry(
                            source="וואטסאפ",
                            text=msg["text"]["body"],
                            phone=msg.get("from"),
                        ))
                        asyncio.create_task(_ack_whatsapp(msg.get("from")))
    except Exception as e:  # noqa: BLE001 - a malformed payload must not 500 to Meta
        log.warning("webhook parse issue (payload logged): %s", e)
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
        "reps": reps.REPS,
        "rotation_next": reps.REPS[reps.current_position()],
    }


@app.get("/health")
async def health() -> dict:
    return {"ok": True}
