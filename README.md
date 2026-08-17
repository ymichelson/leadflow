# LeadFlow

**דמו חי:** [טופס יצירת קשר](https://web-production-b50b3.up.railway.app/) ·
[תצוגת מערכת](https://web-production-b50b3.up.railway.app/ops)

שלב ב' של פרויקט הבית: פרוסה אנכית שמקבלת פנייה מטופס אתר, מסווגת אותה,
מונעת כפילות, כותבת אותה ל-HubSpot ושומרת אותה לניסיון חוזר אם ה-CRM אינו זמין.

## מה בחרתי להוכיח

המסלול המרכזי הוא:

```text
טופס אתר -> שמירה עמידה + 202 -> worker -> נרמול -> Gemini/Claude
          -> סף ביטחון -> חיפוש ב-HubSpot -> יצירה/עדכון -> retry במקרה כשל
```

זו אינה המערכת המלאה משלב א'. זו הפרוסה שעליה שאר המערכת נשענת: אם הפנייה
לא נקלטה ונכתבה בצורה אמינה, אין משמעות לניתוב או למדידת זמני תגובה.

## מה עובד היום

- טופס אתר אמיתי בעברית ו-API לקליטת הפנייה.
- inbox עמיד ואידמפוטנטי: הפנייה נשמרת ב-SQLite לפני החזרת `202`. מזהה
  submission מונע עיבוד כפול, ו-worker משחזר עבודה שנקטעה אחרי restart.
- אחרי השלמה, הטקסט ופרטי הקשר נמחקים מה-inbox; נשאר hash שמאפשר לזהות retry
  בלי להפוך את SQLite לעותק נוסף של ה-CRM.
- נרמול מספרי טלפון ישראליים כדי שאותו אדם לא ייווצר פעמיים בפורמטים שונים.
- adapters ל-Claude ול-Gemini שמחזירים אותו חוזה: קטגוריה, דחיפות, תקציר
  וציון ביטחון. `AI_PROVIDER` בוחר ספק בלי לשנות את שאר ה-pipeline.
- validation בקוד: ערך שהמודל המציא נפסל ועובר לבדיקת אדם.
- חיבור אמיתי ל-HubSpot: חיפוש לפי טלפון או אימייל, יצירת contact או הוספת note.
- שדות HubSpot ייעודיים לקטגוריה, דחיפות ו-`needs_review`, כדי שהחלטת המערכת
  תהיה ניתנת לחיפוש ולא קבורה רק בטקסט של note.
- ספאם בביטחון גבוה נשמר כ-`UNQUALIFIED`; פנייה לא ודאית נשארת `NEW` ומסומנת
  לבדיקת אדם. בדיקת אדם אינה מסומנת בטעות כ-`ATTEMPTED_TO_CONTACT`.
- תור retry ב-SQLite עם backoff ו-dead-letter. ניסיון חוזר שומר את כל המידע
  שהיה נכתב בניסיון הראשון.
- שעון שעות עבודה ישראלי ובקר SLA ניסיוני.
- מסך `/ops` שממחיש את פעולת המערכת לצורך הדמו. הוא אינו מוצר נוסף לצוות.

## מה אמיתי, ומה עדיין אב-טיפוס

### אומת בפועל

- טוקן HubSpot המקומי תקף.
- השדה `leadflow_assigned_rep` קיים בחשבון אמיתי.
- השדות `leadflow_category`, `leadflow_urgency` ו-`leadflow_needs_review`
  נוצרו ואומתו בחשבון האמיתי.
- יש בחשבון אנשי קשר שקיבלו ערך בשדה הזה.
- בדיקות אוטומטיות מכסות את הזרימה: intake עמיד, idempotency, restart,
  ליד חדש, לקוח קיים, כשל CRM ו-retry.
- זרימת website-to-CRM אומתה מקצה לקצה על contact סינתטי מסומן: Gemini החזיר
  `new_lead` בביטחון 95, HubSpot עדכן את אותו contact במקום ליצור כפילות,
  והשדות נשמרו כ-`NEW`, `normal` ו-`needs_review=false` עם note חדש.

### ספקי AI

- Gemini מוגדר כספק הדמו דרך `AI_PROVIDER=gemini`. הוא משתמש ב-structured
  output עם JSON Schema, ולא רק מבקש JSON בפרומפט.
- חיבור Gemini אומת בפועל. ה-eval הראשון קיבל 13/16 וחשף שתי טעויות בטוחות
  בגלל חפיפה בין `support` ל-`existing_customer`. אחרי הגדרת precedence עסקי
  מפורש התקבלה תוצאה של 16/16, עם כל ארבע הפניות העמומות מתחת לסף ובלי טעות
  בטוחה. זו ריצת מדידה אחת על 16 דוגמאות סינתטיות, לא טענת 100% ב-production.
- בהרצה עם free tier נחשפה מגבלת קצב נמוכה; `python eval.py --delay 13` קיים
  כדי למדוד בלי להציף חשבון חינמי. הדמו הנוכחי משתמש בפרויקט עם billing.
- Claude נשאר adapter נתמך בהתאם לתכנון משלב א', אך אין כרגע
  `ANTHROPIC_API_KEY` מקומי. בחירת ספק production דורשת השוואה על פניות
  היסטוריות אמיתיות של הלקוח.
- `eval.py --dry-run` בודק רק את מנגנון המדידה ואינו תוצאת מודל.

### קיים בקוד אך לא אומת מקצה לקצה בסביבה הנוכחית
- WhatsApp: קיים webhook בפורמט Meta ואימות חתימה, אבל אין כרגע App Secret,
  token ומספר עסקי שמוכיחים תנועה אמיתית. בלי `APP_SECRET` ה-endpoint דוחה
  בקשות, כדי שדמו ציבורי לא יקבל webhook מזויף. ה-adapter שומר הודעה לפני 200
  ומשתמש ב-message ID למניעת כפילות, אבל ללא Meta אמיתי הוא עדיין ניסיוני.

### לא נבנה

- קליטת אימיילים.
- דוח שבועי שנשלח למנכ"ל.
- מענה אוטומטי מלא ללקוח.
- מערכת production מרובת workers.

## שתי מגבלות שחשוב לומר בקול

### השיוך לנציג הוא מדיניות מודגמת, לא HubSpot ownership

הקוד בוחר שם נציג ב-round-robin ושומר אותו בשדה `leadflow_assigned_rep`. זה
מדגים את כלל השיוך ואת קיבוץ הדוח, אבל אינו כותב `hubspot_owner_id`. בחיבור
ללקוח אמיתי צריך למפות את הנציגים ל-owner IDs של ה-CRM.

### בקר ה-SLA הוא prototype

כרגע הבקר מחפש contacts ישנים שעדיין בסטטוס `NEW`. זה proxy שימושי לדמו, אבל
לא מדידה מלאה של תגובה ראשונה: contact אינו inquiry, ושינוי סטטוס אינו בהכרח
שיחה עם הלקוח. לקוח קיים שפונה שוב דורש שעון חדש, ותגובה אמיתית צריכה להגיע
מאירוע call/email/WhatsApp או משדה CRM מוסכם.

הממצא הזה הוא אחת המסקנות המרכזיות של שלב ג', לא משהו שמסתירים.

## מבנה הקוד

- `main.py` - FastAPI, טופס הדמו, WhatsApp adapter, `/ops`, `/status` ו-lifespan.
- `intake.py` - durable inbox, idempotency, recovery ו-worker אסינכרוני.
- `pipeline.py` - נרמול, סיווג, סף ביטחון והעברה ל-CRM.
- `classify.py` - adapters ל-Gemini/Claude, validation ו-fail-open לבדיקת אדם.
- `crm.py` - HubSpot, dedupe, contact/note, שדות LeadFlow, SLA ו-retry queue.
- `reps.py` - מדיניות round-robin לדמו.
- `business_hours.py` - חישוב שעות עבודה לפי `Asia/Jerusalem`.
- `sla.py` - בקרי SLA ודממה.
- `eval.py` - harness למדידת המסווג על 16 מקרי בדיקה ריאליסטיים.
- `test_leadflow.py` - בדיקות לוגיקה ואורקסטרציה.

## הרצה

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
uvicorn main:app --reload
```

פתחו:

- `http://localhost:8000/` - טופס לקוח.
- `http://localhost:8000/ops` - חלון הדמו למערכת.
- `http://localhost:8000/status` - מצב JSON.

בדיקות:

```bash
pip install -r requirements-dev.txt
pytest test_leadflow.py
python eval.py --dry-run
```

`--dry-run` משתמש במסווג מזויף ואינו מודד את Claude. למדידה אמיתית:

```bash
python eval.py
```

ב-Gemini free tier יש מגבלת קצב נמוכה. להרצה מלאה בלי לחרוג ממנה:

```bash
python eval.py --delay 13
```

## משתני סביבה חשובים

| משתנה | תפקיד |
|---|---|
| `ANTHROPIC_API_KEY` | מאפשר סיווג אמיתי ב-Claude |
| `CLAUDE_MODEL` | מודל Claude; ברירת המחדל מוגדרת ב-`classify.py` |
| `GEMINI_API_KEY` | מאפשר סיווג ב-Gemini Developer API |
| `GEMINI_MODEL` | ברירת מחדל `gemini-3.5-flash` |
| `AI_PROVIDER` | `gemini` או `anthropic` |
| `HUBSPOT_TOKEN` | HubSpot Private App token |
| `WHATSAPP_VERIFY_TOKEN` | Meta verification handshake |
| `APP_SECRET` | אימות חתימת webhook; חובה לפני חיבור אמיתי |
| `WHATSAPP_TOKEN`, `WHATSAPP_PHONE_ID` | אישור קבלה אופציונלי ב-WhatsApp |
| `CONFIDENCE_THRESHOLD` | מתחת לסף הפנייה דורשת בדיקת אדם; ברירת מחדל 70 |
| `REPS` | שמות הנציגים במדיניות הדמו |
| `SLA_HOURS` | יעד התגובה בשעות עבודה; ברירת מחדל 4 |
| `WORK_DAYS`, `WORK_START`, `WORK_END` | הגדרת שבוע ושעות העבודה |
| `BUSINESS_TZ` | ברירת מחדל `Asia/Jerusalem` |
| `STATE_DB` | קובץ SQLite של ה-inbox ותור ה-retry |
| `RETRY_DB` | fallback לאחור אם `STATE_DB` לא הוגדר |
| `EXPOSE_DEMO_PII` | ברירת מחדל false; הצגת שמות אמיתיים בדמו פרטי בלבד |

Scopes נדרשים ב-HubSpot:

- `crm.objects.contacts.read`
- `crm.objects.contacts.write`
- `crm.schemas.contacts.write`
- `crm.objects.notes.write`

## גבולות העמידות

SQLite שורד restart כל עוד אותו דיסק נשמר. הוא אינו שורד החלפת container
ב-Railway או Render ללא volume מתמיד. המימוש מניח instance/worker יחיד; לפני
production מרובה replicas צריך Postgres או queue מנוהל.

המערכת מספקת at-least-once processing. submission ID מונע retry רגיל של
הדפדפן, אבל crash בדיוק אחרי יצירת note ב-HubSpot ולפני סימון `completed`
עדיין עלול ליצור note כפול לאחר restart. מזהה הקליטה נכתב ב-note כדי לאפשר
איתור, אך exactly-once מול API חיצוני דורש idempotency גם בצד ה-CRM.

## CI ופריסה ל-Railway

ה-workflow ב-`.github/workflows/ci.yml` מריץ compile ואת כל הבדיקות בכל push
ובכל pull request. אין בו secrets, והוא לא פונה ל-Gemini או ל-HubSpot.

`railway.json` מגדיר Railpack, פקודת start, בדיקת `/health` ו-restart במקרה
של קריסה. כדי שה-durable intake יהיה באמת עמיד גם לאחר deploy או restart:

הדמו הנוכחי פרוס כ-service יחיד ב-Railway, עם volume שמחובר ל-`/data`
ו-`STATE_DB=/data/leadflow_queue.db`.

1. מחברים את ה-repository ל-Railway כשירות יחיד ועם replica יחיד.
2. מצרפים volume ומגדירים לו mount path של `/data`.
3. מגדירים `STATE_DB=/data/leadflow_queue.db`.
4. מעתיקים ל-Variables את `AI_PROVIDER`, מפתח ה-AI ו-`HUBSPOT_TOKEN` מתוך
   `.env` המקומי. אין להעלות את `.env` ל-Git.
5. יוצרים public domain ובודקים `/health`, את הטופס ואת הרשומה ב-HubSpot.

בלי volume, SQLite נשאר durable רק בתוך אותו container ולא עומד בהבטחה לאחר
redeploy. בגלל SQLite וה-worker הפנימי, אין להגדיל כרגע ליותר מ-replica אחד.

## מה נשאר עד production

1. להגדיר אירוע response אמיתי ושעון נפרד לכל inquiry.
2. למפות נציגים ל-owner IDs אמיתיים ב-CRM.
3. לאמת WhatsApp אמיתי מקצה לקצה ולחבר ערוץ אימייל.
4. לשלוח התראות ודוח לאנשים בפועל, לא רק להציג אותם במסך.
5. להריץ shadow mode ולכייל את סף הביטחון על פניות היסטוריות אמיתיות.
6. להגדיר בעלים תפעולי, הרשאות, retention ונוהל טיפול ב-dead letters.
