#!/usr/bin/env python3
"""Measure how good the classifier actually is.

Runs a fixed set of realistic Hebrew inquiries through classify_inquiry and
prints expected vs actual with the confidence score, then an accuracy summary.

The test set is deliberately NOT rigged to score 100%. Four cases are
genuinely ambiguous - a one-word "היי", a bare "כמה?" - because a classifier
that scores perfectly on a test set someone wrote to flatter it has measured
nothing. The interesting number is not the accuracy, it is whether the cases
we get wrong are the cases we already flagged as low confidence. That is the
difference between a system that is wrong and a system that is wrong quietly.

Run:
    python eval.py             # real Claude calls, needs ANTHROPIC_API_KEY
    python eval.py --dry-run   # offline, fake classifier, proves the harness
"""

import argparse
import sys

import config  # noqa: F401  loads .env before anything reads the API key
from classify import classify_inquiry
# Imported, not re-declared: the eval has to judge against the same threshold
# production uses. Tune CONFIDENCE_THRESHOLD and this report moves with it.
from pipeline import CONFIDENCE_THRESHOLD

VALID_CATEGORIES = {"new_lead", "existing_customer", "support", "spam", "other"}


# --------------------------------------------------------------- the test set
# `ambiguous` marks cases where a careful human could reasonably disagree with
# the expected label. They are scored separately so one number cannot hide
# behind the other.

CASES = [
    # ---- clear new leads ----
    {
        "text": "שלום, ראיתי את האתר שלכם ואשמח לקבל הצעת מחיר להתקנת מזגן "
                "בדירת 4 חדרים ברמת גן. אפשר לחזור אליי? ישראל, 050-1234567",
        "expected": "new_lead",
        "note": "פנייה מלאה עם שם, טלפון וכוונת קנייה",
    },
    {
        "text": "היי! כמה עולה שירות ניקיון משרדים פעמיים בשבוע? המשרד שלנו 120 מ\"ר",
        "expected": "new_lead",
        "note": "שאלת מחיר עם פרטים מספיקים",
    },
    {
        "text": "מעוניין בהצעת מחיר לצילום אירוע חתונה באוגוסט הקרוב, בערך 250 אורחים",
        "expected": "new_lead",
        "note": "כוונת קנייה ברורה עם תאריך והיקף",
    },
    {
        "text": "שלום, קיבלתי המלצה עליכם מחבר. אני רוצה לשמוע על השירותים והמחירים שלכם",
        "expected": "new_lead",
        "note": "ליד חדש בלי מוצר ספציפי",
    },

    # ---- existing customer chasing something he never got ----
    {
        "text": "שלום, דיברתי עם אחד הנציגים שלכם לפני שבועיים והבטיחו לשלוח לי הצעת "
                "מחיר. עדיין לא קיבלתי כלום. אשמח שמישהו יחזור אליי, אני צריך להחליט "
                "עד סוף השבוע",
        "expected": "existing_customer",
        "note": "המקרה שהלקוח שילם עליו: מחכה להצעה שלא הגיעה",
    },
    {
        "text": "היי, אני לקוח שלכם כבר שנתיים. ביקשתי הצעה להרחבת השירות ואף אחד לא "
                "חוזר אליי. זו כבר הפעם השלישית שאני פונה",
        "expected": "existing_customer",
        "note": "לקוח קיים, פנייה חוזרת, אמור לצאת בדחיפות גבוהה",
    },
    {
        "text": "מה קורה עם ההצעה ששלחתם לי? יש לי שאלה על סעיף 3 במחיר",
        "expected": "existing_customer",
        "note": "קצר אבל מפנה להצעה קיימת",
    },

    # ---- support ----
    {
        "text": "המערכת שהתקנתם אצלנו לא עובדת מאתמול, יש נורה אדומה שמהבהבת. דחוף, "
                "אנחנו לא יכולים לעבוד ככה",
        "expected": "support",
        "note": "תקלה אצל לקוח קיים, דחיפות גבוהה",
    },
    {
        "text": "היי, אפשר לקבל חשבונית מס על התשלום מחודש שעבר? צריך את זה להנהלת חשבונות",
        "expected": "support",
        "note": "בקשה אדמיניסטרטיבית, לא הזדמנות מכירה",
    },
    {
        "text": "רציתי לבטל את התור שקבעתי ליום שלישי ולתאם מחדש לשבוע הבא",
        "expected": "support",
        "note": "שינוי תיאום, קל לבלבל עם לקוח קיים",
    },

    # ---- spam ----
    {
        "text": "🔥🔥 הזדמנות אחרונה! קידום אתרים בגוגל למקום הראשון מובטח! "
                "לחצו כאן ➡️ bit.ly/promo2024 להסרה השב הסר",
        "expected": "spam",
        "note": "פרסומת אוטומטית קלאסית",
    },
    {
        "text": "שלום, אנחנו חברה להלוואות חוץ בנקאיות. אישור תוך 24 שעות ללא בטחונות. "
                "מעוניין? השב כן",
        "expected": "spam",
        "note": "פנייה יזומה מסחרית, לא לקוח",
    },

    # ---- deliberately ambiguous / very short: the honest failure modes ----
    {
        "text": "היי",
        "expected": "other",
        "ambiguous": True,
        "note": "אין שום מידע. כל סיווג בביטחון גבוה כאן הוא ניחוש",
    },
    {
        "text": "כמה?",
        "expected": "new_lead",
        "ambiguous": True,
        "note": "כנראה שאלת מחיר, אבל בלי הקשר גם לקוח קיים יכול לשאול",
    },
    {
        "text": "אפשר לדבר עם מישהו?",
        "expected": "other",
        "ambiguous": True,
        "note": "יכול להיות מכירה, תמיכה או תלונה. בכוונה לא ניתן להכרעה",
    },
    {
        "text": "שלחתי הודעה קודם",
        "expected": "existing_customer",
        "ambiguous": True,
        "note": "רומז על פנייה קודמת אבל לא אומר על מה",
    },
]


# ------------------------------------------------------------- offline mode
# A crude keyword classifier used only by --dry-run. It exists so the harness,
# the table and the arithmetic can be exercised with no API key and no cost.
# It is deliberately mediocre - if the fake scored 100% the summary code path
# that reports misses would never run, and a bug there would hide forever.

def _fake_classify(text: str) -> dict:
    t = text.strip()
    if any(w in t for w in ("bit.ly", "הלוואות", "להסרה", "קידום אתרים")):
        cat, conf = "spam", 95
    elif any(w in t for w in ("לא עובדת", "תקלה", "חשבונית", "לבטל את התור")):
        cat, conf = "support", 84
    elif any(w in t for w in ("הבטיחו", "לקוח שלכם", "ההצעה ששלחתם")):
        cat, conf = "existing_customer", 88
    elif len(t) < 25:
        cat, conf = "other", 30          # short text -> honest uncertainty
    else:
        cat, conf = "new_lead", 79
    return {"category": cat, "urgency": "normal", "name": None,
            "summary": "(dry-run)", "confidence": conf, "error": None}


# ------------------------------------------------------------------- report

def _shorten(text: str, width: int = 60) -> str:
    one_line = " ".join(text.split())
    return one_line if len(one_line) <= width else one_line[:width - 1] + "…"


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate the LeadFlow classifier.")
    parser.add_argument("--dry-run", action="store_true",
                        help="use a fake offline classifier (no API key, no cost)")
    args = parser.parse_args()

    classifier = _fake_classify if args.dry_run else classify_inquiry

    print()
    if args.dry_run:
        print("=" * 78)
        print("  DRY RUN - fake classifier. These numbers measure NOTHING about the AI.")
        print("  They only prove the harness runs. Drop --dry-run for real results.")
        print("=" * 78)
    print(f"LeadFlow classifier eval | {len(CASES)} cases | "
          f"confidence threshold = {CONFIDENCE_THRESHOLD}")
    print()

    header = f"{'#':>2}  {'expected':<18} {'actual':<18} {'conf':>4}  {'hit':<4} message"
    print(header)
    print("-" * len(header))

    rows, errors = [], []
    for i, case in enumerate(CASES, 1):
        verdict = classifier(case["text"])
        actual = verdict.get("category", "?")
        conf = verdict.get("confidence", 0)
        hit = actual == case["expected"]
        low = conf < CONFIDENCE_THRESHOLD
        if verdict.get("error"):
            errors.append((i, verdict["error"]))

        rows.append({**case, "n": i, "actual": actual, "conf": conf, "hit": hit,
                     "low": low, "ambiguous": case.get("ambiguous", False)})

        flag = "OK  " if hit else "MISS"
        marker = "*" if low else " "   # * = below threshold -> goes to human review
        print(f"{i:>2}  {case['expected']:<18} {actual:<18} {conf:>4}{marker} {flag} "
              f"{_shorten(case['text'])}")

    print()
    print("  * = below the confidence threshold, so the pipeline routes it to human review")
    print()

    # --- if the classifier never actually ran, say so instead of scoring it ---
    if len(errors) == len(CASES):
        print("!! EVERY call failed - the classifier never ran. This is not a 0% score,")
        print("!! it is a broken run. First error:")
        print(f"!!   {errors[0][1]}")
        print("!! Check ANTHROPIC_API_KEY, or use --dry-run to test the harness offline.")
        print()
        return 2

    clear = [r for r in rows if not r["ambiguous"]]
    ambig = [r for r in rows if r["ambiguous"]]
    hits = [r for r in rows if r["hit"]]
    low_conf = [r for r in rows if r["low"]]
    misses = [r for r in rows if not r["hit"]]
    # The number that actually matters: wrong AND confident. Nobody catches these.
    silent = [r for r in misses if not r["low"]]

    def pct(part, whole):
        return f"{100 * len(part) / len(whole):.0f}%" if whole else "n/a"

    print("SUMMARY")
    print(f"  overall accuracy       {len(hits)}/{len(rows)}  ({pct(hits, rows)})")
    print(f"  clear cases            {len([r for r in clear if r['hit']])}/{len(clear)}"
          f"  ({pct([r for r in clear if r['hit']], clear)})")
    print(f"  ambiguous cases        {len([r for r in ambig if r['hit']])}/{len(ambig)}"
          f"  ({pct([r for r in ambig if r['hit']], ambig)})")
    print(f"  below threshold (<{CONFIDENCE_THRESHOLD})   {len(low_conf)}/{len(rows)}"
          f"  -> sent to human review")
    print(f"  wrong AND confident    {len(silent)}   <- the only truly dangerous number")
    if errors:
        print(f"  failed API calls       {len(errors)}  (counted as misses above)")
    print()

    if misses:
        print("MISSES")
        for r in misses:
            tag = "ambiguous" if r["ambiguous"] else "clear"
            caught = "caught by threshold" if r["low"] else "NOT caught - looked confident"
            print(f"  #{r['n']}  [{tag}] expected {r['expected']}, got {r['actual']} "
                  f"@ {r['conf']} - {caught}")
            print(f"      {_shorten(r['text'], 68)}")
            print(f"      why it's here: {r['note']}")
        print()

    print("How to read this: a miss that was already below the threshold is a system")
    print("working as designed - it admitted it didn't know and asked for a human.")
    print("A miss above the threshold is the real bug. That is the number to drive down.")
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
