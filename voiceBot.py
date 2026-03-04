#!/usr/bin/env python3
import os
import re
import time
import random
import requests
import argparse
from datetime import datetime, time as dt_time, timedelta, timezone
from typing import Optional, Union, List, Dict, Any
from dotenv import load_dotenv
from retell import Retell

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
RETELL_API_KEY = os.getenv("RETELL_API_KEY")
RETELL_AGENT_ID = os.getenv("RETELL_AGENT_ID")
FROM_NUMBER = os.getenv("FROM_NUMBER")
VOICE_AI_WEBHOOK_URL = os.getenv("VOICE_AI_WEBHOOK_URL")

# Initialize Retell Client
retell_client: Optional[Retell] = Retell(api_key=RETELL_API_KEY) if RETELL_API_KEY else None

def check_env() -> bool:
    """Ensures all critical environment variables are set."""
    missing = []
    if not SUPABASE_URL: missing.append("SUPABASE_URL")
    if not SUPABASE_KEY: missing.append("SUPABASE_SERVICE_ROLE_KEY")
    if not RETELL_API_KEY: missing.append("RETELL_API_KEY")
    
    # Batch calls need these. Webhook fallback is only if these are missing.
    if not RETELL_AGENT_ID or not FROM_NUMBER:
        print("⚠️  NOTICE: RETELL_AGENT_ID and FROM_NUMBER are not set. Falling back to Webhook.")
        if not VOICE_AI_WEBHOOK_URL: missing.append("VOICE_AI_WEBHOOK_URL")
    
    if missing:
        print(f"❌ CRITICAL ERROR: Missing environment variables: {', '.join(missing)}")
        return False
    return True

LOCK_FILE = "/tmp/voicebot.lock"

def cleanup():
    if os.path.exists(LOCK_FILE):
        os.remove(LOCK_FILE)

def is_call_ongoing(call_id: str) -> bool:
    """Checks if a Retell call is active."""
    if not retell_client or not call_id:
        return False
    try:
        call = retell_client.call.retrieve(call_id)
        return call.call_status in ["ringing", "in_progress", "registered"]
    except:
        return False

EST_AREA_CODES = {
    '212', '315', '332', '347', '516', '518', '585', '607', '631', '646', '716', '718', '845', '914', '917', '929',
    '239', '305', '321', '352', '386', '407', '561', '727', '754', '772', '786', '813', '850', '863', '904', '941', '954',
    '215', '267', '412', '484', '570', '610', '717', '724', '814', '878',
    '339', '351', '413', '508', '617', '774', '781', '857', '978',
    '216', '234', '330', '419', '440', '513', '567', '614', '740', '937',
    '229', '404', '470', '478', '678', '706', '770', '912',
    '252', '336', '704', '828', '910', '919', '980'
}

def is_est_business_hours() -> bool:
    now_est = datetime.now(timezone.utc) + timedelta(hours=-5)
    if now_est.weekday() > 4: return False
    return dt_time(8, 0) <= now_est.time() <= dt_time(17, 0)

def extract_area_code(contact_info: Optional[str]) -> Optional[str]:
    if not contact_info: return None
    digits = re.sub(r"\D", "", contact_info)
    if len(digits) == 11 and digits[0] == "1": digits = digits[1:]
    return str(digits[:3]) if len(digits) >= 10 else None

def fetch_uncalled_leads(limit: int = 100) -> List[Dict[str, Any]]:
    if not SUPABASE_KEY or not SUPABASE_URL: return []
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    url = f"{SUPABASE_URL}/rest/v1/leads?is_called=is.false&processing=is.false&contact_info=not.is.null&limit={limit}"
    res = requests.get(url, headers=headers)
    return res.json() if res.ok else []

def mark_lead_processing(lead_id: str, status: bool = True):
    if not SUPABASE_KEY or not SUPABASE_URL: return
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}", "Content-Type": "application/json"}
    url = f"{SUPABASE_URL}/rest/v1/leads?id=eq.{lead_id}"
    requests.patch(url, headers=headers, json={"processing": status})

def mark_lead_as_called(lead_id: str):
    if not SUPABASE_KEY or not SUPABASE_URL: return
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}", "Content-Type": "application/json"}
    url = f"{SUPABASE_URL}/rest/v1/leads?id=eq.{lead_id}"
    requests.patch(url, headers=headers, json={"is_called": True, "processing": False})

def trigger_batch_calls(leads: List[Dict[str, Any]]) -> Union[bool, str]:
    if not RETELL_AGENT_ID or not FROM_NUMBER or not retell_client: return False
    now_est = datetime.now(timezone.utc) + timedelta(hours=-5)
    date_str = now_est.strftime("%Y-%m-%d %I:%M %p EST")
    tasks = [{
        "to_number": l.get("contact_info"),
        "retell_llm_dynamic_variables": {
            "business_name": l.get("company_name"),
            "lead_url": l.get("website_url"),
            "current_date": date_str,
            "lead_timezone": "EST"
        }
    } for l in leads]
    try:
        retell_client.batch_call.create_batch_call(
            name=f"Aria EST - {now_est.strftime('%Y-%m-%d %H:%M')}",
            from_number=FROM_NUMBER, agent_id=RETELL_AGENT_ID, tasks=tasks
        )
        return True
    except Exception as e:
        if "Queue is full" in str(e): return "queue_full"
        return False

def trigger_webhook_call(lead: Dict[str, Any], max_retries: int = 3) -> Optional[str]:
    if not VOICE_AI_WEBHOOK_URL: return None
    date_str = (datetime.now(timezone.utc) + timedelta(hours=-5)).strftime("%Y-%m-%d %I:%M %p EST")
    payload = {
        "lead_id": lead.get("id"), "business_name": lead.get("company_name"),
        "lead_url": lead.get("website_url"), "contact_info": lead.get("contact_info"),
        "ux_score": lead.get("health_score"), "lead_timezone": "EST", "current_date": date_str
    }
    for r in range(max_retries + 1):
        try:
            res = requests.post(VOICE_AI_WEBHOOK_URL, json=payload)
            if res.ok: return str(res.json().get("call_id") or "queued")
            if res.status_code == 400 and "Queue is full" in res.text:
                if r < max_retries:
                    time.sleep(5 * (2 ** r) + random.uniform(0, 5))
                    continue
                return "queue_full"
            break
        except: break
    return None

def main():
    if not check_env(): return
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    if not args.force and not is_est_business_hours():
        print("⏸️  Paused (Outside 8-5 EST)")
        return

    if os.path.exists(LOCK_FILE): return
    with open(LOCK_FILE, "w") as f: f.write(str(os.getpid()))
    
    try:
        if not args.dry_run: time.sleep(random.uniform(2, 5))
        leads = fetch_uncalled_leads(100)
        est_leads = [l for l in leads if extract_area_code(l.get("contact_info", "")) in EST_AREA_CODES]
        if not est_leads: return
        
        batch_size = 10
        use_native = bool(RETELL_AGENT_ID and FROM_NUMBER and retell_client)
        
        for i in range(0, len(est_leads), batch_size):
            chunk = est_leads[i : i + batch_size]
            if args.dry_run: continue

            if use_native:
                for l in chunk: mark_lead_processing(l.get("id"), True)
                res = trigger_batch_calls(chunk)
                if res == "queue_full":
                    for l in chunk: mark_lead_processing(l.get("id"), False)
                    print("🛑 Cool-down (10m)...")
                    time.sleep(600); continue
                if res:
                    for l in chunk: mark_lead_as_called(l.get("id"))
                else:
                    for l in chunk: mark_lead_processing(l.get("id"), False)
                time.sleep(random.uniform(30, 60))
            else:
                for l in chunk:
                    mark_lead_processing(l.get("id"), True)
                    status = trigger_webhook_call(l)
                    if status == "queue_full":
                        mark_lead_processing(l.get("id"), False)
                        print("🛑 Cool-down (5m)...")
                        time.sleep(300); break
                    if status: mark_lead_as_called(l.get("id"))
                    else: mark_lead_processing(l.get("id"), False)
                    time.sleep(random.uniform(10, 20))
    finally: cleanup()

if __name__ == "__main__":
    main()
