#!/usr/bin/env python3
import os
import re
import time
import random
import requests
import argparse
from datetime import datetime, time as dt_time, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
RETELL_API_KEY = os.getenv("RETELL_API_KEY")
VOICE_AI_WEBHOOK_URL = os.getenv("VOICE_AI_WEBHOOK_URL")

def check_env():
    """Ensures all critical environment variables are set."""
    missing = []
    if not SUPABASE_URL: missing.append("SUPABASE_URL")
    if not SUPABASE_KEY: missing.append("SUPABASE_SERVICE_ROLE_KEY")
    if not RETELL_API_KEY: missing.append("RETELL_API_KEY")
    if not VOICE_AI_WEBHOOK_URL: missing.append("VOICE_AI_WEBHOOK_URL")
    
    if missing:
        print(f"❌ CRITICAL ERROR: Missing environment variables: {', '.join(missing)}")
        print("   Please set these in your Railway dashboard or .env file.")
        return False
    
    if not SUPABASE_URL.startswith("http"):
        print(f"❌ CRITICAL ERROR: Invalid SUPABASE_URL format: '{SUPABASE_URL}'")
        print("   It must start with http:// or https://")
        return False
        
    return True

LOCK_FILE = "/tmp/voicebot.lock"

def cleanup():
    if os.path.exists(LOCK_FILE):
        os.remove(LOCK_FILE)

def is_call_ongoing(call_id: str) -> bool:
    """Checks if a Retell call is still active/ongoing."""
    if not RETELL_API_KEY or not call_id:
        return False
    
    url = f"https://api.retellai.com/v2/get-call/{call_id}"
    headers = {"Authorization": f"Bearer {RETELL_API_KEY}"}
    
    try:
        res = requests.get(url, headers=headers)
        if res.ok:
            data = res.json()
            return data.get("call_status") in ["ringing", "in_progress", "registered"]
    except:
        pass
    return False

# EST Area Codes Mapping (High population states)
EST_AREA_CODES = {
    # New York
    '212', '315', '332', '347', '516', '518', '585', '607', '631', '646', '716', '718', '845', '914', '917', '929',
    # Florida
    '239', '305', '321', '352', '386', '407', '561', '727', '754', '772', '786', '813', '850', '863', '904', '941', '954',
    # Pennsylvania
    '215', '267', '412', '484', '570', '610', '717', '724', '814', '878',
    # Massachusetts
    '339', '351', '413', '508', '617', '774', '781', '857', '978',
    # Ohio (EST)
    '216', '234', '330', '419', '440', '513', '567', '614', '740', '937',
    # Georgia
    '229', '404', '470', '478', '678', '706', '770', '912',
    # North Carolina
    '252', '336', '704', '828', '910', '919', '980'
}

def is_est_business_hours() -> bool:
    """Checks if the current time is between 8:00 AM and 5:00 PM EST (Monday-Friday)."""
    # Current UTC time
    now_utc = datetime.now(timezone.utc)
    
    # Quick approximation of EST (UTC-5)
    est_offset = timedelta(hours=-5)
    now_est = now_utc + est_offset
    
    # 0 = Monday, 6 = Sunday
    if now_est.weekday() > 4:
        return False
        
    current_time = now_est.time()
    start_time = dt_time(8, 0)
    end_time = dt_time(17, 0)
    
    return start_time <= current_time <= end_time

def extract_area_code(contact_info: str) -> str:
    if not contact_info:
        return None
    digits = re.sub(r"\D", "", contact_info)
    if len(digits) == 11 and digits[0] == "1":
        digits = digits[1:]
    if len(digits) >= 10:
        return digits[:3]
    return None

def fetch_uncalled_leads(limit=20):
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}"
    }
    # Only fetch leads that haven't been called AND aren't currently being processed by another bot
    url = f"{SUPABASE_URL}/rest/v1/leads?is_called=is.false&processing=is.false&contact_info=not.is.null&limit={limit}"
    res = requests.get(url, headers=headers)
    if res.ok:
        return res.json()
    print(f"❌ Error fetching leads: {res.text}")
    return []

def mark_lead_processing(lead_id: str, status: bool = True):
    """Locks/Unlocks a lead in Supabase to prevent multiple bots from calling the same person."""
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json"
    }
    url = f"{SUPABASE_URL}/rest/v1/leads?id=eq.{lead_id}"
    requests.patch(url, headers=headers, json={"processing": status})

def mark_lead_as_called(lead_id: str):
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json"
    }
    url = f"{SUPABASE_URL}/rest/v1/leads?id=eq.{lead_id}"
    # Set is_called to True and clear processing lock
    res = requests.patch(url, headers=headers, json={"is_called": True, "processing": False})
    if not res.ok:
        print(f"⚠️ Failed to mark lead {lead_id} as called: {res.text}")

def trigger_outbound_call(lead: dict, max_retries: int = 3) -> str:
    """Sends the lead data to Make.com or the Voice AI platform to initiate the call with retries for queue issues."""
    now_est = datetime.now(timezone.utc) + timedelta(hours=-5)
    current_date_str = now_est.strftime("%Y-%m-%d %I:%M %p EST")
    
    payload = {
        "lead_id": lead.get("id"),
        "business_name": lead.get("company_name"),
        "lead_url": lead.get("website_url"),
        "contact_info": lead.get("contact_info"),
        "ux_score": lead.get("health_score"),
        "lead_timezone": "EST",
        "current_date": current_date_str
    }
    
    retry_count = 0
    base_delay = 5 # Start with 5s delay on first queue-full error
    
    while retry_count <= max_retries:
        print(f"📞 Dispatching Call to: {lead.get('company_name')} [{lead.get('contact_info')}] (Attempt {retry_count + 1})")
        try:
            res = requests.post(VOICE_AI_WEBHOOK_URL, json=payload)
            if res.ok:
                print("✅ Call initiated successfully.")
                try:
                    return res.json().get("call_id")
                except:
                    return "queued_via_make"
            
            # Handle "Queue is full" specifically
            if res.status_code == 400 and "Queue is full" in res.text:
                if retry_count < max_retries:
                    wait_time = base_delay * (2 ** retry_count) + random.uniform(0, 5)
                    print(f"⚠️ Queue full. Retrying in {wait_time:.1f}s... (Attempt {retry_count + 1}/{max_retries})")
                    time.sleep(wait_time)
                    retry_count += 1
                    continue
                else:
                    print(f"❌ Failed to initiate call after {max_retries} retries: Queue remains full. Skipping lead.")
                    break
            else:
                print(f"❌ Failed to initiate call: {res.status_code} - {res.text}")
                break # Don't retry other errors
                
        except Exception as e:
            print(f"❌ Error triggering call: {e}")
            break
            
    return None

def main():
    if not check_env():
        return

    parser = argparse.ArgumentParser(description="EST Voice Caller Bot")
    parser.add_argument("--dry-run", action="store_true", help="Print which EST leads would be called without actually calling them.")
    parser.add_argument("--force", action="store_true", help="Bypass the 8-5 EST check (for testing purposes).")
    args = parser.parse_args()

    if not args.force and not is_est_business_hours():
        print("⏸️  Bot Paused: Current time is outside the 8 AM - 5 PM EST Mon-Fri window.")
        return

    # --- SINGLE INSTANCE LOCK ---
    if os.path.exists(LOCK_FILE):
        # Check if the process is actually running (stale lock check)
        print("⚠️  Another instance of the bot is already running or a stale lock exists.")
        print("   If you are SURE no other bot is running, delete /tmp/voicebot.lock")
        return
    
    with open(LOCK_FILE, "w") as f:
        f.write(str(os.getpid()))
    
    try:
        if not RETELL_API_KEY:
            print("⚠️  WARNING: RETELL_API_KEY is not set. Concurrency limits will NOT be enforced.")
            print("   Please check your .env file in the 'Ai Receptionsts' directory.")

        # Staggered startup to prevent race conditions if multiple bots start at once
        startup_stagger = random.uniform(2, 15)
        if not args.dry_run:
            print(f"⏳ Staggering startup for {startup_stagger:.1f}s to prevent duplicate runs...")
            time.sleep(startup_stagger)

        print("🚀 Running Voice Agent Dispatcher for EST Leads...")
        leads = fetch_uncalled_leads(limit=1200)
        
        est_leads = []
        for lead in leads:
            ac = extract_area_code(lead.get("contact_info", ""))
            if ac in EST_AREA_CODES:
                est_leads.append(lead)
                
        if not est_leads:
            print("📭 No uncalled EST leads found in this batch.")
            return
            
        print(f"🎯 Found {len(est_leads)} eligible EST leads to call.")
        
        active_calls = []
        concurrency_limit = 10
        
        for lead in est_leads:
            # Check if another process snatched this lead while we were staggering
            # (Technically should re-fetch from Supabase, but marking as processing first is safer)
            
            # CONCURRENCY GUARD: If we have reached the limit, wait for a line to open
            if RETELL_API_KEY:
                while len(active_calls) >= concurrency_limit:
                    # Filter out calls that have ended
                    active_calls = [cid for cid in active_calls if cid and is_call_ongoing(cid) and cid != "queued_via_make"]
                    
                    if len(active_calls) >= concurrency_limit:
                        print(f"⏳ All {concurrency_limit} lines busy. Waiting 15s...")
                        time.sleep(15)
            
            if args.dry_run:
                print(f"  [DRY RUN] Would call {lead.get('company_name')} at {lead.get('contact_info')}")
            else:
                mark_lead_processing(lead.get("id"), True)
                
                call_id = trigger_outbound_call(lead)
                if call_id:
                    if call_id != "queued_via_make":
                        active_calls.append(call_id)
                    mark_lead_as_called(lead.get("id"))
                else:
                    mark_lead_processing(lead.get("id"), False)
                
                pacing_delay = random.uniform(10, 20)
                time.sleep(pacing_delay)

    except Exception as e:
        print(f"💥 Fatal error in main loop: {e}")
    finally:
        cleanup()

if __name__ == "__main__":
    main()
