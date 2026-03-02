#!/usr/bin/env python3
import os
import re
import requests
import argparse
from datetime import datetime, time, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

# Replace this with the URL to trigger the outbound call
# E.g., a Make.com Webhook URL or the Bland/Retell/Vapi API endpoint
VOICE_AI_WEBHOOK_URL = os.getenv("VOICE_AI_WEBHOOK_URL", "https://hook.us2.make.com/2ktodpcug2qjy8tcrer2lcyolnjjo6rs")

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
    start_time = time(8, 0)
    end_time = time(17, 0)
    
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
    # Only fetch leads that haven't been called yet.
    url = f"{SUPABASE_URL}/rest/v1/leads?is_called=is.false&contact_info=not.is.null&limit={limit}"
    res = requests.get(url, headers=headers)
    if res.ok:
        return res.json()
    print(f"❌ Error fetching leads: {res.text}")
    return []

def mark_lead_as_called(lead_id: str):
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json"
    }
    url = f"{SUPABASE_URL}/rest/v1/leads?id=eq.{lead_id}"
    res = requests.patch(url, headers=headers, json={"is_called": True})
    if not res.ok:
        print(f"⚠️ Failed to mark lead {lead_id} as called: {res.text}")

def trigger_outbound_call(lead: dict):
    """Sends the lead data to Make.com or the Voice AI platform to initiate the call."""
    now_est = datetime.now(timezone.utc) + timedelta(hours=-5)
    # Use a more explicit ISO-like format for easier parsing by downstream tools
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
    
    # Uncomment and replace with actual API request to the dialer
    print(f"📞 Dispatching Call to: {lead.get('company_name')} [{lead.get('contact_info')}]")
    try:
        res = requests.post(VOICE_AI_WEBHOOK_URL, json=payload)
        if res.ok:
            print("✅ Call initiated successfully.")
            mark_lead_as_called(lead.get("id"))
        else:
            print(f"❌ Failed to initiate call: {res.status_code} - {res.text}")
    except Exception as e:
        print(f"❌ Error triggering call: {e}")

def main():
    parser = argparse.ArgumentParser(description="EST Voice Caller Bot")
    parser.add_argument("--dry-run", action="store_true", help="Print which EST leads would be called without actually calling them.")
    parser.add_argument("--force", action="store_true", help="Bypass the 8-5 EST check (for testing purposes).")
    args = parser.parse_args()

    if not args.force and not is_est_business_hours():
        print("⏸️  Bot Paused: Current time is outside the 8 AM - 5 PM EST Mon-Fri window.")
        return

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
    
    for lead in est_leads:
        if args.dry_run:
            print(f"  [DRY RUN] Would call {lead.get('company_name')} at {lead.get('contact_info')}")
        else:
            trigger_outbound_call(lead)
            # Add a delay between calls to prevent "Queue is full" errors in Make.com
            time.sleep(3)

if __name__ == "__main__":
    main()
