"""
🏋️ Gym Data Collector – Direct Supabase Push
Holt Auslastungsdaten aller Studios und schreibt sie DIREKT in Supabase.
Kein CSV-Umweg nötig!

Läuft automatisch via GitHub Actions (täglich 23:30) oder manuell in Colab.
"""

import os
import requests
import json
import time
from datetime import datetime

# ============================================================
# SUPABASE KONFIGURATION
# ============================================================

SUPABASE_URL = "https://lubjhjoetscgpsckckjs.supabase.co"
SUPABASE_KEY = os.environ.get(
    "SUPABASE_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imx1Ympoam9ldHNjZ3BzY2tja2pzIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzA5MDY0MDYsImV4cCI6MjA4NjQ4MjQwNn0.VGBNcCZS8inFImzuc_-DWpzRFI7rPY4oPCWvhrGafoU",
)

DEVICE_HASH = "admin"

# ============================================================
# HELPER
# ============================================================

WEEKDAYS_MAP = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"]


def get_weekday():
    return WEEKDAYS_MAP[int(datetime.now().strftime("%w"))]


def get_date():
    return datetime.now().strftime("%Y-%m-%d")


def parse_hour(time_val):
    if isinstance(time_val, str):
        return int(time_val.split(":")[0])
    elif isinstance(time_val, dict):
        return time_val.get("hour", 0)
    return 0


# ============================================================
# SUPABASE PUSH
# ============================================================


def supabase_push_observations(observations):
    """Pusht via push_observations RPC-Funktion (wie die iOS-App)."""
    if not observations:
        return 0

    url = f"{SUPABASE_URL}/rest/v1/rpc/push_observations"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }

    BATCH_SIZE = 500
    inserted = 0

    for i in range(0, len(observations), BATCH_SIZE):
        batch = observations[i : i + BATCH_SIZE]
        payload = {"p_data": batch}

        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=15)
            if resp.status_code in range(200, 300):
                inserted += len(batch)
                print(f"  ✅ Batch {i // BATCH_SIZE + 1}: {len(batch)} Datenpunkte gepusht")
            else:
                print(f"  ❌ Batch {i // BATCH_SIZE + 1}: HTTP {resp.status_code} – {resp.text[:200]}")
        except Exception as e:
            print(f"  ❌ Batch {i // BATCH_SIZE + 1}: {e}")

        if i + BATCH_SIZE < len(observations):
            time.sleep(0.3)

    return inserted


def supabase_push_direct(observations):
    """Fallback: Direkter Insert in utilization_observations."""
    if not observations:
        return 0

    url = f"{SUPABASE_URL}/rest/v1/utilization_observations"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }

    BATCH_SIZE = 500
    inserted = 0

    for i in range(0, len(observations), BATCH_SIZE):
        batch = observations[i : i + BATCH_SIZE]

        try:
            resp = requests.post(url, headers=headers, json=batch, timeout=15)
            if resp.status_code in range(200, 300):
                inserted += len(batch)
                print(f"  ✅ Direct-Insert Batch {i // BATCH_SIZE + 1}: {len(batch)} Zeilen")
            else:
                print(f"  ❌ Direct-Insert HTTP {resp.status_code} – {resp.text[:200]}")
        except Exception as e:
            print(f"  ❌ Direct-Insert Fehler: {e}")

        if i + BATCH_SIZE < len(observations):
            time.sleep(0.3)

    return inserted


def supabase_refresh_view():
    """Materialized View aktualisieren."""
    url = f"{SUPABASE_URL}/rest/v1/rpc/refresh_global_averages"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }
    try:
        resp = requests.post(url, headers=headers, json={}, timeout=30)
        if resp.status_code in range(200, 300):
            print("✅ Materialized View 'global_averages' aktualisiert!")
            return True
        else:
            print(f"⚠️  View-Refresh HTTP {resp.status_code} – {resp.text[:200]}")
            return False
    except Exception as e:
        print(f"⚠️  View-Refresh Fehler: {e}")
        return False


# ============================================================
# STUDIOS LADEN
# ============================================================

print("=" * 55)
print("🏋️  GYM DATA COLLECTOR – Direct Supabase Push")
print("=" * 55)
print()
print("📡 Studios laden...\n")

# --- Magicline (McFit, John Reed, ZEAL, etc.) ---
print("  Magicline Studios...")
try:
    resp = requests.get(
        "https://rsg-group.api.magicline.com/connect/v1/studio", timeout=15
    )
    resp.raise_for_status()
    magicline_raw = resp.json()
    magicline_studios = [
        {"id": s["id"], "name": s.get("studioName", "?"), "source": "magicline"}
        for s in magicline_raw
        if s.get("id")
    ]
    print(f"  ✅ {len(magicline_studios)} Magicline Studios")
except Exception as e:
    magicline_studios = []
    print(f"  ❌ Magicline Fehler: {e}")

# --- FitX ---
print("  FitX Studios...")
try:
    headers_fitx = {"x-tenant": "fitx", "accept": "application/json"}
    resp = requests.get(
        "https://mein.fitx.de/nox/public/v1/studios",
        headers=headers_fitx,
        timeout=15,
    )
    resp.raise_for_status()
    fitx_raw = resp.json()
    fitx_studios = [
        {"id": s["id"], "name": s.get("name", "?"), "source": "fitx"}
        for s in fitx_raw
        if s.get("id")
    ]
    print(f"  ✅ {len(fitx_studios)} FitX Studios")
except Exception as e:
    fitx_studios = []
    print(f"  ❌ FitX Fehler: {e}")

# --- Fitness First (automatisch von GitHub) ---
print("  Fitness First Studios...")
fitnessfirst_studios = []
FF_JSON_URL = "https://raw.githubusercontent.com/developeryp/GymTime/main/fitness_first_studios.json"
try:
    resp = requests.get(FF_JSON_URL, timeout=10)
    resp.raise_for_status()
    ff_raw = resp.json()
    fitnessfirst_studios = [
        {"id": s["id"], "name": s.get("studioName", "?"), "source": "fitnessfirst"}
        for s in ff_raw
        if s.get("id")
    ]
    print(f"  ✅ {len(fitnessfirst_studios)} Fitness First Studios")
except Exception as e:
    fitnessfirst_studios = []
    print(f"  ❌ Fitness First Fehler: {e}")

all_studios = magicline_studios + fitx_studios + fitnessfirst_studios
print(f"\n📊 Gesamt: {len(all_studios)} Studios\n")

# ============================================================
# AUSLASTUNGSDATEN HOLEN
# ============================================================

APIS = {
    "magicline": "https://rsg-group.api.magicline.com/connect/v1/studio/{studio_id}/utilization",
    "fitx": "https://mein.fitx.de/nox/public/v1/studios/{studio_id}/utilization",
    "fitnessfirst": "https://www.fitnessfirst.de/club/api/checkins/{studio_id}",
}

all_observations = []
weekday = get_weekday()
date_str = get_date()
success_count = 0
fail_count = 0
skip_count = 0

print(f"📅 Datum: {date_str} ({weekday})")
print(f"🔄 Hole Auslastung für {len(all_studios)} Studios...\n")

for i, studio in enumerate(all_studios):
    sid = studio["id"]
    source = studio["source"]
    name = studio["name"]

    if (i + 1) % 50 == 0 or i == 0:
        print(f"  [{i + 1}/{len(all_studios)}] {name}...")

    url = APIS[source].format(studio_id=sid)
    headers = {}
    if source == "fitx":
        headers = {"x-tenant": "fitx", "accept": "application/json"}

    try:
        resp = requests.get(url, headers=headers, timeout=8)
        if resp.status_code != 200:
            fail_count += 1
            continue

        data = resp.json()
        if source == "fitnessfirst":
            items = data.get("data", {}).get("items", [])
        else:
            items = data.get("items", [])

        if not items:
            skip_count += 1
            continue

        # Aktuelle Stunde finden
        current_hour = None
        for item in items:
            if item.get("isCurrent", False):
                current_hour = parse_hour(item.get("startTime"))
                break

        if current_hour is None:
            skip_count += 1
            continue

        # Vergangene Stunden sammeln (gleiche Logik wie Swift-App)
        count = 0
        for item in items:
            hour = parse_hour(item.get("startTime"))
            pct = item.get("percentage", 0)
            is_current = item.get("isCurrent", False)

            if hour < current_hour and not is_current:
                all_observations.append(
                    {
                        "studio_id": sid,
                        "weekday": weekday,
                        "hour": hour,
                        "percentage": pct,
                        "observed_at": date_str,
                        "device_hash": DEVICE_HASH,
                    }
                )
                count += 1

        if count > 0:
            success_count += 1
        else:
            skip_count += 1

    except Exception:
        fail_count += 1
        continue

    # Rate limiting
    if (i + 1) % 10 == 0:
        time.sleep(0.5)


# ============================================================
# ERGEBNIS
# ============================================================

print(f"\n{'=' * 55}")
print(f"✅ Erfolgreich: {success_count} Studios")
print(f"⏭️  Übersprungen: {skip_count} (geschlossen/keine Daten)")
print(f"❌ Fehler: {fail_count}")
print(f"📊 Gesamt Datenpunkte: {len(all_observations)}")
print(f"{'=' * 55}\n")

if all_observations:
    pcts = [o["percentage"] for o in all_observations]
    zeros = sum(1 for p in pcts if p == 0)
    low = sum(1 for p in pcts if 0 < p < 30)
    mid = sum(1 for p in pcts if 30 <= p < 70)
    high = sum(1 for p in pcts if p >= 70)
    print(f"  📈 Verteilung:")
    print(f"     0%: {zeros}x (frühe Stunden / geschlossen)")
    print(f"     1-29%: {low}x")
    print(f"     30-69%: {mid}x")
    print(f"     70%+: {high}x")
    print(f"     Max: {max(pcts)}%\n")

    # ============================================================
    # DIREKT NACH SUPABASE PUSHEN
    # ============================================================

    print("🚀 Pushe Daten nach Supabase...\n")

    inserted = supabase_push_observations(all_observations)

    if inserted == 0:
        print("\n⚠️  RPC fehlgeschlagen, versuche Direct Insert...\n")
        inserted = supabase_push_direct(all_observations)

    print(f"\n{'=' * 55}")
    print(f"📊 {inserted}/{len(all_observations)} Datenpunkte in Supabase geschrieben")
    print(f"{'=' * 55}\n")

    if inserted > 0:
        print("🔄 Aktualisiere Materialized View...")
        supabase_refresh_view()

    print("\n✅ Fertig! Daten sind jetzt in Supabase verfügbar.")

else:
    print("⚠️  Keine Daten gesammelt – nichts zu pushen.")
