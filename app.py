#!/usr/bin/env python3
"""
Pytrends Flask API v6 — Multi-set manual trigger, global 4h cooldown, always-complete dashboard
Deploy FREE on Render.com  |  n8n Cloud calls this via HTTP Request node.

Architecture:
  • Dual cache per game-set key:
      current   — 23h TTL (may have failed countries)
      last_good — permanent, written only when ALL 6 countries succeed
  • Merge logic: failed/empty countries → substitute last_good data
  • GLOBAL cooldown: 4h minimum between ANY fresh Google fetch (shared across all game sets)
    → Prevents rate-limiting when running 5 different game sets per day
    → Each set has its own cache key but shares the global fetch timestamp
  • /cache/status — returns per-set cache state + global cooldown status + recommendation
  • Country-level source tagging — dashboard shows "↩ prev" for fallback data

5-set daily usage (n8n manual trigger, spaced 4h apart):
  Run Set A at 02:00 ICT → global cooldown starts
  Run Set B at 06:00 ICT → 4h passed, safe
  Run Set C at 10:00 ICT → 4h passed, safe
  Run Set D at 14:00 ICT → 4h passed, safe
  Run Set E at 18:00 ICT → 4h passed, safe

  Total Google requests per day: 5 sets × 6 countries = 30 calls spread over 16 hours (~1.9/h)
  Extremely safe — well within Google Trends rate limit tolerance.
"""

from flask import Flask, request, jsonify
from pytrends.request import TrendReq
import time, random, os, hashlib, json
from datetime import datetime, timedelta

app = Flask(__name__)

CACHE_FILE      = "/tmp/trends_cache_v6.json"
CACHE_TTL_HOURS = 23     # per-set cache TTL
COOLDOWN_HOURS  = 4      # global minimum hours between ANY fresh Google fetch

SEA_COUNTRIES = [
    ("TH", "TH"),
    ("MY", "Malay"),
    ("ID", "Indo"),
    ("SG", "SG"),
    ("PH", "PH"),
    ("VN", "Viet"),
]
ALL_LABELS = [c[1] for c in SEA_COUNTRIES]

# ── Cache helpers ─────────────────────────────────────────────────────────────
def _load_store():
    try:
        if os.path.exists(CACHE_FILE):
            with open(CACHE_FILE, "r") as f:
                return json.load(f)
    except Exception:
        pass
    return {"current": {}, "last_good": {}, "last_fresh_fetch": None}

def _save_store(store):
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(store, f)
    except Exception as e:
        print(f"[CACHE] save error: {e}")

def _cache_key(games, timeframe):
    raw = json.dumps({"games": sorted(games), "timeframe": timeframe})
    return hashlib.md5(raw.encode()).hexdigest()

def _get_current(store, key):
    entry = store["current"].get(key)
    if entry:
        if datetime.now() < datetime.fromisoformat(entry["expires"]):
            print(f"[CACHE HIT current] key={key[:8]}")
            return entry
        print(f"[CACHE EXPIRED current] key={key[:8]}")
    return None

def _get_last_good(store, key):
    entry = store["last_good"].get(key)
    if entry:
        print(f"[CACHE HIT last_good] key={key[:8]} saved={entry.get('saved_at','?')}")
    return entry

def _set_current(store, key, payload):
    store["current"][key] = {
        **payload,
        "expires":  (datetime.now() + timedelta(hours=CACHE_TTL_HOURS)).isoformat(),
        "saved_at": datetime.now().isoformat(),
    }

def _set_last_good(store, key, payload):
    store["last_good"][key] = {
        **payload,
        "saved_at": datetime.now().isoformat(),
    }

# ── Merge: fill failed countries from last_good ───────────────────────────────
def _merge_with_fallback(current_data, last_good_entry, games):
    """
    For every country where current_data has all-empty values,
    substitute data from last_good. Tag substituted countries with source='fallback'.
    Returns merged data dict + list of fallback countries used.
    """
    if not last_good_entry:
        return current_data, []

    last_good_data  = last_good_entry.get("data", {})
    merged          = dict(current_data)
    fallback_used   = []

    for label in ALL_LABELS:
        country_data = merged.get(label, {})
        has_values = any(
            v.get("values") for v in country_data.values()
            if isinstance(v, dict)
        )
        if not has_values and label in last_good_data:
            merged[label] = {
                game: {**v, "source": "fallback"}
                for game, v in last_good_data[label].items()
            }
            fallback_used.append(label)
            print(f"[FALLBACK] {label} → using last_good data")

    return merged, fallback_used

# ── Global cooldown helper ─────────────────────────────────────────────────────
def _get_cooldown_status(store):
    """
    Returns (safe_to_fetch, cooldown_remaining_hours, hours_since_fresh_fetch).
    Global — shared across ALL game sets. Prevents back-to-back fetches that burn
    through Google Trends rate limits when running multiple different game sets.
    """
    last_fresh = store.get("last_fresh_fetch")
    if not last_fresh:
        return True, 0.0, None

    last_fresh_dt = datetime.fromisoformat(last_fresh)
    hours_since   = (datetime.now() - last_fresh_dt).total_seconds() / 3600

    if hours_since >= COOLDOWN_HOURS:
        return True, 0.0, round(hours_since, 1)
    else:
        remaining = COOLDOWN_HOURS - hours_since
        return False, round(remaining, 1), round(hours_since, 1)

# ─────────────────────────────────────────────────────────────────────────────
@app.route("/health", methods=["GET"])
def health():
    store = _load_store()
    safe_to_fetch, cooldown_remaining, hours_since = _get_cooldown_status(store)
    return jsonify({
        "status":                   "ok",
        "countries":                [c[0] for c in SEA_COUNTRIES],
        "current_entries":          len(store.get("current", {})),
        "last_good_entries":        len(store.get("last_good", {})),
        "safe_to_fetch":            safe_to_fetch,
        "cooldown_remaining_hours": cooldown_remaining,
        "hours_since_fresh_fetch":  hours_since,
        "last_fresh_fetch_at":      store.get("last_fresh_fetch"),
        "cooldown_hours":           COOLDOWN_HOURS,
        "version":                  "v6",
    })

@app.route("/cache/status", methods=["POST"])
def cache_status():
    """
    n8n Safety Gate calls this before deciding whether to fetch fresh or use cache.
    Returns per-set cache state AND global cooldown status.
    safe_to_fetch = global cooldown passed (regardless of which game set was last fetched).
    """
    body      = request.get_json(force=True, silent=True) or {}
    games     = body.get("games", [])
    timeframe = body.get("timeframe", "today 3-m")

    if not games:
        return jsonify({"error": "games required"}), 400

    key   = _cache_key(games, timeframe)
    store = _load_store()

    safe_to_fetch, cooldown_remaining, hours_since_fresh = _get_cooldown_status(store)
    current   = store["current"].get(key)
    last_good = store["last_good"].get(key)

    result = {
        "key":                       key[:8],
        "has_current":               current is not None,
        "has_last_good":             last_good is not None,
        "current_expired":           True,
        "current_age_hours":         None,
        "current_missing_countries": [],
        "last_good_age_hours":       None,
        # Global cooldown (shared across ALL game sets)
        "safe_to_fetch":             safe_to_fetch,
        "cooldown_remaining_hours":  cooldown_remaining,
        "hours_since_fresh_fetch":   hours_since_fresh,
        "last_fresh_fetch_at":       store.get("last_fresh_fetch"),
        "cooldown_hours":            COOLDOWN_HOURS,
        "recommendation":            "fetch_fresh",
    }

    if current:
        expires  = datetime.fromisoformat(current["expires"])
        saved_at = datetime.fromisoformat(current["saved_at"])
        age_h    = (datetime.now() - saved_at).total_seconds() / 3600
        expired  = datetime.now() >= expires

        result["current_expired"]   = expired
        result["current_age_hours"] = round(age_h, 1)

        missing = []
        for label in ALL_LABELS:
            country_data = current.get("data", {}).get(label, {})
            has_values   = any(
                v.get("values") for v in country_data.values()
                if isinstance(v, dict)
            )
            if not has_values:
                missing.append(label)
        result["current_missing_countries"] = missing

        if not expired and not missing:
            result["recommendation"] = "use_cache"
        elif not expired and missing and last_good:
            result["recommendation"] = "use_cache_with_fallback"
        else:
            result["recommendation"] = "fetch_fresh"

    if last_good:
        lg_saved = datetime.fromisoformat(last_good["saved_at"])
        lg_age   = (datetime.now() - lg_saved).total_seconds() / 3600
        result["last_good_age_hours"] = round(lg_age, 1)

    return jsonify(result)

@app.route("/cache/clear", methods=["POST"])
def clear_cache():
    try:
        if os.path.exists(CACHE_FILE):
            os.remove(CACHE_FILE)
    except Exception:
        pass
    return jsonify({"status": "ok", "message": "cache cleared"})

# ─────────────────────────────────────────────────────────────────────────────
@app.route("/trends", methods=["POST"])
def get_trends():
    import json as _json

    # ── Parse body ────────────────────────────────────────────────────────────
    raw_body = request.get_data(as_text=True)
    idx = raw_body.find('{')
    if idx > 0:
        raw_body = raw_body[idx:]

    body = None
    try:
        body = request.get_json(force=True, silent=True)
        if isinstance(body, str):
            body = _json.loads(body)
    except Exception:
        pass
    if not isinstance(body, dict):
        try:
            body = _json.loads(raw_body)
            if isinstance(body, str):
                body = _json.loads(body)
        except Exception:
            body = {}
    if not isinstance(body, dict):
        body = {}

    games     = body.get("games") or body.get("game_list", [])
    timeframe = body.get("timeframe", "today 3-m")
    force     = body.get("force_refresh", False)

    print(f"[REQUEST] games={games} timeframe={timeframe} force={force}")

    if not games:
        return jsonify({"status": "error", "message": "games array required"}), 400

    key   = _cache_key(games, timeframe)
    store = _load_store()

    safe_to_fetch, cooldown_remaining, hours_since_fresh = _get_cooldown_status(store)

    # ── Cache check ───────────────────────────────────────────────────────────
    if not force:
        current = _get_current(store, key)
        if current:
            merged_data, fallback_used = _merge_with_fallback(
                current.get("data", {}),
                _get_last_good(store, key),
                games
            )
            return jsonify({
                **current,
                "data":                     merged_data,
                "fallback_used":            fallback_used,
                "from_cache":               True,
                "safe_to_fetch":            safe_to_fetch,
                "cooldown_remaining_hours": cooldown_remaining,
            })

    # ── Global cooldown gate (blocks even force_refresh if too soon) ──────────
    if not safe_to_fetch:
        print(f"[COOLDOWN] Blocked — {cooldown_remaining}h remaining. Serving best available data.")
        current = store["current"].get(key)
        last_good_entry = _get_last_good(store, key)
        if current:
            merged_data, fallback_used = _merge_with_fallback(
                current.get("data", {}), last_good_entry, games
            )
            return jsonify({
                **current,
                "data":                     merged_data,
                "fallback_used":            fallback_used,
                "from_cache":               True,
                "blocked_reason":           f"Global cooldown: {cooldown_remaining}h remaining",
                "safe_to_fetch":            False,
                "cooldown_remaining_hours": cooldown_remaining,
            })
        if last_good_entry:
            return jsonify({
                **last_good_entry,
                "from_cache":               True,
                "fallback_used":            ALL_LABELS,
                "blocked_reason":           f"Global cooldown: {cooldown_remaining}h remaining",
                "safe_to_fetch":            False,
                "cooldown_remaining_hours": cooldown_remaining,
            })
        return jsonify({
            "status":                   "error",
            "message":                  f"Cooldown active ({cooldown_remaining:.1f}h remaining). No cache available.",
            "cooldown_remaining_hours": cooldown_remaining,
        }), 429

    # ── Fresh fetch from Google Trends ───────────────────────────────────────
    pytrends = TrendReq(
        hl="en-US", tz=420,
        timeout=(15, 35),
        retries=1,
        backoff_factor=0.5
    )

    results          = {}
    failed_countries = []
    total_countries  = len(SEA_COUNTRIES)

    for country_idx, (geo, label) in enumerate(SEA_COUNTRIES):
        results[label] = {}
        chunks       = [games[i: i + 5] for i in range(0, len(games), 5)]
        total_chunks = len(chunks)
        country_ok   = True

        for chunk_idx, chunk in enumerate(chunks):
            for attempt in range(2):
                try:
                    if attempt == 0 and (country_idx > 0 or chunk_idx > 0):
                        time.sleep(random.uniform(1, 3))   # pre-request jitter

                    pytrends.build_payload(
                        chunk, cat=0, timeframe=timeframe, geo=geo, gprop=""
                    )
                    df = pytrends.interest_over_time()

                    if not df.empty:
                        if "isPartial" in df.columns:
                            df = df.drop(columns=["isPartial"])
                        for g in chunk:
                            if g in df.columns:
                                results[label][g] = {
                                    "dates":  [str(d.date()) for d in df.index],
                                    "values": df[g].tolist(),
                                    "avg":    round(float(df[g].mean()), 2),
                                    "peak":   int(df[g].max()),
                                    "source": "fresh",
                                }
                            else:
                                results[label][g] = {
                                    "dates": [], "values": [], "avg": 0, "peak": 0,
                                    "source": "empty"
                                }
                    else:
                        for g in chunk:
                            results[label][g] = {
                                "dates": [], "values": [], "avg": 0, "peak": 0,
                                "source": "empty"
                            }
                    break  # success

                except Exception as e:
                    msg = str(e)
                    is_rate = ("429" in msg or "too many" in msg.lower() or
                               "response" in msg.lower() or "retries exceeded" in msg.lower())
                    if attempt == 0:
                        wait = random.uniform(20, 30) if is_rate else random.uniform(6, 10)
                        print(f"[{geo}] chunk={chunk_idx} retry wait={wait:.0f}s err={msg[:80]}")
                        time.sleep(wait)
                    else:
                        for g in chunk:
                            results[label][g] = {
                                "dates": [], "values": [], "avg": 0, "peak": 0,
                                "error": msg[:120], "source": "error"
                            }
                        country_ok = False

            if chunk_idx < total_chunks - 1:
                time.sleep(random.uniform(8, 12))

        if not country_ok:
            failed_countries.append(label)

        if country_idx < total_countries - 1:
            delay = random.uniform(15, 20)
            print(f"[DELAY] after {geo}: {delay:.1f}s")
            time.sleep(delay)

    # ── Record global fresh fetch timestamp ──────────────────────────────────
    store["last_fresh_fetch"] = datetime.now().isoformat()
    print(f"[COOLDOWN] Fresh fetch complete. Next safe fetch in {COOLDOWN_HOURS}h.")

    # ── Save current cache (partial results are fine) ─────────────────────────
    current_payload = {
        "status":           "ok",
        "data":             results,
        "games":            games,
        "timeframe":        timeframe,
        "countries":        ALL_LABELS,
        "failed_countries": failed_countries,
        "from_cache":       False,
    }
    _set_current(store, key, current_payload)

    # ── Update last_good only when ALL countries succeeded ────────────────────
    if not failed_countries:
        all_have_data = all(
            any(v.get("values") for v in results[label].values() if isinstance(v, dict))
            for label in ALL_LABELS
        )
        if all_have_data:
            _set_last_good(store, key, current_payload)
            print(f"[LAST_GOOD] Updated — all 6 countries have data")

    _save_store(store)

    # ── Merge fallback for any failed countries before returning ──────────────
    merged_data, fallback_used = _merge_with_fallback(
        results,
        _get_last_good(store, key),
        games
    )

    return jsonify({
        **current_payload,
        "data":          merged_data,
        "fallback_used": fallback_used,
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
