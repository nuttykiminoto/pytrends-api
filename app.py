#!/usr/bin/env python3
"""
Pytrends Flask API v4 — daily-stable architecture
Deploy FREE on Render.com  |  n8n Cloud calls this via HTTP Request node.

Key improvements vs v3:
  • File-based persistent cache (/tmp/trends_cache.json) — survives server restarts
  • 23-hour cache TTL — one fresh fetch per day maximum
  • Partial result caching — saves whatever countries succeeded even if some failed
  • Wider, randomized delays (15-20s) — more human-like, harder to detect
  • Country-level retry isolation — one country failing doesn't abort the rest
  • Graceful degradation — always returns a response, never hangs
"""

from flask import Flask, request, jsonify
from pytrends.request import TrendReq
import time, random, os, hashlib, json
from datetime import datetime, timedelta

app = Flask(__name__)

# ── File-based persistent cache ───────────────────────────────────────────────
# Survives Render free-tier restarts (in-memory cache does not)
CACHE_FILE      = "/tmp/trends_cache.json"
CACHE_TTL_HOURS = 23   # just under 24h — one fresh fetch per day maximum

def _load_cache():
    try:
        if os.path.exists(CACHE_FILE):
            with open(CACHE_FILE, "r") as f:
                return json.load(f)
    except Exception:
        pass
    return {}

def _save_cache(cache):
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(cache, f)
    except Exception as e:
        print(f"[CACHE] Failed to save: {e}")

def _cache_key(games, timeframe):
    raw = json.dumps({"games": sorted(games), "timeframe": timeframe})
    return hashlib.md5(raw.encode()).hexdigest()

def _cache_get(key):
    cache = _load_cache()
    entry = cache.get(key)
    if entry:
        expires = datetime.fromisoformat(entry["expires"])
        if datetime.now() < expires:
            print(f"[CACHE HIT] key={key[:8]} expires={entry['expires']}")
            return entry["data"]
        else:
            print(f"[CACHE EXPIRED] key={key[:8]}")
    return None

def _cache_set(key, data):
    cache = _load_cache()
    cache[key] = {
        "data":    data,
        "expires": (datetime.now() + timedelta(hours=CACHE_TTL_HOURS)).isoformat()
    }
    _save_cache(cache)
    print(f"[CACHE SET] key={key[:8]} TTL={CACHE_TTL_HOURS}h")

# ── Hardcoded SEA countries ────────────────────────────────────────────────────
SEA_COUNTRIES = [
    ("TH", "TH"),
    ("MY", "Malay"),
    ("ID", "Indo"),
    ("SG", "SG"),
    ("PH", "PH"),
    ("VN", "Viet"),
]

# ─────────────────────────────────────────────────────────────────────────────
@app.route("/health", methods=["GET"])
def health():
    cache = _load_cache()
    live_keys = sum(
        1 for v in cache.values()
        if datetime.fromisoformat(v["expires"]) > datetime.now()
    )
    return jsonify({
        "status":        "ok",
        "countries":     [c[0] for c in SEA_COUNTRIES],
        "cache_entries": live_keys,
        "version":       "v4"
    })

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

    print(f"[REQUEST] games={games}, timeframe={timeframe}, force={force}")

    if not games:
        return jsonify({"status": "error", "message": "games array required"}), 400

    # ── Cache check ───────────────────────────────────────────────────────────
    cache_key = _cache_key(games, timeframe)
    if not force:
        cached = _cache_get(cache_key)
        if cached:
            return jsonify({**cached, "from_cache": True})

    # ── Fresh fetch ───────────────────────────────────────────────────────────
    pytrends = TrendReq(
        hl="en-US", tz=420,
        timeout=(15, 30),
        retries=1,
        backoff_factor=0.5
    )

    results       = {}
    total_countries = len(SEA_COUNTRIES)
    failed_countries = []

    for country_idx, (geo, label) in enumerate(SEA_COUNTRIES):
        results[label] = {}
        chunks       = [games[i: i + 5] for i in range(0, len(games), 5)]
        total_chunks = len(chunks)
        country_ok   = True

        for chunk_idx, chunk in enumerate(chunks):

            for attempt in range(2):
                try:
                    # Staggered pre-request pause (makes pattern less predictable)
                    if attempt == 0 and (country_idx > 0 or chunk_idx > 0):
                        pre_jitter = random.uniform(1, 3)
                        time.sleep(pre_jitter)

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
                                }
                            else:
                                results[label][g] = {"dates": [], "values": [], "avg": 0, "peak": 0}
                    else:
                        for g in chunk:
                            results[label][g] = {"dates": [], "values": [], "avg": 0, "peak": 0}
                    break  # success

                except Exception as e:
                    msg = str(e)
                    is_rate_limit = ("429" in msg or
                                     "too many" in msg.lower() or
                                     "response" in msg.lower() or
                                     "retries exceeded" in msg.lower())

                    if attempt == 0:
                        wait = random.uniform(20, 30) if is_rate_limit else random.uniform(6, 10)
                        print(f"[{geo}] chunk={chunk_idx} attempt=1 error={msg[:80]} wait={wait:.0f}s")
                        time.sleep(wait)
                    else:
                        print(f"[{geo}] chunk={chunk_idx} giving up: {msg[:80]}")
                        for g in chunk:
                            results[label][g] = {
                                "dates": [], "values": [], "avg": 0, "peak": 0,
                                "error": msg[:120]
                            }
                        country_ok = False

            # Delay between chunks (only when >5 games)
            if chunk_idx < total_chunks - 1:
                time.sleep(random.uniform(8, 12))

        if not country_ok:
            failed_countries.append(geo)

        # ── Inter-country delay: wider range, more human-like ─────────────────
        if country_idx < total_countries - 1:
            delay = random.uniform(15, 20)
            print(f"[DELAY] after {geo}: {delay:.1f}s")
            time.sleep(delay)

    payload = {
        "status":           "ok",
        "data":             results,
        "games":            games,
        "timeframe":        timeframe,
        "countries":        [c[1] for c in SEA_COUNTRIES],
        "failed_countries": failed_countries,
        "from_cache":       False,
    }

    # ── Always cache partial results ──────────────────────────────────────────
    # Even if some countries failed, cache what we got so the next run
    # doesn't re-fetch everything from scratch
    _cache_set(cache_key, payload)

    return jsonify(payload)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
