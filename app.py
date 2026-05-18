#!/usr/bin/env python3
"""
Pytrends Flask API v2 — optimized for speed (<3 min for 5 games × 6 countries)
Deploy FREE on Render.com  |  n8n Cloud calls this via HTTP Request node.

Speed optimizations vs v1:
  • In-memory cache (1-hour TTL) — repeated runs return instantly
  • Inter-country delay: 20-35s → 8-12s
  • Inter-chunk delay:   10-18s → 5-8s  (only fires when >5 games)
  • Retry delay:         40-80s × n → 20-30s (429) / 8-12s (other)
  • Retry only 2 attempts instead of 3
  • Skips final country/chunk delay (no wasted sleep after last item)
"""

from flask import Flask, request, jsonify
from pytrends.request import TrendReq
import time, random, os, hashlib, json
from datetime import datetime, timedelta

app = Flask(__name__)

# ── In-memory cache (1-hour TTL) ──────────────────────────────────────────────
_cache = {}
CACHE_TTL_HOURS = 1

def _cache_key(games, timeframe):
    raw = json.dumps({"games": sorted(games), "timeframe": timeframe})
    return hashlib.md5(raw.encode()).hexdigest()

def _cache_get(key):
    entry = _cache.get(key)
    if entry and datetime.now() < entry["expires"]:
        print(f"[CACHE HIT] key={key[:8]}")
        return entry["data"]
    return None

def _cache_set(key, data):
    _cache[key] = {
        "data": data,
        "expires": datetime.now() + timedelta(hours=CACHE_TTL_HOURS)
    }

# ── Hardcoded SEA countries — never change, never passed from n8n ─────────────
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
    return jsonify({
        "status": "ok",
        "countries": [c[0] for c in SEA_COUNTRIES],
        "cache_entries": len(_cache),
        "version": "v2"
    })

@app.route("/cache/clear", methods=["POST"])
def clear_cache():
    _cache.clear()
    return jsonify({"status": "ok", "message": "cache cleared"})

# ─────────────────────────────────────────────────────────────────────────────
@app.route("/trends", methods=["POST"])
def get_trends():
    import json as _json
    raw_body = request.get_data(as_text=True)
    try:
        body = _json.loads(raw_body)
        if isinstance(body, str):
            body = _json.loads(body)  # fix double-encoded JSON from n8n
    except Exception:
        body = {}
    games     = body.get("games") or body.get("game_list", [])
    timeframe = body.get("timeframe", "today 3-m")
    force     = body.get("force_refresh", False)   # set true to bypass cache

    if not games:
        return jsonify({"status": "error", "message": "games array required"}), 400

    # ── Cache check ───────────────────────────────────────────────────────────
    cache_key = _cache_key(games, timeframe)
    if not force:
        cached = _cache_get(cache_key)
        if cached:
            return jsonify({**cached, "from_cache": True})

    # ── Fresh fetch ───────────────────────────────────────────────────────────
    # timeout=(connect, read): read timeout generous for slow Trends responses
    pytrends = TrendReq(
        hl="en-US", tz=420,
        timeout=(10, 25),
        retries=1,
        backoff_factor=0.3
    )
    results = {}
    total_countries = len(SEA_COUNTRIES)

    for country_idx, (geo, label) in enumerate(SEA_COUNTRIES):
        results[label] = {}
        chunks = [games[i: i + 5] for i in range(0, len(games), 5)]
        total_chunks = len(chunks)

        for chunk_idx, chunk in enumerate(chunks):

            for attempt in range(2):   # max 2 retries (faster failure)
                try:
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
                    break   # success — exit retry loop

                except Exception as e:
                    msg = str(e)
                    is_rate_limit = ("429" in msg or
                                     "too many" in msg.lower() or
                                     "response" in msg.lower())
                    if attempt == 0:
                        # Short wait then retry
                        wait = random.uniform(20, 30) if is_rate_limit else random.uniform(8, 12)
                        print(f"[{geo}] chunk={chunk_idx} retry: {msg[:80]} — wait {wait:.0f}s")
                        time.sleep(wait)
                    else:
                        # Give up on this chunk — record empty/error
                        for g in chunk:
                            results[label][g] = {
                                "dates": [], "values": [], "avg": 0, "peak": 0,
                                "error": msg[:120]
                            }

            # ── Delay between chunks (skip after final chunk) ─────────────────
            if chunk_idx < total_chunks - 1:
                time.sleep(random.uniform(5, 8))

        # ── Delay between countries (skip after final country) ────────────────
        if country_idx < total_countries - 1:
            time.sleep(random.uniform(8, 12))

    payload = {
        "status":     "ok",
        "data":       results,
        "games":      games,
        "timeframe":  timeframe,
        "countries":  [c[1] for c in SEA_COUNTRIES],
        "from_cache": False,
    }
    _cache_set(cache_key, payload)
    return jsonify(payload)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
