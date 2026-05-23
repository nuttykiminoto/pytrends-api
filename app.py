#!/usr/bin/env python3
"""
Pytrends Flask API v7 — Individual retry for low-volume games
Deploy FREE on Render.com  |  n8n Cloud calls this via HTTP Request node.

Architecture:
  Phase 1 — Grouped query (up to 5 keywords per call, current behaviour).
             Results tagged source='group'.
  Phase 2 — Individual retry for any game/country pair that returned empty.
             Queries ONE keyword at a time, so Google normalises against
             that game alone — recovers low-volume terms suppressed by
             grouped normalisation. Results tagged source='individual'.
  Phase 3 — Cache + cooldown recorded AFTER both phases complete.

  Dual cache per game-set key:
    current   — 23h TTL (may have individual-recovered data)
    last_good — permanent, written only when ALL pairs have data
  Merge logic: failed/empty countries → substitute last_good data
  GLOBAL cooldown: 4h minimum between ANY fresh Google fetch

Country-level source tagging:
  'group'      — retrieved in grouped query
  'individual' — recovered via solo retry
  'fallback'   — substituted from last_good cache
  'no_data'    — genuinely no Google Trends data
  'error'      — API error on both attempts
"""

from flask import Flask, request, jsonify
from pytrends.request import TrendReq
import time, random, os, hashlib, json
from datetime import datetime, timedelta

app = Flask(__name__)

CACHE_FILE      = "/tmp/trends_cache_v7.json"
CACHE_TTL_HOURS = 23
COOLDOWN_HOURS  = 4

# When a high-volume game is in the same grouped query, it suppresses
# lower-volume games — their values get normalized down to near-zero.
# Any game whose grouped-query peak is at or below this threshold is
# re-queried individually so it gets its own 0–100 normalization.
SUPPRESSION_THRESHOLD = 20

# Safety limits — prevent runaway force-refresh and individual retry floods.
# force_refresh is dangerous: bypasses cooldown → higher block risk.
# Max 2 force_refresh calls allowed in any rolling 24-hour window.
FORCE_REFRESH_MAX_PER_24H = 2
# Individual retry makes one API call per (game × country) pair.
# With 4 games × 6 countries = 24 pairs max. Cap at 10 to limit exposure
# when suppression is widespread and many pairs need individual retry.
INDIVIDUAL_RETRY_BUDGET = 30   # raised from 10 — covers up to 6 countries × 5 games

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
    if not last_good_entry:
        return current_data, []

    last_good_data = last_good_entry.get("data", {})
    merged         = dict(current_data)
    fallback_used  = []

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

# ── force_refresh rate-limit helpers ──────────────────────────────────────────
def _get_force_refresh_log(store):
    """Return list of ISO timestamps of force_refresh calls in the last 24h."""
    log = store.get("force_refresh_log", [])
    cutoff = (datetime.now() - timedelta(hours=24)).isoformat()
    return [ts for ts in log if ts >= cutoff]

def _can_force_refresh(store):
    """Return (allowed: bool, used: int, remaining: int)."""
    log   = _get_force_refresh_log(store)
    used  = len(log)
    remaining = max(0, FORCE_REFRESH_MAX_PER_24H - used)
    return remaining > 0, used, remaining

def _record_force_refresh(store):
    log = _get_force_refresh_log(store)
    log.append(datetime.now().isoformat())
    store["force_refresh_log"] = log

# ── Global cooldown helper ─────────────────────────────────────────────────────
def _get_cooldown_status(store):
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

# ── Individual retry for a single game/country pair ───────────────────────────
def _fetch_individual(pytrends, geo, label, game, timeframe):
    """
    Query one game for one country in isolation.
    Normalization is relative to that game alone (not suppressed by grouped query).
    Returns a result dict with source='individual', or source='no_data'/'error'.
    """
    for attempt in range(2):
        try:
            if attempt > 0:
                time.sleep(random.uniform(10, 15))
            pytrends.build_payload([game], cat=0, timeframe=timeframe, geo=geo, gprop="")
            df = pytrends.interest_over_time()
            if not df.empty and game in df.columns:
                if "isPartial" in df.columns:
                    df = df.drop(columns=["isPartial"])
                return {
                    "dates":  [str(d.date()) for d in df.index],
                    "values": df[game].tolist(),
                    "avg":    round(float(df[game].mean()), 2),
                    "peak":   int(df[game].max()),
                    "source": "individual",
                }
            else:
                return {"dates": [], "values": [], "avg": 0, "peak": 0, "source": "no_data"}
        except Exception as e:
            msg = str(e)
            is_rate = "429" in msg or "too many" in msg.lower()
            if is_rate:
                # Return immediately with rate_limited sentinel — caller stops the loop.
                print(f"[INDIVIDUAL] {geo}/{game} → 429 rate limit, stopping individual retry")
                return {"dates": [], "values": [], "avg": 0, "peak": 0,
                        "error": msg[:120], "source": "rate_limited"}
            if attempt == 0:
                wait = random.uniform(8, 12)
                print(f"[INDIVIDUAL] {geo}/{game} retry wait={wait:.0f}s err={msg[:60]}")
                time.sleep(wait)
            else:
                return {"dates": [], "values": [], "avg": 0, "peak": 0,
                        "error": msg[:120], "source": "error"}

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
        "version":                  "v7",
    })

@app.route("/cache/status", methods=["POST"])
def cache_status():
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

@app.route("/force_refresh/status", methods=["GET"])
def force_refresh_status():
    """How many force_refresh calls remain in the current 24-hour window."""
    store = _load_store()
    fr_allowed, fr_used, fr_remaining = _can_force_refresh(store)
    log = _get_force_refresh_log(store)
    return jsonify({
        "force_refresh_allowed":     fr_allowed,
        "force_refresh_used_24h":    fr_used,
        "force_refresh_remaining":   fr_remaining,
        "force_refresh_max_per_24h": FORCE_REFRESH_MAX_PER_24H,
        "force_refresh_log":         log,
    })

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

    games           = body.get("games") or body.get("game_list", [])
    timeframe       = body.get("timeframe", "today 3-m")
    force           = body.get("force_refresh", False)
    skip_individual = body.get("skip_individual_retry", False)  # opt-out flag

    print(f"[REQUEST] games={games} timeframe={timeframe} force={force} skip_individual={skip_individual}")

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

    # ── force_refresh rate-limit gate ────────────────────────────────────────
    # force=True bypasses cooldown, which is the highest-risk operation.
    # Max FORCE_REFRESH_MAX_PER_24H uses per rolling 24-hour window.
    if force:
        fr_allowed, fr_used, fr_remaining = _can_force_refresh(store)
        if not fr_allowed:
            print(f"[FORCE_REFRESH] BLOCKED — used {fr_used}/{FORCE_REFRESH_MAX_PER_24H} in last 24h")
            # Fall back to best available cache instead of hard error
            current = _get_current(store, key)
            if current:
                merged_data, fallback_used = _merge_with_fallback(
                    current.get("data", {}), _get_last_good(store, key), games
                )
                return jsonify({
                    **current,
                    "data":                     merged_data,
                    "fallback_used":            fallback_used,
                    "from_cache":               True,
                    "blocked_reason":           f"force_refresh limit: used {fr_used}/{FORCE_REFRESH_MAX_PER_24H} in 24h",
                    "safe_to_fetch":            safe_to_fetch,
                    "cooldown_remaining_hours": cooldown_remaining,
                })
            last_good_entry = _get_last_good(store, key)
            if last_good_entry:
                return jsonify({
                    **last_good_entry,
                    "from_cache":               True,
                    "fallback_used":            ALL_LABELS,
                    "blocked_reason":           f"force_refresh limit: used {fr_used}/{FORCE_REFRESH_MAX_PER_24H} in 24h",
                    "safe_to_fetch":            safe_to_fetch,
                    "cooldown_remaining_hours": cooldown_remaining,
                })
            return jsonify({
                "status":  "error",
                "message": f"force_refresh limit reached ({fr_used}/{FORCE_REFRESH_MAX_PER_24H} in 24h). No cache available.",
            }), 429
        # Rate limit OK — record this use now so concurrent requests don't double-fire
        _record_force_refresh(store)
        _save_store(store)
        print(f"[FORCE_REFRESH] allowed ({fr_used+1}/{FORCE_REFRESH_MAX_PER_24H} used in 24h)")

    # ── Global cooldown gate ──────────────────────────────────────────────────
    if not safe_to_fetch and not force:
        print(f"[COOLDOWN] Blocked — {cooldown_remaining}h remaining.")
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

    # ── Phase 1: Grouped fetch ────────────────────────────────────────────────
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
                        time.sleep(random.uniform(1, 3))

                    pytrends.build_payload(
                        chunk, cat=0, timeframe=timeframe, geo=geo, gprop=""
                    )
                    df = pytrends.interest_over_time()

                    if not df.empty:
                        if "isPartial" in df.columns:
                            df = df.drop(columns=["isPartial"])
                        for g in chunk:
                            if g in df.columns:
                                values = df[g].tolist()
                                results[label][g] = {
                                    "dates":  [str(d.date()) for d in df.index],
                                    "values": values,
                                    "avg":    round(float(df[g].mean()), 2),
                                    "peak":   int(df[g].max()),
                                    "source": "group",
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
                    break

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

    # ── Phase 2: Individual retry for empty game/country pairs ────────────────
    # Find pairs where grouped query returned no data.
    # Query each individually — this avoids suppression from grouped normalization.
    individual_recovered     = []
    individual_failed        = []
    individual_budget_used   = 0
    individual_rate_limited  = False

    if not skip_individual:
        empty_pairs = []
        for geo, label in SEA_COUNTRIES:
            for g in games:
                entry    = results.get(label, {}).get(g, {})
                vals     = entry.get("values", [])
                peak_val = entry.get("peak", 0)
                # Retry if: no data, all-zero, OR grouped peak so low the game is
                # likely suppressed by a higher-volume co-query game.
                # Individual query gives this game its own 0-100 normalization.
                needs_retry = (
                    not vals
                    or not any(v > 0 for v in vals)
                    or peak_val <= SUPPRESSION_THRESHOLD
                )
                if needs_retry:
                    empty_pairs.append((geo, label, g))

        if empty_pairs:
            budget_used      = 0
            rate_limited_429  = False
            capped_pairs     = empty_pairs[:INDIVIDUAL_RETRY_BUDGET]
            skipped_pairs    = empty_pairs[INDIVIDUAL_RETRY_BUDGET:]

            if skipped_pairs:
                print(f"[INDIVIDUAL RETRY] budget cap={INDIVIDUAL_RETRY_BUDGET}: "
                      f"running {len(capped_pairs)}, skipping {len(skipped_pairs)}")
            else:
                print(f"[INDIVIDUAL RETRY] {len(capped_pairs)} pairs → retrying individually")

            prev_label = None
            for i, (geo, label, g) in enumerate(capped_pairs):
                # Longer delay when switching countries
                if i > 0:
                    wait = random.uniform(18, 25) if label != prev_label else random.uniform(10, 15)
                    print(f"[INDIVIDUAL] waiting {wait:.1f}s before {geo}/{g}")
                    time.sleep(wait)

                print(f"[INDIVIDUAL] querying {geo}/{g}")
                result = _fetch_individual(pytrends, geo, label, g, timeframe)
                results[label][g] = result
                budget_used += 1

                if result.get("source") == "rate_limited":
                    # 429 from Google — stop immediately, do not retry any more pairs
                    rate_limited_429 = True
                    individual_failed.append(f"{label}/{g}")
                    print(f"[INDIVIDUAL RETRY] 429 received — stopping (used {budget_used} of {INDIVIDUAL_RETRY_BUDGET} budget)")
                    break
                elif result.get("values"):
                    individual_recovered.append(f"{label}/{g}")
                    print(f"[INDIVIDUAL] {geo}/{g} ✓ recovered {len(result['values'])} pts (source=individual)")
                else:
                    individual_failed.append(f"{label}/{g}")
                    print(f"[INDIVIDUAL] {geo}/{g} → {result.get('source','no_data')}")

                prev_label = label

            individual_budget_used  = budget_used
            individual_rate_limited = rate_limited_429
            print(f"[INDIVIDUAL RETRY] done — recovered={individual_recovered} "
                  f"failed={individual_failed} budget_used={budget_used} "
                  f"rate_limited={rate_limited_429}")

    # ── Record global fresh fetch timestamp (after ALL phases) ───────────────
    store["last_fresh_fetch"] = datetime.now().isoformat()
    print(f"[COOLDOWN] Fresh fetch complete (phases 1+2). Next safe fetch in {COOLDOWN_HOURS}h.")

    # ── Compute coverage stats ────────────────────────────────────────────────
    total_pairs  = len(SEA_COUNTRIES) * len(games)
    filled_pairs = sum(
        1 for _, label in SEA_COUNTRIES
        for g in games
        if results.get(label, {}).get(g, {}).get("values")
    )
    coverage_pct = round(filled_pairs / total_pairs * 100) if total_pairs > 0 else 0

    # ── Save current cache ────────────────────────────────────────────────────
    current_payload = {
        "status":                      "ok",
        "data":                        results,
        "games":                       games,
        "timeframe":                   timeframe,
        "countries":                   ALL_LABELS,
        "failed_countries":            failed_countries,
        "individual_recovered":        individual_recovered,
        "individual_failed":           individual_failed,
        "individual_budget_used":      individual_budget_used,
        "individual_budget_cap":       INDIVIDUAL_RETRY_BUDGET,
        "individual_rate_limited":     individual_rate_limited,
        "coverage_pct":                coverage_pct,
        "from_cache":                  False,
    }
    _set_current(store, key, current_payload)

    # ── Update last_good only when all pairs have data ────────────────────────
    if not failed_countries and not individual_failed:
        all_have_data = all(
            results.get(label, {}).get(g, {}).get("values")
            for _, label in SEA_COUNTRIES
            for g in games
        )
        if all_have_data:
            _set_last_good(store, key, current_payload)
            print(f"[LAST_GOOD] Updated — all pairs have data (coverage={coverage_pct}%)")

    _save_store(store)

    # ── Merge fallback for any still-missing countries ────────────────────────
    merged_data, fallback_used = _merge_with_fallback(
        results,
        _get_last_good(store, key),
        games
    )

    return jsonify({
        **current_payload,
        "data":                 merged_data,
        "fallback_used":        fallback_used,
        "coverage_pct":         coverage_pct,
        "individual_recovered": individual_recovered,
        "individual_failed":    individual_failed,
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
