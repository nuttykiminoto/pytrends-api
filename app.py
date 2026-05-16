#!/usr/bin/env python3
"""
Pytrends Flask API — deploy FREE on Render.com
Handles Google Trends queries for SEA countries with rate-limit protection.
n8n Cloud calls this via HTTP Request node.
"""
from flask import Flask, request, jsonify
from pytrends.request import TrendReq
import time, random, os

app = Flask(__name__)

# Hardcoded SEA countries — these NEVER change, n8n does not pass them
SEA_COUNTRIES = [
    ("TH", "TH"),
    ("MY", "Malay"),
    ("ID", "Indo"),
    ("SG", "SG"),
    ("PH", "PH"),
    ("VN", "Viet"),
]

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "countries": [c[0] for c in SEA_COUNTRIES]})

@app.route("/trends", methods=["POST"])
def get_trends():
    body      = request.get_json(force=True)
    games     = body.get("games") or body.get("game_list", [])
    timeframe = body.get("timeframe", "today 3-m")

    if not games:
        return jsonify({"status": "error", "message": "games array required"}), 400

    pytrends = TrendReq(hl="en-US", tz=420, timeout=(10, 35),
                        retries=2, backoff_factor=0.5)
    results = {}

    for (geo, label) in SEA_COUNTRIES:
        results[label] = {}

        # Pytrends: max 5 keywords per request — chunk the games list
        for i in range(0, len(games), 5):
            chunk = games[i : i + 5]

            for attempt in range(3):
                try:
                    pytrends.build_payload(chunk, cat=0, timeframe=timeframe,
                                           geo=geo, gprop="")
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
                        for g in chunk:
                            results[label][g] = {"dates": [], "values": [], "avg": 0, "peak": 0}
                    break

                except Exception as e:
                    msg = str(e)
                    if attempt < 2:
                        wait = random.uniform(40, 80) * (attempt + 1)
                        print(f"[{geo}] retry {attempt+1}: {msg} — wait {wait:.0f}s")
                        time.sleep(wait)
                    else:
                        for g in chunk:
                            results[label][g] = {"dates": [], "values": [], "error": msg}

            time.sleep(random.uniform(10, 18))   # between chunks

        time.sleep(random.uniform(20, 35))        # between countries

    return jsonify({
        "status":    "ok",
        "data":      results,
        "games":     games,
        "timeframe": timeframe,
        "countries": [c[1] for c in SEA_COUNTRIES],
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
