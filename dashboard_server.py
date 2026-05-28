"""
WikiGuard - Real-Time Edit Fraud Detection
dashboard_server.py — Flask backend serving live fraud data to the dashboard
"""

import os
import json
import glob
import time
from flask import Flask, jsonify, render_template_string
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

OUTPUT_DIR = "C:/tmp/wikiguard_output"

# In-memory ring buffer so dashboard has data even before Spark writes
_buffer = []
MAX_BUFFER = 200

def load_latest_events(limit=50):
    """Read the most recent fraud events from Spark's JSON output."""
    events = []
    try:
        files = sorted(glob.glob(os.path.join(OUTPUT_DIR, "*.json")), key=os.path.getmtime, reverse=True)
        for fpath in files[:10]:  # read last 10 files
            try:
                with open(fpath) as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            try:
                                events.append(json.loads(line))
                            except json.JSONDecodeError:
                                pass
            except Exception:
                pass
    except Exception:
        pass

    # Sort by event_time descending
    events.sort(key=lambda x: x.get("event_time", ""), reverse=True)
    return events[:limit]


@app.route("/api/events")
def get_events():
    events = load_latest_events(100)
    return jsonify(events)


@app.route("/api/stats")
def get_stats():
    events = load_latest_events(200)
    total = len(events)
    high_risk = sum(1 for e in events if e.get("fraud_label") == "HIGH_RISK")
    suspicious = sum(1 for e in events if e.get("fraud_label") == "SUSPICIOUS")
    scores = [e.get("fraud_score", 0) for e in events if e.get("fraud_score") is not None]
    avg_score = round(sum(scores) / len(scores), 1) if scores else 0

    wikis = {}
    for e in events:
        w = e.get("wiki", "unknown")
        wikis[w] = wikis.get(w, 0) + 1

    top_wikis = sorted(wikis.items(), key=lambda x: x[1], reverse=True)[:6]

    return jsonify({
        "total_flagged": total,
        "high_risk": high_risk,
        "suspicious": suspicious,
        "avg_score": avg_score,
        "top_wikis": top_wikis,
        "timestamp": time.time()
    })


@app.route("/")
def index():
    return "WikiGuard API running. Open dashboard.html in your browser."


if __name__ == "__main__":
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print("[WikiGuard Dashboard Server] Running on http://localhost:5000")
    app.run(host="0.0.0.0", port=5000, debug=False)