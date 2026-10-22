"""Live fraud-alerts dashboard.

    streamlit run dashboard/app.py

Background daemon threads (fraud/collector.py) read Kafka and hand alerts to the
page through a queue.Queue. One Collector per server, not per browser tab. The
page is a fragment that polls snapshot() every second and never touches Kafka.
"""
import html
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # so `fraud` imports

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from fraud.collector import Collector
from fraud.common import BOOTSTRAP, CITIES

LABEL = {"impossible_travel": "Impossible travel", "velocity": "Velocity", "amount_spike": "Amount spike"}
COLOR = {"impossible_travel": "#ff6b6b", "velocity": "#f5b942", "amount_spike": "#5ab0ff"}
FRESH_S = 5  # how long an alert counts as "new" on screen
BANNER_S = 6
TOAST_EVERY_S = 4


@st.cache_resource
def collector():
    return Collector()


def track_new(alerts, now):
    """Remember when each alert first appeared; raise the critical banner for new travel alerts."""
    ss = st.session_state
    ss.setdefault("first_seen", {})
    ss.setdefault("banner_until", 0.0)
    ss.setdefault("banner_text", "")
    ss.setdefault("toast_pending", 0)
    ss.setdefault("last_toast", 0.0)
    primed = ss.get("primed", False)
    for a in reversed(alerts):  # oldest first, so the banner ends on the newest
        k = f"{a['tx_id']}|{a['rule']}"
        if k in ss.first_seen:
            continue
        # alerts already there when the page first opens are history, not news
        ss.first_seen[k] = now if primed else 0.0
        if primed and a["rule"] == "impossible_travel":
            ss.banner_text = f"CRITICAL: impossible travel, {a['user_id']}: {a['detail']}"
            ss.banner_until = now + BANNER_S
            ss.toast_pending += 1
    ss.primed = True
    # One summary toast at most every few seconds, or a busy stream buries the screen
    if ss.toast_pending and now - ss.last_toast >= TOAST_EVERY_S:
        st.toast(f"{ss.toast_pending} new impossible-travel alert(s)", icon="🚨")
        ss.toast_pending, ss.last_toast = 0, now
    live_keys = {f"{a['tx_id']}|{a['rule']}" for a in alerts}
    for k in [k for k in ss.first_seen if k not in live_keys]:  # forget alerts that scrolled off
        del ss.first_seen[k]


def is_fresh(a, now):
    seen = st.session_state.first_seen.get(f"{a['tx_id']}|{a['rule']}", 0.0)
    return bool(seen) and now - seen < FRESH_S


def make_map(alerts, now):
    fig = go.Figure()
    lines_lat, lines_lon = [], []
    for a in alerts:
        if a.get("from_city") in CITIES and a["city"] in CITIES:
            (la1, lo1), (la2, lo2) = CITIES[a["from_city"]], CITIES[a["city"]]
            lines_lat += [la1, la2, None]
            lines_lon += [lo1, lo2, None]
    if lines_lat:
        fig.add_trace(go.Scattergeo(lat=lines_lat, lon=lines_lon, mode="lines", hoverinfo="skip",
                                    line=dict(width=1, color=COLOR["impossible_travel"]), opacity=0.5,
                                    showlegend=False))
    for rule, label in LABEL.items():
        pts = [a for a in alerts if a["rule"] == rule and a["city"] in CITIES]
        if not pts:
            continue
        fig.add_trace(go.Scattergeo(
            lat=[CITIES[a["city"]][0] for a in pts],
            lon=[CITIES[a["city"]][1] for a in pts],
            mode="markers", name=label,
            marker=dict(color=COLOR[rule], opacity=0.7,
                        size=[18 if is_fresh(a, now) else 8 for a in pts],
                        line=dict(width=[3 if is_fresh(a, now) else 0 for a in pts], color="#ffffff")),
            text=[f"{a['user_id']} · {a['city']}<br>{a['detail']}" for a in pts],
            hovertemplate="%{text}<extra>" + label + "</extra>",
        ))
    fig.update_geos(projection_type="natural earth", showland=True, landcolor="#2a3340",
                    showocean=True, oceancolor="#10151c", showcountries=True, countrycolor="#3a4555",
                    coastlinecolor="#4a5668", showframe=False, bgcolor="rgba(0,0,0,0)")
    fig.update_layout(height=430, margin=dict(l=0, r=0, t=0, b=0), paper_bgcolor="rgba(0,0,0,0)",
                      legend=dict(orientation="h", y=0.02, x=0.01, bgcolor="rgba(0,0,0,0.35)"))
    return fig


def make_table(alerts, now):
    return pd.DataFrame([
        {
            "": "NEW" if is_fresh(a, now) else "",
            "Time": time.strftime("%H:%M:%S", time.localtime(a["seen_at"])),
            "Rule": LABEL.get(a["rule"], a["rule"]),
            "User": a["user_id"],
            "City": a["city"],
            "Amount": a["amount"],
            "Detail": a["detail"],
        }
        for a in alerts[:50]
    ])


st.set_page_config(page_title="Fraud Alerts", page_icon="🚨", layout="wide")
st.markdown(
    """
    <style>
      .critical { padding: 12px 16px; border-radius: 10px; font-weight: 600; color: #fff;
                  background: #d6293e; animation: flash 0.9s ease-in-out infinite alternate; margin-bottom: 8px; }
      @keyframes flash { from { background: #d6293e; } to { background: #7a1020; } }
      @media (prefers-reduced-motion: reduce) { .critical { animation: none; } }
    </style>
    """,
    unsafe_allow_html=True,
)
st.title("Fraud Alerts")


@st.fragment(run_every=1)
def live():
    s = collector().snapshot()
    now = time.time()
    track_new(s["alerts"], now)

    if s["error"]:
        st.error(f"Waiting for Kafka at {BOOTSTRAP}: {s['error']}")

    if now < st.session_state.banner_until:
        st.markdown(f'<div class="critical" role="alert">{html.escape(st.session_state.banner_text)}</div>',
                    unsafe_allow_html=True)

    r = s["by_rule"]
    cols = st.columns(6)
    cols[0].metric("Transactions", f"{s['tx_total']:,}")
    cols[1].metric("Tx / sec", f"{s['tx_per_sec']:.1f}")
    cols[2].metric("Alerts", f"{s['alert_total']:,}")
    cols[3].metric("Impossible travel", f"{r.get('impossible_travel', 0):,}")
    cols[4].metric("Velocity", f"{r.get('velocity', 0):,}")
    cols[5].metric("Amount spike", f"{r.get('amount_spike', 0):,}")

    st.subheader("Alert map")
    # scrollZoom off: otherwise scrolling the page over the map zooms the map instead
    st.plotly_chart(make_map(s["alerts"][:80], now), width="stretch", key="map",
                    config={"scrollZoom": False, "displayModeBar": False})

    st.subheader("Latest alerts")
    if s["alerts"]:
        st.dataframe(make_table(s["alerts"], now), hide_index=True, width="stretch", height=380,
                     column_config={"Amount": st.column_config.NumberColumn(format="$%.2f")})
    else:
        st.info("No alerts yet. Start the producer and detector.")


live()
