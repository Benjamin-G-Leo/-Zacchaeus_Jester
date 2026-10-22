"""Fraud monitor. A dark, minimal Streamlit dashboard.

    streamlit run dashboard/app.py

Data path: one daemon thread (fraud/collector.py) owns the KafkaConsumer and hands
alerts to this page through a queue.Queue. This script never touches Kafka. It
draws one frame, sleeps a second, and reruns itself.
"""
import html
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # so `fraud` imports

import plotly.graph_objects as go
import streamlit as st

from fraud import rules as R
from fraud.collector import Collector
from fraud.common import BOOTSTRAP, CITIES

LABEL = {"impossible_travel": "Impossible travel", "velocity": "Velocity", "amount_spike": "Amount spike"}
COLOR = {"impossible_travel": "#ef4444", "velocity": "#f59e0b", "amount_spike": "#3b82f6"}
CSS_CLASS = {"impossible_travel": "c-red", "velocity": "c-amber", "amount_spike": "c-blue"}
FRESH_S = 3  # an alert is "new" on screen for this long
BANNER_S = 6
ROWS_PER_COLUMN = 12
SEVERITY = {"impossible_travel": "Critical", "velocity": "High", "amount_spike": "Medium"}
FRAME_S = 1.0
MAP_EVERY_S = 3
PULSE_S = 1.6  # keep in sync with the CSS pulse animation

CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Geist+Mono:wght@400;500&family=Inter:wght@400;500;600&display=swap');

:root {
  --bg: #09090b; --card: #18181b; --line: #27272a; --line-soft: #1f1f23;
  --text: #fafafa; --muted: #a1a1aa; --faint: #71717a;
  --red: #ef4444; --amber: #f59e0b; --blue: #3b82f6; --green: #22c55e;
  --sans: 'Inter', -apple-system, BlinkMacSystemFont, 'SF Pro Text', 'Segoe UI', sans-serif;
  --mono: 'Geist Mono', ui-monospace, 'SF Mono', Menlo, monospace;
}

/* page shell: drop Streamlit's own chrome */
html, body, [data-testid="stApp"], [data-testid="stAppViewContainer"] { background: var(--bg) !important; }
[data-testid="stApp"] * { font-family: var(--sans); }
header[data-testid="stHeader"], #MainMenu, footer, [data-testid="stStatusWidget"],
[data-testid="stToolbar"], [data-testid="stDecoration"] { display: none !important; }
.block-container { max-width: 1080px !important; padding: 2.2rem 1.25rem 3rem !important; }
/* the page redraws every second; never fade the old frame while the next one arrives */
[data-stale="true"], .stale-element { opacity: 1 !important; transition: none !important; }
[data-testid="stVerticalBlock"] { gap: 0.9rem !important; }

/* header */
.top { display: flex; align-items: center; justify-content: space-between; margin-bottom: 4px; }
.brand { display: flex; align-items: baseline; gap: 10px; }
.brand h1 { font-size: 15px; font-weight: 600; letter-spacing: -0.01em; color: var(--text); margin: 0; padding: 0; }
.brand span { font-size: 13px; color: var(--faint); }
.chip { display: inline-flex; align-items: center; gap: 7px; font-size: 12px; color: var(--muted);
        border: 1px solid var(--line); border-radius: 999px; padding: 3px 10px; background: var(--card); }

/* status dots */
.dot { display: inline-block; width: 7px; height: 7px; border-radius: 50%; flex: none; background: var(--faint); }
.dot.c-red { background: var(--red); } .dot.c-amber { background: var(--amber); }
.dot.c-blue { background: var(--blue); } .dot.c-green { background: var(--green); }
.dot.pulse.c-red { animation: ping-red 1.6s ease-out infinite; }
.dot.pulse.c-green { animation: ping-green 1.6s ease-out infinite; }
@keyframes ping-red { 0% { box-shadow: 0 0 0 0 rgba(239,68,68,.55); } 80%,100% { box-shadow: 0 0 0 8px rgba(239,68,68,0); } }
@keyframes ping-green { 0% { box-shadow: 0 0 0 0 rgba(34,197,94,.5); } 80%,100% { box-shadow: 0 0 0 8px rgba(34,197,94,0); } }

/* metric badges */
.badges { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 10px; }
.badge { background: var(--card); border: 1px solid var(--line); border-radius: 8px; padding: 12px 14px; min-height: 84px; }
.badge .label { display: flex; align-items: center; gap: 8px; font-size: 12px; color: var(--muted); }
.badge .value { font-family: var(--mono); font-size: 24px; font-weight: 500; color: var(--text);
                letter-spacing: -0.02em; margin-top: 8px; font-variant-numeric: tabular-nums; }
.badge .sub { font-family: var(--mono); font-size: 11px; color: var(--faint); margin-left: 6px; letter-spacing: 0; }
.badge .spark { display: block; margin-top: 4px; }

/* section titles */
.section { display: flex; align-items: center; justify-content: space-between; margin: 10px 2px 0;
           font-size: 12px; font-weight: 500; color: var(--muted); }
.section .count { font-family: var(--mono); color: var(--faint); }

/* critical banner */
.banner { display: flex; align-items: center; gap: 10px; border: 1px solid rgba(239,68,68,.45);
          background: rgba(239,68,68,.08); border-radius: 8px; padding: 10px 14px; font-size: 13px; color: var(--text); }
.banner b { color: var(--red); font-weight: 600; }
.banner .mono { font-family: var(--mono); font-size: 12px; color: var(--muted); }

/* three alert lists: one card per rule, every row is a button */
[class*="st-key-list_"] { background: var(--card); border: 1px solid var(--line); border-radius: 8px;
                          overflow: hidden; gap: 0 !important; }
[class*="st-key-list_"] [data-testid="stVerticalBlock"] { gap: 0 !important; }
.col-head { display: flex; align-items: center; gap: 8px; padding: 11px 14px; font-size: 12.5px; font-weight: 500;
            color: var(--text); border-bottom: 1px solid var(--line); }
.col-head .count { margin-left: auto; font-family: var(--mono); font-size: 11.5px; color: var(--faint); font-weight: 400; }
[class*="st-key-row_"] { margin: 0 !important; }
[class*="st-key-row_"] button { display: flex; justify-content: flex-start; width: 100%; min-height: 0; padding: 9px 14px;
        border: 0; border-bottom: 1px solid var(--line-soft); border-radius: 0; background: transparent;
        color: var(--muted); font-size: 13px; text-align: left; }
[class*="st-key-row_"] button:hover { background: #1f1f23; color: var(--text); }
[class*="st-key-row_"] button:focus-visible { outline: 1px solid var(--blue); outline-offset: -1px; }
[class*="st-key-row_"] button > div { width: 100%; }
[class*="st-key-row_"] button p { margin: 0; text-align: left; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
[class*="st-key-row_"] button span, [class*="st-key-row_"] button div { min-width: 0; }
[class*="st-key-row_"] button code { white-space: nowrap !important; word-break: keep-all !important; }
[class*="st-key-row_"] button strong { color: var(--text); font-weight: 500; }
[class*="st-key-row_"] button code { font-family: var(--mono); font-size: 11.5px; color: var(--muted); background: #27272a;
        border-radius: 4px; padding: 1px 6px; }
.empty { padding: 30px 14px; text-align: center; color: var(--faint); font-size: 13px; }

/* detail dialog */
[data-testid="stDialog"] div[role="dialog"] { background: var(--bg); border: 1px solid var(--line); border-radius: 12px; }
.d-head { display: flex; align-items: center; gap: 10px; margin-bottom: 6px; }
.d-head .t { font-size: 17px; font-weight: 600; color: var(--text); letter-spacing: -0.01em; }
.sev { font-size: 11px; font-weight: 500; border-radius: 999px; padding: 2px 9px; border: 1px solid var(--line); color: var(--muted); }
.sev.c-red { color: var(--red); border-color: rgba(239,68,68,.45); background: rgba(239,68,68,.08); }
.sev.c-amber { color: var(--amber); border-color: rgba(245,158,11,.45); background: rgba(245,158,11,.08); }
.sev.c-blue { color: var(--blue); border-color: rgba(59,130,246,.45); background: rgba(59,130,246,.08); }
.why { font-size: 13.5px; line-height: 1.55; color: var(--muted); margin: 6px 0 14px; }
.why b { color: var(--text); font-weight: 500; }
.kv { display: grid; grid-template-columns: 150px 1fr; border: 1px solid var(--line); border-radius: 8px;
      overflow: hidden; background: var(--card); }
.kv > div { padding: 8px 14px; font-size: 13px; border-bottom: 1px solid var(--line-soft); }
.kv > div:nth-last-child(-n+2) { border-bottom: 0; }
.kv .k { color: var(--faint); }
.kv .v { color: var(--text); font-family: var(--mono); font-size: 12.5px; }
.kv .v small { color: var(--faint); font-family: var(--sans); font-size: 12px; }
.sub-h { font-size: 12px; font-weight: 500; color: var(--muted); margin: 16px 2px 6px; }
.rel { display: flex; gap: 10px; align-items: center; padding: 7px 2px; font-size: 12.5px; color: var(--muted);
       border-bottom: 1px solid var(--line-soft); }
.rel:last-child { border-bottom: 0; }
.rel b { color: var(--text); font-weight: 500; }
.rel .when { margin-left: auto; font-family: var(--mono); font-size: 11.5px; color: var(--faint); white-space: nowrap; }

/* map: a bordered module like the other cards */
[data-testid="stPlotlyChart"] { border: 1px solid var(--line); border-radius: 8px; overflow: hidden; background: var(--bg); }

@media (max-width: 640px) { .badge .value { font-size: 20px; } .kv { grid-template-columns: 110px 1fr; } }
@media (prefers-reduced-motion: reduce) { .dot.pulse { animation: none !important; } }
</style>
"""


@st.cache_resource
def collector():
    return Collector()


def track_new(alerts, now):
    """Note when each alert first appeared on screen, and raise the banner for new travel alerts."""
    ss = st.session_state
    ss.setdefault("first_seen", {})
    ss.setdefault("banner_until", 0.0)
    ss.setdefault("banner", "")
    primed = ss.get("primed", False)
    for a in reversed(alerts):  # oldest first, so the banner ends on the newest
        k = f"{a['tx_id']}|{a['rule']}"
        if k in ss.first_seen:
            continue
        # alerts already there when the page first opens are history, not news
        ss.first_seen[k] = now if primed else 0.0
        if primed and a["rule"] == "impossible_travel":
            ss.banner = (a["user_id"], a["detail"])
            ss.banner_until = now + BANNER_S
    ss.primed = True
    live = {f"{a['tx_id']}|{a['rule']}" for a in alerts}
    for k in [k for k in ss.first_seen if k not in live]:  # forget alerts that scrolled off
        del ss.first_seen[k]


def age_of(a, now):
    seen = st.session_state.first_seen.get(f"{a['tx_id']}|{a['rule']}", 0.0)
    return now - seen if seen else None


def is_fresh(a, now):
    age = age_of(a, now)
    return age is not None and age < FRESH_S


def phase(now):
    """Negative delay keeps a pulsing dot in step even though the page is redrawn every second."""
    return f"animation-delay:-{now % PULSE_S:.2f}s"


def ago(seconds):
    seconds = max(0, int(seconds))
    if seconds < 2:
        return "now"
    if seconds < 60:
        return f"{seconds}s ago"
    return f"{seconds // 60}m ago"


def sparkline(values, w=132, h=26):
    top = max(max(values), 1)
    n = len(values) - 1
    pts = " ".join(f"{i * w / n:.1f},{h - 2 - (v / top) * (h - 5):.1f}" for i, v in enumerate(values))
    return (f'<svg class="spark" width="{w}" height="{h}" viewBox="0 0 {w} {h}" aria-hidden="true">'
            f'<polyline points="{pts}" fill="none" stroke="#71717a" stroke-width="1.4" '
            f'stroke-linejoin="round" stroke-linecap="round"/></svg>')


def badge(label, value, dot, now, sub="", extra="", pulse=False):
    style = f' style="{phase(now)}"' if pulse else ""
    cls = f"dot {dot}{' pulse' if pulse else ''}"
    sub = f'<span class="sub">{sub}</span>' if sub else ""
    return (f'<div class="badge"><div class="label"><span class="{cls}"{style}></span>{label}</div>'
            f'<div class="value">{value}{sub}</div>{extra}</div>')


def header_html(snap, now):
    if snap["error"]:
        return (f'<div class="top"><div class="brand"><h1>Fraud monitor</h1></div>'
                f'<span class="chip"><span class="dot c-amber"></span>Waiting for Kafka at {html.escape(BOOTSTRAP)}</span></div>')
    return (f'<div class="top"><div class="brand"><h1>Fraud monitor</h1><span>transactions · fraud_alerts</span></div>'
            f'<span class="chip"><span class="dot c-green pulse" style="{phase(now)}"></span>Live</span></div>')


def badges_html(snap, now):
    r = snap["by_rule"]
    active = snap["active_alerts"]
    cells = [
        badge("Transactions", f"{snap['tx_total']:,}", "c-green", now),
        badge("Throughput", f"{snap['tx_per_sec']:.0f}", "", now, sub="tx/s", extra=sparkline(snap["tx_history"])),
        badge("Active anomalies", f"{active:,}", "c-red" if active else "", now, sub="last 60s", pulse=bool(active)),
        badge("Impossible travel", f"{r.get('impossible_travel', 0):,}", "c-red", now),
        badge("Velocity", f"{r.get('velocity', 0):,}", "c-amber", now),
        badge("Amount spike", f"{r.get('amount_spike', 0):,}", "c-blue", now),
    ]
    return f'<div class="badges">{"".join(cells)}</div>'


def banner_html(now):
    ss = st.session_state
    if now >= ss.banner_until:
        return ""
    user, detail = ss.banner
    return (f'<div class="banner" role="alert"><span class="dot c-red pulse" style="{phase(now)}"></span>'
            f'<b>Critical</b><span>Impossible travel</span>'
            f'<span class="mono">{html.escape(user)} · {html.escape(detail.replace("->", "→"))}</span></div>')


def esc_md(text):
    """Dollar signs would start LaTeX in a Streamlit label."""
    return text.replace("$", "\\$")


def plain(text, limit=24):
    """Labels are Markdown. Keep letters, digits, spaces, dots and dashes so wire text can't add links or images."""
    return re.sub(r"[^\w .\-]", "", str(text))[:limit]


def row_label(a, now):
    new = ":green[●] " if is_fresh(a, now) else ""
    rule = a["rule"]
    if rule == "impossible_travel":
        main = f"**{plain(a.get('from_city', '?'))} → {plain(a['city'])}**"
    elif rule == "velocity":
        main = f"**{a.get('count', R.VELOCITY_MAX_TX):g} tx in {a.get('window_s', R.VELOCITY_WINDOW_S):g}s**"
    else:
        main = f"**${a['amount']:,.0f}**" + (f" vs ${a['typical']:,.0f}" if "typical" in a else "")
    return esc_md(f"{new}{main} `{plain(a['user_id'])}` · {ago(now - a['seen_at'])}")


def open_detail(alert):
    st.session_state.selected = alert


def clear_selection():
    st.session_state.selected = None


def alert_columns(snap, now):
    """One card per rule. Every row is a button; clicking one opens its detail dialog."""
    for col, rule in zip(st.columns(3, gap="small"), LABEL):
        with col, st.container(key=f"list_{rule}"):
            st.markdown(
                f'<div class="col-head"><span class="dot {CSS_CLASS[rule]}"></span>{LABEL[rule]}'
                f'<span class="count">{snap["by_rule"].get(rule, 0):,}</span></div>',
                unsafe_allow_html=True,
            )
            seen, rows = set(), []
            for a in snap["alerts"]:  # newest first
                k = f"{a['rule']}_{a['tx_id']}"
                if a["rule"] == rule and k not in seen:  # a replayed alert must not repeat a widget key
                    seen.add(k)
                    rows.append((k, a))
                    if len(rows) == ROWS_PER_COLUMN:
                        break
            if not rows:
                st.markdown('<div class="empty">None yet</div>', unsafe_allow_html=True)
            for k, a in rows:
                # on_click runs before the next frame, so the click is kept even if the row has scrolled away
                st.button(row_label(a, now), key=f"row_{k}", type="tertiary", width="stretch",
                          on_click=open_detail, args=(a,))


def fmt_duration(seconds):
    seconds = round(seconds)
    return f"{seconds // 60}m {seconds % 60:02d}s" if seconds >= 60 else f"{seconds}s"


def has(a, *keys):
    return all(k in a for k in keys)


def detail_rows(a):
    """(label, html) pairs for the detail table.

    Every value is escaped, because it comes off the wire (an unescaped user id once
    became live HTML in the popup). Only the small gray notes are our own markup.
    Older alerts may lack the extra fields.
    """

    def v(text, note=""):
        out = html.escape(str(text))
        return f"{out} <small>{html.escape(note)}</small>" if note else out

    rows = [
        ("User", v(a["user_id"])),
        ("Transaction", v(a["tx_id"])),
        ("Amount", v(f"${a['amount']:,.2f}")),
        ("City", v(a["city"])),
        ("Received", v(time.strftime("%H:%M:%S", time.localtime(a["seen_at"])))),
    ]
    rule = a["rule"]
    if rule == "impossible_travel":
        rows += [("From", v(a.get("from_city", "unknown"))), ("To", v(a["city"]))]
        if has(a, "distance_km", "elapsed_s", "speed_kmh"):
            rows += [
                ("Distance", v(f"{a['distance_km']:,.0f} km")),
                ("Time between swipes", v(fmt_duration(a["elapsed_s"]))),
                ("Implied speed", v(f"{a['speed_kmh']:,.0f} km/h", f"(limit {R.MAX_SPEED_KMH})")),
            ]
        if a["ts"] > a["seen_at"] + 5:
            rows.append(("Event time", v(time.strftime("%H:%M:%S", time.localtime(a["ts"])),
                                         "(simulated, ahead of the clock)")))
    elif rule == "velocity" and has(a, "count", "window_s"):
        rows += [("Transactions", v(f"{a['count']:g}", f"(alert at {R.VELOCITY_MAX_TX}+)")),
                 ("Window", v(f"{a['window_s']:g} seconds"))]
    elif rule == "amount_spike" and has(a, "typical", "limit", "multiple", "samples"):
        rows += [
            ("Typical purchase", v(f"${a['typical']:,.2f}", f"(from {a['samples']:g} purchases)")),
            ("This purchase", v(f"{a['multiple']}x typical")),
            ("Alert threshold", v(f"${a['limit']:,.2f}", f"(mean + {R.AMOUNT_SIGMAS} std dev)")),
        ]
    return rows


def why_text(a):
    rule = a["rule"]
    user = html.escape(a["user_id"])
    if rule == "impossible_travel" and has(a, "distance_km", "elapsed_s", "speed_kmh", "from_city"):
        return (f"<b>{user}</b> paid in <b>{html.escape(a['from_city'])}</b> and then in "
                f"<b>{html.escape(a['city'])}</b>, {a['distance_km']:,.0f} km apart, {fmt_duration(a['elapsed_s'])} later. "
                f"That needs about <b>{a['speed_kmh']:,.0f} km/h</b>. Even a plane tops out near {R.MAX_SPEED_KMH} km/h.")
    if rule == "velocity" and has(a, "count", "window_s"):
        return (f"<b>{user}</b> made <b>{a['count']:g} transactions</b> inside {a['window_s']:g} seconds. "
                f"The rule fires at {R.VELOCITY_MAX_TX} in a sliding window, which looks like a stolen card being used in a burst.")
    if rule == "amount_spike" and has(a, "typical", "limit", "multiple", "samples"):
        return (f"<b>${a['amount']:,.2f}</b> is <b>{a['multiple']}x</b> what {user} normally spends "
                f"(${a['typical']:,.2f}). It is above their personal threshold of ${a['limit']:,.2f}, and at least "
                f"{R.AMOUNT_MIN_RATIO}x their average.")
    return html.escape(a["detail"].replace("->", "→"))


@st.dialog("Alert detail", width="large", on_dismiss=clear_selection)
def detail_dialog(a, recent, now):
    rule = a["rule"]
    st.markdown(
        f'<div class="d-head"><span class="dot {CSS_CLASS[rule]}"></span><span class="t">{LABEL[rule]}</span>'
        f'<span class="sev {CSS_CLASS[rule]}">{SEVERITY[rule]}</span></div>'
        f'<div class="why">{why_text(a)}</div>'
        f'<div class="kv">{"".join(f"<div class=k>{k}</div><div class=v>{v}</div>" for k, v in detail_rows(a))}</div>',
        unsafe_allow_html=True,
    )
    st.plotly_chart(make_map([a], now, height=240, focus=True), width="stretch", key="detail_map",
                    config={"scrollZoom": False, "displayModeBar": False})

    others = [o for o in recent if o["user_id"] == a["user_id"] and o["tx_id"] != a["tx_id"]][:5]
    if others:
        items = "".join(
            f'<div class="rel"><span class="dot {CSS_CLASS[o["rule"]]}"></span><b>{LABEL[o["rule"]]}</b>'
            f'<span>{html.escape(o["detail"].replace("->", "→"))}</span>'
            f'<span class="when">{ago(now - o["seen_at"])}</span></div>'
            for o in others
        )
        st.markdown(f'<div class="sub-h">Other recent alerts for this user</div>{items}', unsafe_allow_html=True)
    with st.expander("Raw event"):
        st.json(a)
    if st.button("Close", key="close_detail"):
        clear_selection()
        st.rerun()  # a full rerun closes the dialog


def make_map(alerts, now, height=380, focus=False):
    # an empty map trace up front: with no alerts yet Plotly would otherwise draw plain x/y axes
    fig = go.Figure(go.Scattergeo(lat=[None], lon=[None], hoverinfo="skip", showlegend=False))
    lat, lon = [], []
    for a in alerts:
        if a.get("from_city") in CITIES and a["city"] in CITIES:
            (la1, lo1), (la2, lo2) = CITIES[a["from_city"]], CITIES[a["city"]]
            lat += [la1, la2, None]
            lon += [lo1, lo2, None]
    if lat:
        fig.add_trace(go.Scattergeo(lat=lat, lon=lon, mode="lines", hoverinfo="skip", showlegend=False,
                                    line=dict(width=1, color=COLOR["impossible_travel"]), opacity=0.35))
    for rule, label in LABEL.items():
        pts = [a for a in alerts if a["rule"] == rule and a["city"] in CITIES]
        if not pts:
            continue
        fresh = [is_fresh(a, now) for a in pts]
        fig.add_trace(go.Scattergeo(
            lat=[CITIES[a["city"]][0] for a in pts], lon=[CITIES[a["city"]][1] for a in pts],
            mode="markers", name=label,
            marker=dict(color=COLOR[rule], opacity=0.85, size=[14 if f else 7 for f in fresh],
                        line=dict(width=[2 if f else 0 for f in fresh], color="#fafafa")),
            text=[f"{a['user_id']} · {a['city']}<br>{a['detail']}" for a in pts],
            hovertemplate="%{text}<extra>" + label + "</extra>",
        ))
    fig.update_geos(projection_type="natural earth", showland=True, landcolor="#18181b", showocean=True,
                    oceancolor="#09090b", showcountries=True, countrycolor="#27272a", coastlinecolor="#3f3f46",
                    showframe=False, bgcolor="rgba(0,0,0,0)", fitbounds=False)
    if focus:  # detail view: frame the alert's own cities, with padding, instead of the whole world
        names = {c for a in alerts for c in (a["city"], a.get("from_city")) if c in CITIES}
        lats = [CITIES[c][0] for c in names]
        lons = [CITIES[c][1] for c in names]
        lat_lo, lat_hi = max(-85, min(lats) - 14), min(85, max(lats) + 14)
        # make the window as wide as the card (about 2.4 : 1) so the map fills it, never narrower than the cities
        half = max((lat_hi - lat_lo) * 1.2, (max(lons) - min(lons)) / 2 + 22)
        mid = (min(lons) + max(lons)) / 2
        fig.update_geos(lataxis_range=[lat_lo, lat_hi], lonaxis_range=[max(-180, mid - half), min(180, mid + half)])
        fig.add_trace(go.Scattergeo(  # name both ends; the alert's own marker only marks the destination
            lat=[CITIES[c][0] for c in sorted(names)], lon=[CITIES[c][1] for c in sorted(names)],
            text=sorted(names), mode="text", textposition="top right", hoverinfo="skip", showlegend=False,
            textfont=dict(color="#a1a1aa", size=11)))
        for a in alerts:
            if a.get("from_city") in CITIES:
                fig.add_trace(go.Scattergeo(
                    lat=[CITIES[a["from_city"]][0]], lon=[CITIES[a["from_city"]][1]], mode="markers",
                    hoverinfo="skip", showlegend=False,
                    marker=dict(size=9, color="rgba(0,0,0,0)", line=dict(width=2, color=COLOR[a["rule"]]))))
    fig.update_layout(height=height, margin=dict(l=0, r=0, t=0, b=0), paper_bgcolor="rgba(0,0,0,0)",
                      font=dict(family="Inter, sans-serif", size=11, color="#a1a1aa"),
                      legend=dict(orientation="h", y=0.03, x=0.015, bgcolor="rgba(9,9,11,0.7)",
                                  bordercolor="#27272a", borderwidth=1))
    return fig


st.set_page_config(page_title="Fraud monitor", page_icon="🛡️", layout="wide")
st.markdown(CSS, unsafe_allow_html=True)

snap = collector().snapshot()
now = time.time()
track_new(snap["alerts"], now)

st.markdown(header_html(snap, now), unsafe_allow_html=True)
if now < st.session_state.banner_until:
    st.markdown(banner_html(now), unsafe_allow_html=True)
st.markdown(badges_html(snap, now), unsafe_allow_html=True)

st.markdown('<div class="section">Alert map</div>', unsafe_allow_html=True)
# Plotly remounts whenever its figure changes. Rebuilding it every frame made the map
# redraw each second and reset hover tooltips, so rebuild every few seconds and hand
# over the identical figure in between.
ss = st.session_state
if "map_fig" not in ss or now - ss.map_at >= MAP_EVERY_S:
    ss.map_fig, ss.map_at = make_map(snap["alerts"][:80], now), now
# scrollZoom off, or scrolling the page over the map zooms the map instead
st.plotly_chart(ss.map_fig, width="stretch", key="map", config={"scrollZoom": False, "displayModeBar": False})

st.markdown(f'<div class="section">Live alerts<span class="count">{snap["alert_total"]:,} total</span></div>',
            unsafe_allow_html=True)
alert_columns(snap, now)

if ss.get("selected"):
    detail_dialog(ss.selected, snap["alerts"], now)
else:
    # Frame throttle: draw once, wait a second, redraw. The wait is ten 0.1s sleeps with a
    # trivial Streamlit call between them, because Streamlit only notices a click at its next
    # call, so a click is answered within 0.1s instead of up to a full second.
    # While a detail dialog is open the loop stops, which also freezes the lists for reading.
    tick = st.empty()
    for _ in range(10):
        time.sleep(FRAME_S / 10)
        tick.empty()
    st.rerun()
