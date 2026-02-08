from __future__ import annotations

from datetime import date

import streamlit as st

from portfolio_assistant.ui.streamlit.helpers import (
    build_snapshot,
    ensure_initialized,
    render_account_scope_selector,
)


ensure_initialized()

st.title("Calendar")
st.caption("Daily realized P&L calendar and details.")

account_scope = render_account_scope_selector()
snapshot = build_snapshot(account_scope)

if not snapshot.realized_rows:
    st.info("No realized trades available yet.")
    st.stop()

daily_totals: dict[date, float] = {}
for row in snapshot.realized_rows:
    day = row.closed_at.date()
    daily_totals[day] = daily_totals.get(day, 0.0) + row.realized_pnl

daily_rows = [
    {"date": day.isoformat(), "realized_pnl": round(total, 2)}
    for day, total in sorted(daily_totals.items())
]

st.markdown("### Daily Details")
st.dataframe(daily_rows, use_container_width=True)

st.markdown("### Calendar Heatmap (Week x Weekday)")
weekdays = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
heatmap: dict[str, dict[str, float]] = {}
for day, total in daily_totals.items():
    week_key = f"{day.year}-W{day.isocalendar().week:02d}"
    heatmap.setdefault(week_key, {name: 0.0 for name in weekdays})
    heatmap[week_key][weekdays[day.weekday()]] += total

heatmap_rows = []
for week_key in sorted(heatmap):
    row = {"week": week_key}
    row.update({name: round(heatmap[week_key][name], 2) for name in weekdays})
    heatmap_rows.append(row)

st.dataframe(heatmap_rows, use_container_width=True)
