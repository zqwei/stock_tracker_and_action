from __future__ import annotations

import streamlit as st

st.title("Reconciliation")
st.caption("Compare local computed totals against broker-reported totals.")

st.dataframe(
    [
        {
            "metric": "total_taxable_gain_loss",
            "local": 0.0,
            "broker": 0.0,
            "difference": 0.0,
            "within_tolerance": True,
        }
    ],
    use_container_width=True,
)
