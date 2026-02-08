from __future__ import annotations

import datetime as dt

import streamlit as st

st.title("Tax Year")
st.caption("Taxable gain/loss summary with wash-sale awareness.")

year = st.selectbox("Tax year", [dt.date.today().year, dt.date.today().year - 1])
st.write(f"Preparing 8949-like details for {year}.")
