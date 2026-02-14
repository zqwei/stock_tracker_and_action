from __future__ import annotations

import streamlit as st

UI_THEME_SESSION_KEY = "ui_theme_preset"
DEFAULT_THEME_PRESET = "deep_dark"

THEME_PRESETS: dict[str, dict[str, str]] = {
    "bright": {
        "bg_0": "#edf4ff",
        "bg_1": "#e2ecff",
        "bg_2": "#d4e1f7",
        "surface": "rgba(255, 255, 255, 0.98)",
        "sidebar_bg": "rgba(228, 238, 253, 0.98)",
        "header_bg": "rgba(239, 246, 255, 0.96)",
        "border": "rgba(23, 72, 116, 0.40)",
        "border_strong": "rgba(7, 89, 186, 0.86)",
        "text_strong": "#071e34",
        "text": "#0f3454",
        "text_muted": "#2b557a",
        "accent": "#0b6ce3",
        "accent_2": "#0f8c85",
        "glow_1": "rgba(11, 108, 227, 0.18)",
        "glow_2": "rgba(15, 140, 133, 0.12)",
        "elev_shadow": "rgba(14, 36, 64, 0.16)",
        "button_bg": "#dbe9ff",
        "button_bg_hover": "#cfe2ff",
        "button_border": "#2e73bf",
        "button_text": "#0a2d4d",
        "button_shadow": "rgba(24, 61, 95, 0.20)",
    },
    "dark": {
        "bg_0": "#0b131d",
        "bg_1": "#121f2e",
        "bg_2": "#17293d",
        "surface": "rgba(25, 40, 57, 0.9)",
        "sidebar_bg": "rgba(11, 18, 28, 0.95)",
        "header_bg": "rgba(11, 18, 28, 0.92)",
        "border": "rgba(148, 184, 212, 0.3)",
        "border_strong": "rgba(86, 236, 184, 0.62)",
        "text_strong": "#f2fbff",
        "text": "#d2e4f3",
        "text_muted": "#9fb8cd",
        "accent": "#56ecb8",
        "accent_2": "#30c8ff",
        "glow_1": "rgba(48, 200, 255, 0.16)",
        "glow_2": "rgba(86, 236, 184, 0.14)",
        "elev_shadow": "rgba(0, 0, 0, 0.42)",
        "button_bg": "#163446",
        "button_bg_hover": "#1d435a",
        "button_border": "#2ea7d5",
        "button_text": "#e8f5ff",
        "button_shadow": "rgba(0, 0, 0, 0.34)",
    },
    "deep_dark": {
        "bg_0": "#000000",
        "bg_1": "#000000",
        "bg_2": "#000000",
        "surface": "rgba(12, 12, 12, 0.94)",
        "sidebar_bg": "rgba(0, 0, 0, 0.98)",
        "header_bg": "rgba(0, 0, 0, 0.96)",
        "border": "rgba(120, 128, 143, 0.32)",
        "border_strong": "rgba(78, 225, 255, 0.72)",
        "text_strong": "#f8fbff",
        "text": "#d9e2f0",
        "text_muted": "#9aa8bc",
        "accent": "#4ee1ff",
        "accent_2": "#78f4c4",
        "glow_1": "rgba(0, 0, 0, 0.0)",
        "glow_2": "rgba(0, 0, 0, 0.0)",
        "elev_shadow": "rgba(0, 0, 0, 0.74)",
        "button_bg": "#131518",
        "button_bg_hover": "#1b1f24",
        "button_border": "#434b57",
        "button_text": "#eef3f9",
        "button_shadow": "rgba(0, 0, 0, 0.76)",
    },
    "palenight": {
        "bg_0": "#171726",
        "bg_1": "#21213a",
        "bg_2": "#2d2e4a",
        "surface": "rgba(40, 43, 72, 0.86)",
        "sidebar_bg": "rgba(22, 24, 42, 0.95)",
        "header_bg": "rgba(22, 24, 42, 0.92)",
        "border": "rgba(185, 176, 232, 0.32)",
        "border_strong": "rgba(176, 139, 255, 0.72)",
        "text_strong": "#f6f3ff",
        "text": "#e5deff",
        "text_muted": "#b7abd9",
        "accent": "#b08bff",
        "accent_2": "#78d2ff",
        "glow_1": "rgba(120, 210, 255, 0.16)",
        "glow_2": "rgba(176, 139, 255, 0.16)",
        "elev_shadow": "rgba(6, 8, 22, 0.52)",
        "button_bg": "#303555",
        "button_bg_hover": "#3a4066",
        "button_border": "#9a7eff",
        "button_text": "#f2ebff",
        "button_shadow": "rgba(11, 13, 36, 0.4)",
    },
}

THEME_LABELS = {
    "bright": "Bright",
    "dark": "Dark",
    "deep_dark": "Deep Dark",
    "palenight": "Palenight",
}

THEME_CSS_TEMPLATE = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;600;700;800&family=IBM+Plex+Mono:wght@400;500&display=swap');

:root {
__ROOT_VARS__
  --pa-danger: #ff8e7b;
}

.stApp {
  font-family: "Manrope", "Avenir Next", "Segoe UI", sans-serif;
  color: var(--pa-text);
  background:
    radial-gradient(circle at 18% 4%, var(--pa-glow_1), transparent 36%),
    radial-gradient(circle at 84% 0%, var(--pa-glow_2), transparent 34%),
    linear-gradient(160deg, var(--pa-bg_0) 0%, var(--pa-bg_1) 52%, var(--pa-bg_2) 100%);
}

[data-testid="stAppViewContainer"] > .main {
  animation: pa-fade-up 380ms ease-out;
}

[data-testid="stAppViewContainer"] > .main .block-container {
  padding-top: 1.2rem;
  padding-bottom: 2rem;
  max-width: 1240px;
}

[data-testid="stHeader"] {
  background: var(--pa-header_bg) !important;
  border-bottom: 1px solid color-mix(in oklab, var(--pa-border) 85%, transparent) !important;
}

[data-testid="stToolbar"] {
  background: transparent !important;
}

[data-testid="stDecoration"] {
  background: transparent !important;
}

[data-testid="stSidebar"] {
  background: var(--pa-sidebar_bg);
  border-right: 1px solid color-mix(in oklab, var(--pa-border) 82%, transparent);
  box-shadow: inset -1px 0 0 color-mix(in oklab, var(--pa-border) 35%, transparent);
}

[data-testid="stSidebar"] .block-container {
  padding-top: 1rem;
}

[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p {
  color: var(--pa-text_muted);
}

[data-testid="stSidebar"] [data-testid="stWidgetLabel"] p {
  color: var(--pa-text_strong) !important;
  font-weight: 700 !important;
  letter-spacing: 0.01em;
}

[data-testid="stSidebarNav"] a {
  color: var(--pa-text) !important;
  border-radius: 12px !important;
  border: 1px solid transparent !important;
  margin: 1px 0 !important;
  background: transparent !important;
}

[data-testid="stSidebarNav"] a:hover {
  border-color: color-mix(in oklab, var(--pa-border) 75%, transparent) !important;
  background: color-mix(in oklab, var(--pa-surface) 94%, var(--pa-bg_0)) !important;
}

[data-testid="stSidebarNav"] a[aria-current="page"] {
  border-color: var(--pa-border_strong) !important;
  background: color-mix(in oklab, var(--pa-accent) 20%, var(--pa-surface)) !important;
}

[data-testid="stSidebarNav"] a span {
  color: var(--pa-text) !important;
}

[data-testid="stSidebarNav"] a[aria-current="page"] span {
  color: var(--pa-text_strong) !important;
  font-weight: 700 !important;
}

h1, h2, h3, h4 {
  color: var(--pa-text_strong) !important;
  letter-spacing: 0.01em;
  font-weight: 700;
}

p, li, label {
  color: var(--pa-text);
}

small, .stCaption {
  color: var(--pa-text_muted) !important;
}

code, pre, kbd {
  font-family: "IBM Plex Mono", "JetBrains Mono", monospace;
}

[data-testid="stSidebar"] [data-testid="stRadio"] > div {
  gap: 0.34rem;
}

[data-testid="stSidebar"] [data-testid="stRadio"] label[data-baseweb="radio"] {
  margin: 0;
  min-height: 2.3rem;
}

[data-testid="stSidebar"] [data-testid="stRadio"] label[data-baseweb="radio"] > div:first-child {
  display: none;
}

[data-testid="stSidebar"] [data-testid="stRadio"] label[data-baseweb="radio"] > div:last-child {
  width: 100%;
  border-radius: 14px;
  padding: 0.56rem 0.72rem;
  border: 1px solid color-mix(in oklab, var(--pa-border) 76%, transparent);
  background: color-mix(in oklab, var(--pa-surface) 90%, var(--pa-bg_0));
  box-shadow: inset 0 0 0 1px color-mix(in oklab, var(--pa-border) 35%, transparent);
  transition: border-color 140ms ease, box-shadow 140ms ease, background 140ms ease;
}

[data-testid="stSidebar"] [data-testid="stRadio"] label[data-baseweb="radio"]:hover > div:last-child {
  border-color: color-mix(in oklab, var(--pa-accent) 62%, var(--pa-border)) !important;
  background: color-mix(in oklab, var(--pa-surface) 94%, var(--pa-bg_1)) !important;
}

[data-testid="stSidebar"] [data-testid="stRadio"] label[data-baseweb="radio"] p {
  color: var(--pa-text);
  font-weight: 600;
  margin: 0;
  display: flex;
  align-items: center;
  gap: 0.42rem;
  line-height: 1.25;
}

[data-testid="stSidebar"] [data-testid="stRadio"] label[data-baseweb="radio"]:has(input:checked) > div:last-child {
  border-color: var(--pa-border_strong);
  background: linear-gradient(
    135deg,
    color-mix(in oklab, var(--pa-accent) 34%, transparent),
    color-mix(in oklab, var(--pa-accent_2) 26%, transparent)
  );
  box-shadow:
    inset 0 0 0 1px color-mix(in oklab, var(--pa-border_strong) 75%, transparent),
    0 10px 22px color-mix(in oklab, var(--pa-accent) 35%, transparent);
}

[data-testid="stSidebar"] [data-testid="stRadio"] label[data-baseweb="radio"]:has(input:checked) p {
  color: var(--pa-text_strong);
}

[data-testid="stSidebar"] div[data-baseweb="select"] > div {
  border-radius: 14px !important;
  min-height: 2.45rem;
}

[data-testid="stSidebar"] div[data-baseweb="select"] [role="combobox"] {
  font-weight: 600 !important;
  letter-spacing: 0.005em;
}

[data-testid="stMetric"],
[data-testid="stMetricValue"],
[data-testid="stMetricLabel"] {
  color: var(--pa-text_strong);
}

[data-testid="metric-container"] {
  background: var(--pa-surface);
  border: 1px solid var(--pa-border);
  border-radius: 16px;
  box-shadow: 0 10px 24px var(--pa-elev_shadow);
  padding: 0.86rem 1rem;
}

.stButton > button,
[data-testid="stDownloadButton"] > button,
[data-testid="stFormSubmitButton"] > button {
  border-radius: 999px;
  border: 1px solid var(--pa-button_border);
  background: var(--pa-button_bg);
  color: var(--pa-button_text);
  font-weight: 700;
  box-shadow: 0 8px 18px var(--pa-button_shadow);
  transition: transform 120ms ease, box-shadow 120ms ease, border-color 120ms ease, background 120ms ease;
}

.stButton > button:hover,
[data-testid="stDownloadButton"] > button:hover,
[data-testid="stFormSubmitButton"] > button:hover {
  transform: translateY(-1px);
  border-color: var(--pa-border_strong);
  background: var(--pa-button_bg_hover);
  box-shadow: 0 10px 20px var(--pa-button_shadow);
}

.stButton > button:disabled,
[data-testid="stDownloadButton"] > button:disabled,
[data-testid="stFormSubmitButton"] > button:disabled {
  opacity: 0.45;
  box-shadow: none;
}

a:focus-visible,
button:focus-visible,
[role="button"]:focus-visible,
input:focus-visible,
textarea:focus-visible,
[tabindex]:focus-visible {
  outline: 2px solid var(--pa-border_strong) !important;
  outline-offset: 2px !important;
  box-shadow: 0 0 0 3px color-mix(in oklab, var(--pa-accent) 34%, transparent) !important;
}

[data-testid="stDataFrame"], .stDataFrame {
  border: 1px solid color-mix(in oklab, var(--pa-border) 90%, transparent);
  border-radius: 12px;
  background: color-mix(in oklab, var(--pa-surface) 88%, transparent);
}

[data-testid="stForm"] {
  border-radius: 14px;
  border: 1px solid color-mix(in oklab, var(--pa-border) 80%, transparent);
  background: color-mix(in oklab, var(--pa-surface) 84%, transparent);
}

div[data-baseweb="select"] > div,
div[data-baseweb="input"] > div,
textarea {
  background: color-mix(in oklab, var(--pa-surface) 86%, transparent) !important;
  border: 1px solid color-mix(in oklab, var(--pa-border) 88%, transparent) !important;
  color: var(--pa-text_strong) !important;
  border-radius: 12px !important;
}

div[data-baseweb="select"] input,
div[data-baseweb="input"] input,
textarea {
  color: var(--pa-text_strong) !important;
}

div[data-baseweb="select"] > div:hover,
div[data-baseweb="input"] > div:hover,
textarea:hover {
  border-color: color-mix(in oklab, var(--pa-accent) 70%, transparent) !important;
}

div[data-baseweb="select"] > div:focus-within,
div[data-baseweb="input"] > div:focus-within,
textarea:focus {
  border-color: var(--pa-border_strong) !important;
  box-shadow: 0 0 0 1px color-mix(in oklab, var(--pa-accent) 40%, transparent) !important;
}

[data-testid="stAlert"] {
  border-radius: 12px;
  border: 1px solid color-mix(in oklab, var(--pa-border) 92%, transparent);
  background: color-mix(in oklab, var(--pa-surface) 82%, transparent);
}

[data-testid="stExpander"] {
  border: 1px solid color-mix(in oklab, var(--pa-border) 85%, transparent);
  border-radius: 12px;
  background: color-mix(in oklab, var(--pa-surface) 72%, transparent);
}

[data-testid="stFileUploaderDropzone"] {
  border: 1px dashed color-mix(in oklab, var(--pa-accent) 45%, var(--pa-border)) !important;
  border-radius: 14px !important;
  background: color-mix(in oklab, var(--pa-surface) 93%, var(--pa-bg_0)) !important;
}

[data-testid="stFileUploaderDropzone"] * {
  color: var(--pa-text_strong) !important;
}

[data-testid="stFileUploaderDropzone"] button {
  border-radius: 10px !important;
  border: 1px solid color-mix(in oklab, var(--pa-border_strong) 58%, transparent) !important;
  background: color-mix(in oklab, var(--pa-surface) 97%, transparent) !important;
  color: var(--pa-text_strong) !important;
}

[data-testid="stProgressBar"] > div > div > div {
  background: linear-gradient(90deg, var(--pa-accent_2), var(--pa-accent));
}

[data-baseweb="tab-list"] {
  gap: 0.4rem;
}

[data-baseweb="tab"] {
  color: var(--pa-text_muted) !important;
  border: 1px solid color-mix(in oklab, var(--pa-border) 68%, transparent) !important;
  border-radius: 999px !important;
  padding: 0.34rem 0.84rem !important;
  background: color-mix(in oklab, var(--pa-surface) 82%, transparent) !important;
}

[data-baseweb="tab"][aria-selected="true"] {
  color: var(--pa-text_strong) !important;
  border-color: var(--pa-accent) !important;
  background: color-mix(in oklab, var(--pa-accent) 16%, var(--pa-surface)) !important;
}

button[aria-label^="Help for"] svg {
  color: color-mix(in oklab, var(--pa-accent_2) 72%, white) !important;
}

@keyframes pa-fade-up {
  from {
    opacity: 0.0;
    transform: translateY(8px);
  }
  to {
    opacity: 1.0;
    transform: translateY(0);
  }
}
</style>
"""

BRIGHT_THEME_CSS = """
<style>
[data-testid="stSidebar"] {
  background: linear-gradient(180deg, #eaf1fd 0%, #e2ebfb 100%) !important;
}

[data-testid="stSidebar"] [data-testid="stSidebarNav"] a {
  border-color: #b4cae6 !important;
  background: rgba(255, 255, 255, 0.66) !important;
}

[data-testid="stSidebar"] [data-testid="stSidebarNav"] a span {
  color: #0d3659 !important;
  font-weight: 600 !important;
}

[data-testid="stSidebar"] [data-testid="stRadio"] label[data-baseweb="radio"] > div:last-child {
  background: #f6fbff !important;
  border-color: #9ebadc !important;
}

[data-testid="stSidebar"] [data-testid="stRadio"] label[data-baseweb="radio"]:hover > div:last-child {
  border-color: #2f75bf !important;
  background: #ffffff !important;
}

[data-testid="stSidebar"] [data-testid="stRadio"] label[data-baseweb="radio"] p {
  color: #154067 !important;
}

[data-testid="stSidebar"] [data-testid="stRadio"] label[data-baseweb="radio"]:has(input:checked) p {
  color: #092f52 !important;
}

[data-testid="stSidebar"] div[data-baseweb="select"] > div {
  background: #ffffff !important;
  border-color: #7aa3d0 !important;
}

[data-testid="stSidebar"] div[data-baseweb="select"] [role="combobox"] {
  color: #0d3659 !important;
  font-weight: 700 !important;
}

[data-testid="stSidebar"] [data-testid="stSidebarNav"] a:hover {
  border-color: #2f75bf !important;
  background: #ffffff !important;
}

[data-testid="stSidebar"] [data-testid="stSidebarNav"] a[aria-current="page"] {
  border-color: #085bbc !important;
  background: linear-gradient(120deg, #d6e7ff 0%, #dff9f4 100%) !important;
}

[data-testid="metric-container"] {
  background: rgba(255, 255, 255, 0.98) !important;
  border-color: #a8c1df !important;
}

.stButton > button,
[data-testid="stDownloadButton"] > button,
[data-testid="stFormSubmitButton"] > button {
  background: linear-gradient(180deg, #ffffff 0%, #d8e7ff 100%) !important;
  border-color: #2e72bb !important;
  color: #082a48 !important;
  box-shadow: 0 8px 18px rgba(22, 64, 106, 0.2) !important;
}

.stButton > button:hover,
[data-testid="stDownloadButton"] > button:hover,
[data-testid="stFormSubmitButton"] > button:hover {
  background: linear-gradient(180deg, #f8fbff 0%, #c7dcff 100%) !important;
  border-color: #074f9f !important;
}

div[data-baseweb="select"] > div,
div[data-baseweb="input"] > div,
textarea,
[data-testid="stDataEditor"] input {
  background: #ffffff !important;
  border: 1px solid #92b1d2 !important;
  color: #0d3659 !important;
}

div[data-baseweb="select"] svg,
div[data-baseweb="input"] svg {
  color: #1f4f7f !important;
}

div[data-baseweb="select"] [role="combobox"],
div[data-baseweb="input"] input,
textarea {
  color: #0d3659 !important;
}

div[data-baseweb="select"] input::placeholder,
div[data-baseweb="input"] input::placeholder,
textarea::placeholder {
  color: #5a7ea3 !important;
  opacity: 1 !important;
}

[data-testid="stDataFrame"],
.stDataFrame {
  background: #ffffff !important;
  border-color: #97b5d7 !important;
}

[data-testid="stDataFrame"] [role="columnheader"],
[data-testid="stDataFrame"] [role="rowheader"] {
  background: #dce9fb !important;
  color: #123d63 !important;
  font-weight: 700 !important;
}

[data-testid="stDataFrame"] [role="gridcell"],
[data-testid="stDataFrame"] [role="cell"] {
  background: #ffffff !important;
  color: #123d63 !important;
}

[data-testid="stForm"] {
  background: rgba(255, 255, 255, 0.98) !important;
  border-color: #9dbbdd !important;
}

[data-testid="stAlertContainer"] {
  border-radius: 12px !important;
}

[data-testid="stAlertContainer"] p {
  font-weight: 600 !important;
}

[data-testid="stAlertContainer"]:has([data-testid="stAlertContentInfo"]) {
  background: #eaf4ff !important;
  border-color: #4f8ccb !important;
}

[data-testid="stAlertContainer"]:has([data-testid="stAlertContentInfo"]) p {
  color: #0f3659 !important;
}

[data-testid="stAlertContainer"]:has([data-testid="stAlertContentSuccess"]) {
  background: #e9f8ee !important;
  border-color: #3f9a64 !important;
}

[data-testid="stAlertContainer"]:has([data-testid="stAlertContentSuccess"]) p {
  color: #194f2f !important;
}

[data-testid="stAlertContainer"]:has([data-testid="stAlertContentWarning"]) {
  background: #fff4da !important;
  border-color: #d5972f !important;
}

[data-testid="stAlertContainer"]:has([data-testid="stAlertContentWarning"]) p {
  color: #5d3d03 !important;
}

[data-testid="stAlertContainer"]:has([data-testid="stAlertContentError"]) {
  background: #ffeae8 !important;
  border-color: #d36b5f !important;
}

[data-testid="stAlertContainer"]:has([data-testid="stAlertContentError"]) p {
  color: #6e221e !important;
}

[data-testid="stFileUploaderDropzone"] {
  background: #f7fbff !important;
  border-color: #4f8ccb !important;
}

[data-testid="stFileUploaderDropzone"] p,
[data-testid="stFileUploaderDropzone"] small {
  color: #124063 !important;
}
</style>
"""


def _resolve_theme_key(theme_key: str | None = None) -> str:
    requested = (theme_key or "").strip().lower()
    if requested in THEME_PRESETS:
        st.session_state[UI_THEME_SESSION_KEY] = requested
        return requested

    current = str(st.session_state.get(UI_THEME_SESSION_KEY, DEFAULT_THEME_PRESET)).strip().lower()
    if current not in THEME_PRESETS:
        current = DEFAULT_THEME_PRESET
    return current


def _root_vars(theme_key: str) -> str:
    preset = THEME_PRESETS[theme_key]
    return "".join(f"  --pa-{name}: {value};\n" for name, value in preset.items())


def _theme_specific_css(theme_key: str) -> str:
    if theme_key == "bright":
        return BRIGHT_THEME_CSS
    return ""


def render_theme_selector() -> str:
    options = list(THEME_PRESETS.keys())
    current = _resolve_theme_key()
    if str(st.session_state.get(UI_THEME_SESSION_KEY, "")).strip().lower() not in THEME_PRESETS:
        st.session_state[UI_THEME_SESSION_KEY] = current

    selected = st.radio(
        "Color theme",
        options=options,
        horizontal=True,
        format_func=lambda key: THEME_LABELS.get(key, key.replace("_", " ").title()),
        key=UI_THEME_SESSION_KEY,
        help="Switch between bright, dark, deep dark, and palenight presets.",
    )
    return str(selected)


def apply_futuristic_theme(theme_key: str | None = None) -> None:
    resolved = _resolve_theme_key(theme_key)
    css = THEME_CSS_TEMPLATE.replace("__ROOT_VARS__", _root_vars(resolved))
    css += _theme_specific_css(resolved)
    st.markdown(css, unsafe_allow_html=True)
