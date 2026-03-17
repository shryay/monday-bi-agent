"""
Monday.com BI Agent — Streamlit Application
=============================================
Production-ready conversational BI agent for founder-level queries.
"""

import os
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

from monday_client import MondayClient
from agent import BIAgent

# ------------------------------------------------------------------ #
#  Page config                                                         #
# ------------------------------------------------------------------ #

st.set_page_config(
    page_title="Monday.com BI Agent",
    page_icon="📊",
    layout="centered",
    initial_sidebar_state="collapsed",
)

# ------------------------------------------------------------------ #
#  Custom CSS for a polished look                                      #
# ------------------------------------------------------------------ #

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

    .block-container {
        max-width: 820px;
        padding-top: 2rem;
        padding-bottom: 2rem;
    }

    /* Header area */
    .hero-title {
        font-family: 'Inter', sans-serif;
        font-size: 2rem;
        font-weight: 700;
        background: linear-gradient(135deg, #6C63FF, #FF6584);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 0.25rem;
    }
    .hero-subtitle {
        font-family: 'Inter', sans-serif;
        color: #888;
        font-size: 0.95rem;
        margin-bottom: 1.5rem;
    }

    /* Starter pills */
    .starter-container {
        display: flex;
        flex-wrap: wrap;
        gap: 0.5rem;
        margin: 1rem 0 1.5rem 0;
    }

    /* Chat styling */
    .stChatMessage {
        border-radius: 12px;
        font-family: 'Inter', sans-serif;
    }

    /* Trace styling */
    div[data-testid="stExpander"] {
        border: 1px solid #2a2d35;
        border-radius: 8px;
        background: #14171f;
    }
    div[data-testid="stExpander"] summary {
        font-size: 0.85rem;
        color: #888;
    }

    /* Hide Streamlit branding but keep sidebar toggle */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}

    /* Sidebar clean */
    [data-testid="stSidebar"] {
        background: #0E1117;
        border-right: 1px solid #1a1d25;
    }

    /* Badge */
    .live-badge {
        display: inline-block;
        background: #1a3a1a;
        color: #4ade80;
        padding: 2px 10px;
        border-radius: 12px;
        font-size: 0.75rem;
        font-weight: 600;
        margin-left: 0.5rem;
    }
</style>
""", unsafe_allow_html=True)

# ------------------------------------------------------------------ #
#  Config resolution: .env > st.secrets > sidebar                      #
# ------------------------------------------------------------------ #

PROVIDERS = {
    "Groq (Free)": {
        "base_url": "https://api.groq.com/openai/v1",
        "models": ["llama-3.3-70b-versatile", "llama-3.1-70b-versatile", "llama-3.1-8b-instant"],
        "env_key": "GROQ_API_KEY",
    },
    "OpenRouter (Free)": {
        "base_url": "https://openrouter.ai/api/v1",
        "models": ["meta-llama/llama-3.3-70b-instruct:free", "qwen/qwen3-32b:free", "deepseek/deepseek-r1:free"],
        "env_key": "OPENROUTER_API_KEY",
    },
    "Google Gemini (Free)": {
        "base_url": "https://generativelanguage.googleapis.com/v1beta/openai/",
        "models": ["gemini-2.5-flash", "gemini-2.0-flash", "gemini-2.0-flash-lite"],
        "env_key": "GEMINI_API_KEY",
    },
    "OpenAI (Paid)": {
        "base_url": None,
        "models": ["gpt-4o", "gpt-4o-mini", "gpt-4-turbo"],
        "env_key": "OPENAI_API_KEY",
    },
}


def _resolve(key: str) -> str:
    """Resolve config: st.secrets → env → empty."""
    try:
        v = st.secrets[key]
        if v:
            return v
    except (KeyError, FileNotFoundError):
        pass
    return os.environ.get(key, "")


monday_token = _resolve("MONDAY_API_TOKEN")
deals_board_id = _resolve("DEALS_BOARD_ID")
wo_board_id = _resolve("WORK_ORDERS_BOARD_ID")

# ------------------------------------------------------------------ #
#  Sidebar — minimal, clean                                            #
# ------------------------------------------------------------------ #

with st.sidebar:
    st.markdown("### ⚙️ Settings")

    _provider = st.selectbox("AI Provider", list(PROVIDERS.keys()), index=0)
    _prov_cfg = PROVIDERS[_provider]
    _model = st.selectbox("Model", _prov_cfg["models"], index=0)

    st.markdown("---")
    st.caption("API keys are loaded from `.env` or Streamlit Secrets.")

    _monday_override = st.text_input("Monday.com Token", type="password", value="")
    _llm_override = st.text_input(f"{_provider} Key", type="password", value="")
    _deals_override = st.text_input("Deals Board ID", value="")
    _wo_override = st.text_input("Work Orders Board ID", value="")

    st.markdown("---")
    if st.button("🗑️ Clear Chat"):
        st.session_state.messages = []
        st.rerun()

# Final resolved values (override if sidebar has input)
monday_token = _monday_override or monday_token
llm_key = _llm_override or _resolve(_prov_cfg["env_key"])
deals_board_id = _deals_override or deals_board_id
wo_board_id = _wo_override or wo_board_id
llm_base_url = _prov_cfg["base_url"]

config_ok = all([monday_token, llm_key, deals_board_id, wo_board_id])

# ------------------------------------------------------------------ #
#  Header                                                              #
# ------------------------------------------------------------------ #

st.markdown('<div class="hero-title">Monday.com BI Agent</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="hero-subtitle">'
    'Founder-level business intelligence powered by live Monday.com data '
    '<span class="live-badge">● LIVE</span>'
    '</div>',
    unsafe_allow_html=True,
)

if not config_ok:
    st.warning("Configure your API keys to get started — open the sidebar (arrow top-left) or set up a `.env` file.")
    with st.expander("📖 Quick Setup"):
        st.markdown("""
1. **Monday.com Token** — Avatar → Developers → My Access Tokens
2. **Groq Key (free)** — [console.groq.com](https://console.groq.com) → API Keys
3. **Board IDs** — from the Monday.com board URLs
4. Add all to a `.env` file in the project root (see `.env.example`)
        """)
    st.stop()

# ------------------------------------------------------------------ #
#  Starter questions (shown when chat is empty)                        #
# ------------------------------------------------------------------ #

STARTERS = [
    "How's our pipeline for Mining sector?",
    "Revenue breakdown by sector",
    "What's our overall win rate?",
    "Compare pipeline vs actual revenue",
    "Show collection and billing status",
    "Which deals are on hold?",
]

if "messages" not in st.session_state:
    st.session_state.messages = []
if "pending_query" not in st.session_state:
    st.session_state.pending_query = None

if not st.session_state.messages and st.session_state.pending_query is None:
    cols = st.columns(2)
    for i, q in enumerate(STARTERS):
        with cols[i % 2]:
            if st.button(f"💬 {q}", key=f"starter_{i}", use_container_width=True):
                st.session_state.pending_query = q
                st.rerun()

# ------------------------------------------------------------------ #
#  Chat history                                                        #
# ------------------------------------------------------------------ #

for msg in st.session_state.messages:
    with st.chat_message(msg["role"], avatar="🧑‍💼" if msg["role"] == "user" else "📊"):
        st.markdown(msg["content"])
        if msg["role"] == "assistant" and msg.get("traces"):
            with st.expander(f"🔍 Action Trace — {len(msg['traces'])} steps"):
                for idx, t in enumerate(msg["traces"], 1):
                    label = t.get("step", t.get("tool", t.get("action", "Processing")))
                    st.markdown(f"**Step {idx}** · {label}")
                    detail = {k: v for k, v in t.items() if k != "step"}
                    if detail:
                        st.json(detail)

# ------------------------------------------------------------------ #
#  Chat input & agent execution                                        #
# ------------------------------------------------------------------ #

chat_input = st.chat_input("Ask about deals, pipeline, revenue, billing…")

pending = st.session_state.pending_query
if pending:
    st.session_state.pending_query = None
    prompt = pending
elif chat_input:
    prompt = chat_input
else:
    prompt = None

def _build_fallback_chain(primary_provider, primary_model, primary_key, primary_base_url):
    """Build an ordered list of (provider, model, key, base_url) to try."""
    chain = [(primary_provider, primary_model, primary_key, primary_base_url)]
    for pname, pcfg in PROVIDERS.items():
        if pname == primary_provider:
            continue
        key = _resolve(pcfg["env_key"])
        if key:
            chain.append((pname, pcfg["models"][0], key, pcfg["base_url"]))
    return chain


def _should_fallback(exc_str: str) -> bool:
    return "rate_limit" in exc_str or "429" in exc_str or "503" in exc_str or "UNAVAILABLE" in exc_str


if prompt:
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user", avatar="🧑‍💼"):
        st.markdown(prompt)

    with st.chat_message("assistant", avatar="📊"):
        fallbacks = _build_fallback_chain(_provider, _model, llm_key, llm_base_url)
        history = [
            {"role": m["role"], "content": m["content"]}
            for m in st.session_state.messages[:-1]
        ]

        answer, traces, last_err = None, [], None
        for prov_name, prov_model, prov_key, prov_base in fallbacks:
            with st.spinner(f"Querying via {prov_name} ({prov_model})…"):
                try:
                    agent = BIAgent(
                        openai_api_key=prov_key,
                        monday_client=MondayClient(monday_token),
                        deals_board_id=deals_board_id,
                        wo_board_id=wo_board_id,
                        model=prov_model,
                        base_url=prov_base,
                    )
                    answer, traces = agent.process_query(prompt, history)
                    break
                except Exception as exc:
                    last_err = (prov_name, exc)
                    if _should_fallback(str(exc)):
                        st.caption(f"⚠️ {prov_name} rate-limited — trying next provider…")
                        continue
                    last_err = (prov_name, exc)
                    break

        if answer:
            st.markdown(answer)
            if traces:
                with st.expander(f"🔍 Action Trace — {len(traces)} steps"):
                    for idx, t in enumerate(traces, 1):
                        label = t.get("step", t.get("tool", t.get("action", "Processing")))
                        st.markdown(f"**Step {idx}** · {label}")
                        detail = {k: v for k, v in t.items() if k != "step"}
                        if detail:
                            st.json(detail)
            st.session_state.messages.append(
                {"role": "assistant", "content": answer, "traces": traces}
            )
        else:
            prov_name, exc = last_err
            exc_str = str(exc)
            if _should_fallback(exc_str):
                err = (
                    "**All providers rate-limited.** Free tiers have daily token caps.\n"
                    "- Wait ~15 minutes for limits to reset\n"
                    "- Or add a paid OpenAI key in the sidebar"
                )
                st.warning(err)
            else:
                err = f"**{prov_name}** error: {exc}"
                st.error(err)
            st.session_state.messages.append(
                {"role": "assistant", "content": err, "traces": []}
            )
