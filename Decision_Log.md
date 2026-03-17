# Decision Log — Monday.com BI Agent

## 1. Architecture & Tech Stack

**Streamlit** was chosen as the UI framework because it provides a production-ready chat interface
(`st.chat_input`, `st.chat_message`) with zero frontend code, built-in session state for conversation
memory, and one-click deployment to Streamlit Cloud. For a 6-hour timeline, this eliminated the
overhead of a separate frontend/backend setup while still delivering a polished, interactive prototype.

**Groq (Llama 3.3 70B)** serves as the LLM engine via an OpenAI-compatible API. This choice was
driven by three factors: (a) free tier with 14,400 requests/day, (b) strong tool-calling support that
lets the AI autonomously decide which Monday.com boards to query, and (c) drop-in compatibility
with OpenAI's Python SDK — meaning the system also supports GPT-4o and Google Gemini with a
single provider dropdown. The tool-calling approach (3 defined functions) is the core of the agent:
the LLM interprets the user's question, selects the right tool with appropriate filters, and generates
insights from the returned data — all without hardcoded query logic.

**Monday.com GraphQL API** with cursor-based pagination handles board data fetching. Each user
query triggers fresh API calls, ensuring the evaluator always sees live data. Within a single query,
board data is cached per-request so that multi-tool orchestration (e.g., cross-board analysis) does
not re-fetch the same board. The GraphQL schema allows fetching column definitions and item data
in a single request, minimizing round trips.

**pandas** handles all data cleaning — null detection, date normalization, number parsing, and
summary aggregation. A keyword-based column mapper tolerates naming differences between the
original Excel headers and Monday.com's imported column titles.

## 2. Handling Messy Data

The provided datasets have significant quality challenges: 52% of deal values are missing, 75% of
closure probabilities are null, the Work Orders sheet contains blank header rows, and date formats
are inconsistent across columns. Rather than failing silently, the agent takes a transparent approach:

- **Duplicate header rows** (where "Deal Status" appears as a data value) are detected and filtered
  during processing using exact-match exclusion against known header strings.
- **Missing values** are tracked per-column and reported as a completeness percentage in every
  response, so the founder knows the confidence level of the insight.
- **Date parsing** tries 8 format patterns (ISO, DD/MM/YYYY, US format, etc.) with graceful
  fallback to null. Monetary values are cleaned of commas, currency symbols, and whitespace
  before numeric conversion.
- **Column name matching** uses a keyword-based fuzzy mapper — if Monday.com renames
  "Masked Deal value" to "Masked Deal Value" during import, it still maps correctly.

## 3. Key Trade-offs

**Token-efficient pipeline.** Free-tier LLM providers impose strict token limits (Groq: 100K tokens/day).
To maximize queries per day, the agent uses four strategies: (a) a compact system prompt (~120
tokens vs typical ~350), (b) tool output truncation at 6K characters so the LLM receives summaries,
not raw dumps, (c) conversation history limited to the last 4 exchanges, and (d) per-request board
caching to avoid redundant API calls. Together these reduce per-query token usage by ~60%.
The trade-off is that the LLM cannot run ad-hoc calculations on raw rows, but for founder-level
questions, pre-computed aggregates cover the vast majority of use cases.

**Malformed tool-call recovery.** Llama 3.3 occasionally generates tool calls in an XML-like format
(`<function=name {...}>`) instead of the expected JSON structure. Rather than failing, the agent
catches these errors, parses the intended function and arguments from the error message using regex,
executes the tool, and feeds the result back to the LLM. This makes the system significantly more
robust in production without requiring a paid model.

**Multi-provider support over single-provider optimization.** Supporting Groq, Gemini, and OpenAI
through a unified interface adds slight complexity but demonstrates extensibility and allows the
evaluator to test with their preferred provider.

## 4. What I Would Improve With More Time

- Add chart/visualization generation (matplotlib or plotly) for trend data
- Implement semantic caching with TTL to reduce API calls for repeated questions across sessions
- Add Monday.com MCP server integration for a standardized tool protocol
- Build a feedback loop where the agent learns from corrected answers
- Add authentication so the hosted prototype doesn't require sidebar key entry
