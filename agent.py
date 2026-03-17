"""OpenAI-powered BI agent with function calling for Monday.com data."""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional, Tuple

from openai import OpenAI, BadRequestError

from monday_client import MondayClient
from data_processor import DealsProcessor, WorkOrdersProcessor, cross_board_summary

# ===================================================================== #
#  System prompt                                                         #
# ===================================================================== #

SYSTEM_PROMPT = """\
You are a BI analyst at Skylark Drones (India). Answer founders' questions with precision and insight.

Tools (each makes a LIVE Monday.com API call):
1. query_deals_board – pipeline, deal values, stages, sectors, win rates
2. query_work_orders_board – revenue, billing, collections, execution status
3. cross_board_analysis – combined insights across both boards

Rules:
1. ALWAYS call tools first. Never fabricate numbers.
2. Deliver INSIGHTS, not raw data. Highlight risks, opportunities, recommendations.
3. Note data-quality caveats when relevant.
4. If ambiguous, ask ONE clarifying question.
5. Format money as ₹ X.XX Cr / ₹ X.XX L. Keep answers concise.
"""

# ===================================================================== #
#  Tool definitions for OpenAI function calling                          #
# ===================================================================== #

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "query_deals_board",
            "description": (
                "Fetch and analyse deals / pipeline data from the Monday.com Deals board. "
                "Makes a LIVE API call.  Use for deal pipeline, values, stages, sectors, "
                "probabilities, win rates, and sales performance."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "sector_filter": {
                        "type": "string",
                        "description": "Filter by sector: Mining, Renewables, Powerline, Railways, Construction, Tender, Others, DSP",
                    },
                    "status_filter": {
                        "type": "string",
                        "description": "Filter by deal status: Open, Won, Dead, On Hold",
                    },
                    "stage_filter": {
                        "type": "string",
                        "description": "Filter by deal stage keyword, e.g. 'Lead', 'Proposal', 'Won', 'Negotiations'",
                    },
                    "time_period": {
                        "type": "string",
                        "description": "Time filter: 'Q1 2026', 'this_quarter', 'last_quarter', 'this_year', '2025'",
                    },
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "query_work_orders_board",
            "description": (
                "Fetch and analyse work-order data from the Monday.com Work Orders board. "
                "Makes a LIVE API call.  Use for revenue, billing, collections, receivables, "
                "execution status, and operational metrics."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "sector_filter": {
                        "type": "string",
                        "description": "Filter by sector: Mining, Renewables, Powerline, Railways, Construction",
                    },
                    "execution_status_filter": {
                        "type": "string",
                        "description": "Filter: Completed, Ongoing, Not Started, Pause / struck",
                    },
                    "billing_status_filter": {
                        "type": "string",
                        "description": "Filter: Billed, Partially Billed, Not Billable, Stuck, Update Required",
                    },
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "cross_board_analysis",
            "description": (
                "Analyse data across BOTH Deals and Work Orders boards (two LIVE API calls). "
                "Use when the question spans pipeline and execution / revenue."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "analysis_focus": {
                        "type": "string",
                        "enum": ["pipeline_vs_revenue", "sector_comparison", "conversion"],
                        "description": (
                            "pipeline_vs_revenue = pipeline vs actual revenue; "
                            "sector_comparison = sector performance across boards; "
                            "conversion = deal-to-execution funnel"
                        ),
                    },
                    "sector_filter": {
                        "type": "string",
                        "description": "Optional sector filter applied to both boards",
                    },
                },
                "required": ["analysis_focus"],
            },
        },
    },
]

# ===================================================================== #
#  Agent class                                                           #
# ===================================================================== #


class BIAgent:
    """Conversational BI agent backed by OpenAI + Monday.com live data."""

    def __init__(
        self,
        openai_api_key: str,
        monday_client: MondayClient,
        deals_board_id: str,
        wo_board_id: str,
        model: str = "gpt-4o",
        base_url: Optional[str] = None,
    ):
        client_kwargs: Dict[str, Any] = {"api_key": openai_api_key}
        if base_url:
            client_kwargs["base_url"] = base_url
        self.llm = OpenAI(**client_kwargs)
        self.monday = monday_client
        self.deals_board_id = deals_board_id
        self.wo_board_id = wo_board_id
        self.model = model
        self.traces: List[Dict[str, Any]] = []
        self._deals_cache = None
        self._wo_cache = None

    # ------------------------------------------------------------------ #
    #  Tool execution                                                      #
    # ------------------------------------------------------------------ #

    MAX_TOOL_CHARS = 6000

    def _truncate(self, text: str) -> str:
        if len(text) <= self.MAX_TOOL_CHARS:
            return text
        return text[: self.MAX_TOOL_CHARS] + "\n\n[...data truncated for token efficiency]"

    def _fetch_deals(self):
        if self._deals_cache is not None:
            return self._deals_cache
        _, df = self.monday.fetch_board_items(self.deals_board_id)
        self._deals_cache = df
        return df

    def _fetch_work_orders(self):
        if self._wo_cache is not None:
            return self._wo_cache
        _, df = self.monday.fetch_board_items(self.wo_board_id)
        self._wo_cache = df
        return df

    def _run_tool(self, name: str, args: Dict[str, Any]) -> str:
        self.monday.clear_action_log()

        try:
            if name == "query_deals_board":
                self.traces.append({"tool": name, "params": args, "status": "running"})
                df = self._fetch_deals()
                result = DealsProcessor(df).get_summary(
                    sector_filter=args.get("sector_filter"),
                    status_filter=args.get("status_filter"),
                    stage_filter=args.get("stage_filter"),
                    time_period=args.get("time_period"),
                )
                self.traces[-1].update(
                    status="done",
                    rows=len(df),
                    api_calls=self.monday.get_action_log(),
                )
                return self._truncate(result)

            if name == "query_work_orders_board":
                self.traces.append({"tool": name, "params": args, "status": "running"})
                df = self._fetch_work_orders()
                result = WorkOrdersProcessor(df).get_summary(
                    sector_filter=args.get("sector_filter"),
                    execution_status_filter=args.get("execution_status_filter"),
                    billing_status_filter=args.get("billing_status_filter"),
                )
                self.traces[-1].update(
                    status="done",
                    rows=len(df),
                    api_calls=self.monday.get_action_log(),
                )
                return self._truncate(result)

            if name == "cross_board_analysis":
                self.traces.append({"tool": name, "params": args, "status": "running"})
                d_df = self._fetch_deals()
                w_df = self._fetch_work_orders()
                result = cross_board_summary(
                    d_df,
                    w_df,
                    analysis_focus=args.get("analysis_focus", "sector_comparison"),
                    sector_filter=args.get("sector_filter"),
                )
                self.traces[-1].update(
                    status="done",
                    deals_rows=len(d_df),
                    wo_rows=len(w_df),
                    api_calls=self.monday.get_action_log(),
                )
                return self._truncate(result)

            return f"Unknown tool '{name}'."

        except Exception as exc:
            self.traces[-1].update(status="error", error=str(exc))
            return f"Error running {name}: {exc}"

    # ------------------------------------------------------------------ #
    #  Main query entry-point                                              #
    # ------------------------------------------------------------------ #

    def process_query(
        self,
        user_message: str,
        conversation_history: List[Dict[str, str]],
    ) -> Tuple[str, List[Dict[str, Any]]]:
        """
        Send the user's message through the agent loop (up to 5 tool-call
        iterations) and return (response_text, action_traces).
        """
        self.traces = []

        messages: List[Dict[str, Any]] = [{"role": "system", "content": SYSTEM_PROMPT}]
        for msg in conversation_history[-4:]:
            messages.append({"role": msg["role"], "content": msg["content"]})
        messages.append({"role": "user", "content": user_message})

        max_iters = 5
        for i in range(max_iters):
            try:
                resp = self.llm.chat.completions.create(
                    model=self.model,
                    messages=messages,
                    tools=TOOLS,
                    tool_choice="auto" if i < max_iters - 1 else "none",
                )
            except Exception as e:
                recovered = self._recover_failed_tool_call(e)
                if recovered:
                    fn, fn_args = recovered
                    self.traces.append(
                        {"step": f"Agent decided → {fn} (recovered)", "arguments": fn_args}
                    )
                    result = self._run_tool(fn, fn_args)
                    messages.append(
                        {"role": "assistant", "content": f"I called {fn} with parameters {json.dumps(fn_args)}."}
                    )
                    messages.append(
                        {"role": "user", "content": f"Here is the data from the tool:\n\n{result}\n\nPlease provide your analysis and insights."}
                    )
                    continue
                raise

            choice = resp.choices[0]

            if choice.finish_reason == "tool_calls" and choice.message.tool_calls:
                messages.append(choice.message)
                for tc in choice.message.tool_calls:
                    fn = tc.function.name
                    fn_args = json.loads(tc.function.arguments)
                    self.traces.append(
                        {"step": f"Agent decided → {fn}", "arguments": fn_args}
                    )
                    result = self._run_tool(fn, fn_args)
                    messages.append(
                        {
                            "role": "tool",
                            "tool_call_id": tc.id,
                            "content": result,
                        }
                    )
            else:
                answer = choice.message.content or (
                    "I wasn't able to generate a response. Could you rephrase?"
                )
                return answer, self.traces

        return (
            "I reached the analysis step limit. Please ask a more specific question.",
            self.traces,
        )

    @staticmethod
    def _recover_failed_tool_call(error: Exception) -> Optional[tuple]:
        """Extract function name + args from a malformed Groq/Llama tool call."""
        err_str = str(error)
        if "failed_generation" not in err_str and "tool_use_failed" not in err_str:
            return None
        try:
            failed = ""
            body = getattr(error, "body", None)
            if isinstance(body, dict):
                failed = (
                    body.get("error", {}).get("failed_generation", "")
                    or body.get("failed_generation", "")
                )
            if not failed:
                failed = err_str
            match = re.search(
                r"<function=(\w+)[,\s]*(\{.*?\})\s*</function>",
                failed,
                re.DOTALL,
            )
            if match:
                return match.group(1), json.loads(match.group(2))
        except Exception:
            pass
        return None
