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
            "description": "Query deals pipeline data. Use for pipeline, values, stages, sectors, win rates.",
            "parameters": {
                "type": "object",
                "properties": {
                    "sector_filter": {"type": "string"},
                    "status_filter": {"type": "string"},
                    "stage_filter": {"type": "string"},
                    "time_period": {"type": "string"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "query_work_orders_board",
            "description": "Query work orders data. Use for revenue, billing, collections, execution status.",
            "parameters": {
                "type": "object",
                "properties": {
                    "sector_filter": {"type": "string"},
                    "execution_status_filter": {"type": "string"},
                    "billing_status_filter": {"type": "string"},
                },
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "cross_board_analysis",
            "description": "Analyse across both boards. Use for pipeline vs revenue, sector comparison, conversion.",
            "parameters": {
                "type": "object",
                "properties": {
                    "analysis_focus": {
                        "type": "string",
                        "enum": ["pipeline_vs_revenue", "sector_comparison", "conversion"],
                    },
                    "sector_filter": {"type": "string"},
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
