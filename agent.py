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
You are a senior Business Intelligence analyst at Skylark Drones, a leading drone
technology company in India.  You answer questions from the founders and executive
team with precision, insight, and actionable recommendations.

**Tools at your disposal** (each makes a LIVE Monday.com API call):
1. query_deals_board  – pipeline, deal values, stages, sectors, win rates
2. query_work_orders_board – revenue, billing, collections, execution status
3. cross_board_analysis – combined insights across both boards

**Rules you must follow:**
1. ALWAYS call tools to fetch live data before answering.  Never fabricate numbers.
2. Deliver INSIGHTS, not raw dumps:
   - BAD: "There are 106 Mining deals."
   - GOOD: "Mining leads your pipeline with 106 deals (31% of total), but the
     33% dead-deal rate signals that early qualification needs tightening."
3. Mention data-quality caveats when they affect the answer (missing values,
   incomplete records).
4. If a question is ambiguous, ask ONE clarifying question rather than guessing.
5. Support follow-up questions by referencing earlier conversation context.
6. Format money as ₹ X.XX Cr / ₹ X.XX L.
7. Highlight **risks**, **opportunities**, and **recommendations**.
8. Keep answers concise — executives value brevity with depth available on request.

**Domain context:**
• Sectors: Mining, Renewables, Railways, Powerline, Construction, Others, DSP, Tender
• Deal stages (A→O): Lead → Sales-Qualified → Demo → Feasibility → Proposal →
  Negotiations → Won → WO Received → POC → Invoice → Accrued → Lost → On Hold →
  Not Relevant
• Deal statuses: Open, Won, Dead, On Hold
• Work order statuses: Completed, Ongoing, Not Started, Pause/struck, Partial
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

    # ------------------------------------------------------------------ #
    #  Tool execution                                                      #
    # ------------------------------------------------------------------ #

    def _run_tool(self, name: str, args: Dict[str, Any]) -> str:
        self.monday.clear_action_log()

        try:
            if name == "query_deals_board":
                self.traces.append({"tool": name, "params": args, "status": "running"})
                _, df = self.monday.fetch_board_items(self.deals_board_id)
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
                return result

            if name == "query_work_orders_board":
                self.traces.append({"tool": name, "params": args, "status": "running"})
                _, df = self.monday.fetch_board_items(self.wo_board_id)
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
                return result

            if name == "cross_board_analysis":
                self.traces.append({"tool": name, "params": args, "status": "running"})
                _, d_df = self.monday.fetch_board_items(self.deals_board_id)
                _, w_df = self.monday.fetch_board_items(self.wo_board_id)
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
                return result

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
        for msg in conversation_history[-10:]:
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
            body = getattr(error, "body", None)
            if isinstance(body, dict):
                failed = body.get("error", {}).get("failed_generation", "")
            else:
                failed = err_str
            match = re.search(
                r"<function=(\w+)[,\s]*(\{[^}]*\})\s*</function>",
                failed,
                re.DOTALL,
            )
            if match:
                return match.group(1), json.loads(match.group(2))
        except Exception:
            pass
        return None
