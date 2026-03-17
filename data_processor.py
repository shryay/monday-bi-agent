"""Data cleaning, normalization, and analytics for Monday.com board data.

Handles the intentionally messy data: missing values, inconsistent formats,
duplicate header rows, and mixed types.
"""

from __future__ import annotations

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Any, Dict, List, Optional


# ===================================================================== #
#  Utility helpers                                                       #
# ===================================================================== #

def _parse_number(val: Any) -> Optional[float]:
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        cleaned = val.replace(",", "").replace("₹", "").replace("INR", "").strip()
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def _parse_date(val: Any) -> Optional[datetime]:
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    if isinstance(val, datetime):
        return val
    if isinstance(val, pd.Timestamp):
        return val.to_pydatetime()
    if not isinstance(val, str) or not val.strip():
        return None
    for fmt in (
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%Y-%m-%dT%H:%M:%S",
        "%d %b %Y",
        "%b %d, %Y",
    ):
        try:
            return datetime.strptime(val.strip(), fmt)
        except ValueError:
            continue
    return None


def _fmt_inr(amount: Optional[float]) -> str:
    """Format as Indian Rupees (Cr / L / plain)."""
    if amount is None or (isinstance(amount, float) and np.isnan(amount)):
        return "N/A"
    sign = "-" if amount < 0 else ""
    a = abs(amount)
    if a >= 1e7:
        return f"{sign}₹{a / 1e7:.2f} Cr"
    if a >= 1e5:
        return f"{sign}₹{a / 1e5:.2f} L"
    return f"{sign}₹{a:,.0f}"


def _quarter_bounds(period: str):
    """Return (start, end) datetimes for a human-readable period string."""
    now = datetime.now()
    p = period.lower().strip()

    def _q_start(year, q):
        return datetime(year, (q - 1) * 3 + 1, 1)

    def _q_end(year, q):
        m = q * 3 + 1
        return datetime(year + 1, m - 12, 1) if m > 12 else datetime(year, m, 1)

    cur_q = (now.month - 1) // 3 + 1

    if p in ("this_quarter", "this quarter"):
        return _q_start(now.year, cur_q), _q_end(now.year, cur_q)
    if p in ("last_quarter", "last quarter"):
        prev_q = cur_q - 1 if cur_q > 1 else 4
        prev_y = now.year if cur_q > 1 else now.year - 1
        return _q_start(prev_y, prev_q), _q_end(prev_y, prev_q)
    if p in ("this_year", "this year"):
        return datetime(now.year, 1, 1), datetime(now.year + 1, 1, 1)
    if p.isdigit() and len(p) == 4:
        y = int(p)
        return datetime(y, 1, 1), datetime(y + 1, 1, 1)
    if p.startswith("q") and len(p) >= 6:
        try:
            parts = p.split()
            q, y = int(parts[0][1]), int(parts[1])
            return _q_start(y, q), _q_end(y, q)
        except (ValueError, IndexError):
            pass
    return None, None


def _apply_time_filter(df: pd.DataFrame, col: str, period: str) -> pd.DataFrame:
    if col not in df.columns:
        return df
    start, end = _quarter_bounds(period)
    if start is None:
        return df
    mask = df[col].apply(
        lambda x: isinstance(x, datetime) and start <= x < end
    )
    return df[mask]


def _match_col(df_cols: List[str], keywords: List[str]) -> Optional[str]:
    """Fuzzy-match a DataFrame column name to a list of keyword alternatives."""
    lower_map = {c.lower().strip(): c for c in df_cols}
    for kw in keywords:
        if kw in lower_map:
            return lower_map[kw]
    for kw in keywords:
        for lc, orig in lower_map.items():
            if kw in lc:
                return orig
    return None


def _quality_report(df: pd.DataFrame) -> Dict[str, Any]:
    if df.empty:
        return {"total_records": 0, "completeness": 0, "issues": ["No data"]}
    total = len(df)
    missing: Dict[str, Dict] = {}
    issues: List[str] = []
    for col in df.columns:
        nulls = int(df[col].isna().sum() + (df[col] == "").sum())
        if nulls:
            pct = round(nulls / total * 100, 1)
            missing[col] = {"count": nulls, "pct": pct}
            if pct > 50:
                issues.append(f"{col}: {pct}% missing")
    total_cells = total * len(df.columns)
    filled = total_cells - sum(m["count"] for m in missing.values())
    return {
        "total_records": total,
        "completeness": round(filled / total_cells * 100, 1) if total_cells else 0,
        "missing": missing,
        "issues": issues,
    }


# ===================================================================== #
#  Deals board processor                                                 #
# ===================================================================== #

_DEAL_COL_MAP = {
    "deal_name": ["deal name", "item name", "name"],
    "owner": ["owner code", "owner", "sales owner"],
    "client": ["client code", "client", "customer"],
    "status": ["deal status", "status"],
    "close_date": ["close date (a)", "close date", "actual close date"],
    "probability": ["closure probability", "probability", "win probability"],
    "deal_value": ["masked deal value", "deal value", "value", "amount"],
    "tentative_close": ["tentative close date", "expected close date"],
    "stage": ["deal stage", "stage", "pipeline stage"],
    "product": ["product deal", "product", "product type"],
    "sector": ["sector/service", "sector", "industry", "vertical"],
    "created_date": ["created date", "created", "creation date"],
}


class DealsProcessor:
    """Clean and analyse the Deals board DataFrame."""

    def __init__(self, df: pd.DataFrame):
        self.df = self._clean(df.copy())
        self.quality = _quality_report(self.df)

    # ---- cleaning ---------------------------------------------------- #

    def _clean(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        renames = {}
        for field, kws in _DEAL_COL_MAP.items():
            m = _match_col(df.columns.tolist(), kws)
            if m:
                renames[m] = field
        df = df.rename(columns=renames)

        # drop rows that are header echoes
        for col in ("status", "stage", "sector"):
            if col in df.columns:
                df = df[
                    ~df[col].astype(str).str.lower().isin(
                        ["deal status", "deal stage", "sector/service", "sector"]
                    )
                ]
        df = df.reset_index(drop=True)

        if "deal_value" in df.columns:
            df["deal_value"] = df["deal_value"].apply(_parse_number)
        for dc in ("close_date", "tentative_close", "created_date"):
            if dc in df.columns:
                df[dc] = df[dc].apply(_parse_date)
        for tc in ("status", "sector", "probability", "stage", "product", "owner"):
            if tc in df.columns:
                df[tc] = df[tc].apply(
                    lambda x: x.strip() if isinstance(x, str) and x.strip() else None
                )
        return df

    # ---- summary generation ------------------------------------------ #

    def get_summary(
        self,
        sector_filter: Optional[str] = None,
        status_filter: Optional[str] = None,
        stage_filter: Optional[str] = None,
        time_period: Optional[str] = None,
    ) -> str:
        df = self.df.copy()
        filters: List[str] = []

        if sector_filter and "sector" in df.columns:
            df = df[df["sector"].str.lower().str.contains(sector_filter.lower(), na=False)]
            filters.append(f"sector={sector_filter}")
        if status_filter and "status" in df.columns:
            df = df[df["status"].str.lower().str.contains(status_filter.lower(), na=False)]
            filters.append(f"status={status_filter}")
        if stage_filter and "stage" in df.columns:
            df = df[df["stage"].str.lower().str.contains(stage_filter.lower(), na=False)]
            filters.append(f"stage={stage_filter}")
        if time_period and "created_date" in df.columns:
            df = _apply_time_filter(df, "created_date", time_period)
            filters.append(f"period={time_period}")

        if df.empty:
            return f"No deals found with filters: {', '.join(filters) or 'none'}."

        L: List[str] = ["=== DEALS BOARD — LIVE DATA ==="]
        L.append(f"Filters: {', '.join(filters) or 'None'}")
        L.append(f"Total deals matched: {len(df)}\n")

        # --- status breakdown ---
        if "status" in df.columns:
            L.append("STATUS BREAKDOWN:")
            for st, g in df.groupby("status", dropna=False):
                lbl = st or "Unknown"
                val = g["deal_value"].sum() if "deal_value" in df.columns else None
                vs = f" | Value: {_fmt_inr(val)}" if val and not pd.isna(val) else ""
                L.append(f"  {lbl}: {len(g)} deals{vs}")
            L.append("")

        # --- stage breakdown ---
        if "stage" in df.columns:
            L.append("DEAL STAGE BREAKDOWN:")
            for stage, cnt in df["stage"].value_counts(dropna=False).items():
                L.append(f"  {stage or 'Unknown'}: {cnt}")
            L.append("")

        # --- sector breakdown ---
        if "sector" in df.columns:
            L.append("SECTOR BREAKDOWN:")
            for sec, g in df.groupby("sector", dropna=False):
                lbl = sec or "Unknown"
                val = g["deal_value"].sum() if "deal_value" in df.columns else None
                vs = f" | Value: {_fmt_inr(val)}" if val and not pd.isna(val) else ""
                L.append(f"  {lbl}: {len(g)} deals{vs}")
            L.append("")

        # --- pipeline metrics (open deals) ---
        if "deal_value" in df.columns:
            open_df = df[df["status"].str.lower() == "open"] if "status" in df.columns else df
            if not open_df.empty:
                L.append("PIPELINE METRICS (Open Deals):")
                L.append(f"  Active pipeline value: {_fmt_inr(open_df['deal_value'].sum())}")
                L.append(f"  Average deal size: {_fmt_inr(open_df['deal_value'].mean())}")
                L.append(f"  Largest open deal: {_fmt_inr(open_df['deal_value'].max())}")
                L.append(f"  Open deal count: {len(open_df)}")
                L.append("")

        # --- win / loss ---
        if "status" in df.columns:
            won = len(df[df["status"].str.lower() == "won"])
            dead = len(df[df["status"].str.lower() == "dead"])
            closed = won + dead
            L.append("WIN / LOSS:")
            L.append(f"  Won: {won} | Dead: {dead}")
            if closed:
                L.append(f"  Win rate: {round(won / closed * 100, 1)}%")
            if "deal_value" in df.columns:
                wv = df[df["status"].str.lower() == "won"]["deal_value"].sum()
                L.append(f"  Won deal total value: {_fmt_inr(wv)}")
            L.append("")

        # --- probability (open deals) ---
        if "probability" in df.columns and "status" in df.columns:
            odf = df[df["status"].str.lower() == "open"]
            if not odf.empty and odf["probability"].notna().any():
                L.append("CLOSURE PROBABILITY (Open Deals):")
                for prob, g in odf.groupby("probability", dropna=False):
                    lbl = prob or "Not Set"
                    val = g["deal_value"].sum() if "deal_value" in df.columns else None
                    vs = f" | Value: {_fmt_inr(val)}" if val and not pd.isna(val) else ""
                    L.append(f"  {lbl}: {len(g)} deals{vs}")
                L.append("")

        # --- top owners ---
        if "owner" in df.columns:
            L.append("TOP OWNERS (by deal count):")
            for owner, cnt in df["owner"].value_counts(dropna=False).head(5).items():
                L.append(f"  {owner or 'Unassigned'}: {cnt}")
            L.append("")

        # --- data quality ---
        L.append("DATA QUALITY:")
        L.append(f"  Overall completeness: {self.quality['completeness']}%")
        for iss in self.quality["issues"][:5]:
            L.append(f"  ⚠ {iss}")

        return "\n".join(L)


# ===================================================================== #
#  Work Orders board processor                                           #
# ===================================================================== #

_WO_COL_MAP = {
    "deal_name": ["deal name masked", "deal name", "item name", "name"],
    "customer": ["customer name code", "customer", "client"],
    "serial": ["serial #", "serial", "wo number"],
    "nature": ["nature of work", "work nature", "contract type"],
    "execution_status": ["execution status", "exec status"],
    "sector": ["sector"],
    "work_type": ["type of work", "work type"],
    "po_date": ["date of po/loi", "po date", "order date"],
    "start_date": ["probable start date", "start date"],
    "end_date": ["probable end date", "end date"],
    "owner": ["bd/kam personnel code", "owner", "personnel"],
    "amount_excl": ["amount in rupees (excl of gst) (masked)", "amount excl gst", "order value"],
    "amount_incl": ["amount in rupees (incl of gst) (masked)", "amount incl gst"],
    "billed_excl": ["billed value in rupees (excl of gst.) (masked)", "billed excl gst"],
    "billed_incl": ["billed value in rupees (incl of gst.) (masked)", "billed incl gst"],
    "collected": ["collected amount in rupees (incl of gst.) (masked)", "collected amount"],
    "unbilled_excl": ["amount to be billed in rs. (exl. of gst) (masked)", "to be billed excl"],
    "unbilled_incl": ["amount to be billed in rs. (incl. of gst) (masked)", "to be billed incl"],
    "receivable": ["amount receivable (masked)", "receivable", "ar amount"],
    "wo_status": ["wo status (billed)", "wo status"],
    "billing_status": ["billing status"],
    "platform": [
        "is any skylark software platform part of the client deliverables in this deal?",
        "platform",
        "software",
    ],
    "last_invoice_date": ["last invoice date"],
    "delivery_date": ["data delivery date"],
}

_WO_NUM_COLS = [
    "amount_excl", "amount_incl", "billed_excl", "billed_incl",
    "collected", "unbilled_excl", "unbilled_incl", "receivable",
]


class WorkOrdersProcessor:
    """Clean and analyse the Work Orders board DataFrame."""

    def __init__(self, df: pd.DataFrame):
        self.df = self._clean(df.copy())
        self.quality = _quality_report(self.df)

    def _clean(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        renames = {}
        for field, kws in _WO_COL_MAP.items():
            m = _match_col(df.columns.tolist(), kws)
            if m:
                renames[m] = field
        df = df.rename(columns=renames)

        for col in _WO_NUM_COLS:
            if col in df.columns:
                df[col] = df[col].apply(_parse_number)

        for dc in ("po_date", "start_date", "end_date", "last_invoice_date", "delivery_date"):
            if dc in df.columns:
                df[dc] = df[dc].apply(_parse_date)

        for tc in ("execution_status", "sector", "wo_status", "billing_status", "nature", "owner"):
            if tc in df.columns:
                df[tc] = df[tc].apply(
                    lambda x: x.strip() if isinstance(x, str) and x.strip() else None
                )
        return df

    def get_summary(
        self,
        sector_filter: Optional[str] = None,
        execution_status_filter: Optional[str] = None,
        billing_status_filter: Optional[str] = None,
    ) -> str:
        df = self.df.copy()
        filters: List[str] = []

        if sector_filter and "sector" in df.columns:
            df = df[df["sector"].str.lower().str.contains(sector_filter.lower(), na=False)]
            filters.append(f"sector={sector_filter}")
        if execution_status_filter and "execution_status" in df.columns:
            df = df[df["execution_status"].str.lower().str.contains(execution_status_filter.lower(), na=False)]
            filters.append(f"exec_status={execution_status_filter}")
        if billing_status_filter and "billing_status" in df.columns:
            df = df[df["billing_status"].str.lower().str.contains(billing_status_filter.lower(), na=False)]
            filters.append(f"billing={billing_status_filter}")

        if df.empty:
            return f"No work orders found with filters: {', '.join(filters) or 'none'}."

        L: List[str] = ["=== WORK ORDERS BOARD — LIVE DATA ==="]
        L.append(f"Filters: {', '.join(filters) or 'None'}")
        L.append(f"Total work orders matched: {len(df)}\n")

        # --- revenue ---
        if "amount_excl" in df.columns:
            total_val = df["amount_excl"].sum()
            L.append("REVENUE OVERVIEW (Excl GST):")
            L.append(f"  Total order value: {_fmt_inr(total_val)}")
            if "billed_excl" in df.columns:
                billed = df["billed_excl"].sum()
                L.append(f"  Billed: {_fmt_inr(billed)}")
                if total_val:
                    L.append(f"  Billing %: {round(billed / total_val * 100, 1)}%")
            if "collected" in df.columns:
                L.append(f"  Collected (incl GST): {_fmt_inr(df['collected'].sum())}")
            if "receivable" in df.columns:
                L.append(f"  Receivable: {_fmt_inr(df['receivable'].sum())}")
            if "unbilled_excl" in df.columns:
                L.append(f"  Unbilled: {_fmt_inr(df['unbilled_excl'].sum())}")
            L.append("")

        # --- execution status ---
        if "execution_status" in df.columns:
            L.append("EXECUTION STATUS:")
            for st, cnt in df["execution_status"].value_counts(dropna=False).items():
                L.append(f"  {st or 'Unknown'}: {cnt}")
            if "execution_status" in df.columns:
                completed = (df["execution_status"].str.lower() == "completed").sum()
                L.append(f"  Completion rate: {round(completed / len(df) * 100, 1)}%")
            L.append("")

        # --- sector ---
        if "sector" in df.columns:
            L.append("SECTOR BREAKDOWN:")
            for sec, g in df.groupby("sector", dropna=False):
                lbl = sec or "Unknown"
                val = g["amount_excl"].sum() if "amount_excl" in df.columns else None
                vs = f" | Value: {_fmt_inr(val)}" if val and not pd.isna(val) else ""
                coll = g["collected"].sum() if "collected" in df.columns else None
                cs = f" | Collected: {_fmt_inr(coll)}" if coll and not pd.isna(coll) else ""
                L.append(f"  {lbl}: {len(g)} WOs{vs}{cs}")
            L.append("")

        # --- wo status ---
        if "wo_status" in df.columns:
            L.append("WO STATUS:")
            for st, cnt in df["wo_status"].value_counts(dropna=False).items():
                L.append(f"  {st or 'Not Set'}: {cnt}")
            L.append("")

        # --- nature of work ---
        if "nature" in df.columns:
            L.append("WORK NATURE:")
            for nat, cnt in df["nature"].value_counts(dropna=False).items():
                L.append(f"  {nat or 'Unknown'}: {cnt}")
            L.append("")

        # --- billing status ---
        if "billing_status" in df.columns:
            non_null = df[df["billing_status"].notna()]
            if not non_null.empty:
                L.append("BILLING STATUS:")
                for bs, cnt in non_null["billing_status"].value_counts().items():
                    L.append(f"  {bs}: {cnt}")
                L.append("")

        # --- top customers ---
        if "customer" in df.columns:
            L.append("TOP CUSTOMERS (by WO count):")
            for cust, cnt in df["customer"].value_counts().head(5).items():
                L.append(f"  {cust}: {cnt} WOs")
            L.append("")

        # --- data quality ---
        L.append("DATA QUALITY:")
        L.append(f"  Overall completeness: {self.quality['completeness']}%")
        for iss in self.quality["issues"][:5]:
            L.append(f"  ⚠ {iss}")

        return "\n".join(L)


# ===================================================================== #
#  Cross-board analysis                                                  #
# ===================================================================== #

def cross_board_summary(
    deals_df: pd.DataFrame,
    wo_df: pd.DataFrame,
    analysis_focus: str,
    sector_filter: Optional[str] = None,
) -> str:
    dp = DealsProcessor(deals_df)
    wp = WorkOrdersProcessor(wo_df)
    d = dp.df.copy()
    w = wp.df.copy()

    if sector_filter:
        if "sector" in d.columns:
            d = d[d["sector"].str.lower().str.contains(sector_filter.lower(), na=False)]
        if "sector" in w.columns:
            w = w[w["sector"].str.lower().str.contains(sector_filter.lower(), na=False)]

    L = ["=== CROSS-BOARD ANALYSIS — LIVE DATA ==="]
    L.append(f"Focus: {analysis_focus}")
    if sector_filter:
        L.append(f"Sector filter: {sector_filter}")
    L.append("")

    if analysis_focus == "pipeline_vs_revenue":
        pipeline = d[d["status"].str.lower() == "open"]["deal_value"].sum() if "deal_value" in d.columns and "status" in d.columns else 0
        won_val = d[d["status"].str.lower() == "won"]["deal_value"].sum() if "deal_value" in d.columns and "status" in d.columns else 0
        wo_rev = w["amount_excl"].sum() if "amount_excl" in w.columns else 0
        collected = w["collected"].sum() if "collected" in w.columns else 0
        receivable = w["receivable"].sum() if "receivable" in w.columns else 0

        L.append("PIPELINE vs REVENUE:")
        L.append(f"  Active pipeline (open deals): {_fmt_inr(pipeline)}")
        L.append(f"  Won deals total: {_fmt_inr(won_val)}")
        L.append(f"  Work order revenue (excl GST): {_fmt_inr(wo_rev)}")
        L.append(f"  Collected: {_fmt_inr(collected)}")
        L.append(f"  Receivable: {_fmt_inr(receivable)}")
        if won_val and wo_rev:
            L.append(f"  Won→WO conversion: {round(wo_rev / won_val * 100, 1)}%")
        if wo_rev and collected:
            L.append(f"  Collection efficiency: {round(collected / wo_rev * 100, 1)}%")

    elif analysis_focus == "sector_comparison":
        sectors = set()
        if "sector" in d.columns:
            sectors.update(d["sector"].dropna().unique())
        if "sector" in w.columns:
            sectors.update(w["sector"].dropna().unique())

        L.append("SECTOR COMPARISON:")
        for sec in sorted(sectors):
            ds = d[d["sector"].str.lower() == sec.lower()] if "sector" in d.columns else pd.DataFrame()
            ws = w[w["sector"].str.lower() == sec.lower()] if "sector" in w.columns else pd.DataFrame()
            d_cnt = len(ds)
            d_val = ds["deal_value"].sum() if "deal_value" in ds.columns and not ds.empty else 0
            w_cnt = len(ws)
            w_rev = ws["amount_excl"].sum() if "amount_excl" in ws.columns and not ws.empty else 0
            w_coll = ws["collected"].sum() if "collected" in ws.columns and not ws.empty else 0
            L.append(f"\n  {sec}:")
            L.append(f"    Deals: {d_cnt} | Pipeline: {_fmt_inr(d_val)}")
            L.append(f"    WOs:   {w_cnt} | Revenue: {_fmt_inr(w_rev)} | Collected: {_fmt_inr(w_coll)}")

    elif analysis_focus == "conversion":
        total_deals = len(d)
        won = len(d[d["status"].str.lower() == "won"]) if "status" in d.columns else 0
        total_wo = len(w)
        completed = len(w[w["execution_status"].str.lower() == "completed"]) if "execution_status" in w.columns else 0

        L.append("DEAL → EXECUTION CONVERSION:")
        L.append(f"  Total deals: {total_deals}")
        L.append(f"  Won deals: {won}")
        L.append(f"  Total work orders: {total_wo}")
        L.append(f"  Completed WOs: {completed}")
        if won:
            L.append(f"  WO-per-won-deal ratio: {round(total_wo / won, 2)}")
        if total_wo:
            L.append(f"  WO completion rate: {round(completed / total_wo * 100, 1)}%")

    L.append(f"\nDATA QUALITY:")
    L.append(f"  Deals completeness: {dp.quality['completeness']}%")
    L.append(f"  Work orders completeness: {wp.quality['completeness']}%")

    return "\n".join(L)
