"""Monday.com GraphQL API client with cursor-based pagination and action logging."""

import time
import requests
import pandas as pd
from typing import Optional, Dict, Any, List, Tuple

MONDAY_API_URL = "https://api.monday.com/v2"
API_VERSION = "2024-10"


class MondayClient:
    """Handles all communication with the Monday.com GraphQL API."""

    def __init__(self, api_token: str):
        self.api_token = api_token
        self.headers = {
            "Authorization": api_token,
            "Content-Type": "application/json",
            "API-Version": API_VERSION,
        }
        self.action_log: List[Dict[str, Any]] = []

    # ------------------------------------------------------------------ #
    #  Low-level API helpers                                               #
    # ------------------------------------------------------------------ #

    def _execute_query(self, query: str, variables: Optional[Dict] = None) -> Dict:
        """Execute a GraphQL query and return the parsed JSON response."""
        payload: Dict[str, Any] = {"query": query}
        if variables:
            payload["variables"] = variables

        start = time.time()
        self.action_log.append(
            {
                "action": "Monday.com API Call",
                "query_preview": query.strip()[:250],
                "timestamp": time.strftime("%H:%M:%S"),
            }
        )

        try:
            resp = requests.post(
                MONDAY_API_URL, json=payload, headers=self.headers, timeout=30
            )
            elapsed = round(time.time() - start, 2)

            if resp.status_code == 401:
                raise RuntimeError(
                    "Authentication failed — check your Monday.com API token."
                )
            if resp.status_code == 429:
                raise RuntimeError(
                    "Rate-limited by Monday.com. Wait a moment and try again."
                )
            resp.raise_for_status()

            result = resp.json()
            if "errors" in result:
                msgs = [e.get("message", str(e)) for e in result["errors"]]
                raise RuntimeError(f"Monday.com GraphQL error: {'; '.join(msgs)}")

            self.action_log[-1].update(response_time=f"{elapsed}s", status="success")
            return result

        except requests.exceptions.Timeout:
            self.action_log[-1]["status"] = "timeout"
            raise RuntimeError("Monday.com API request timed out (30 s).")
        except requests.exceptions.ConnectionError:
            self.action_log[-1]["status"] = "connection_error"
            raise RuntimeError(
                "Cannot reach Monday.com API — check your internet connection."
            )

    def validate_connection(self) -> bool:
        """Return True if the API token is valid."""
        try:
            r = self._execute_query("query { me { name } }")
            return "data" in r
        except Exception:
            return False

    # ------------------------------------------------------------------ #
    #  Board data fetching                                                 #
    # ------------------------------------------------------------------ #

    def fetch_board_items(
        self, board_id: str, limit: int = 500
    ) -> Tuple[Dict[str, Any], pd.DataFrame]:
        """
        Fetch every item from *board_id* (with cursor pagination).

        Returns
        -------
        metadata : dict   – board name, id, column list, row count
        df       : DataFrame – one row per item, columns = Monday column titles
        """
        first_query = (
            "query { boards(ids: [%s]) { name "
            "columns { id title type } "
            "items_page(limit: %d) { cursor items { id name "
            "column_values { id text } } } } }" % (board_id, min(limit, 500))
        )

        result = self._execute_query(first_query)
        boards = result.get("data", {}).get("boards", [])
        if not boards:
            raise RuntimeError(f"Board {board_id} not found — check the board ID.")

        board = boards[0]
        columns = board["columns"]
        col_map = {c["id"]: c["title"] for c in columns}

        page = board["items_page"]
        all_items: List[Dict] = list(page.get("items", []))
        cursor = page.get("cursor")

        while cursor and len(all_items) < limit:
            nq = (
                'query { next_items_page(limit: %d, cursor: "%s") '
                "{ cursor items { id name column_values { id text } } } }"
                % (min(limit - len(all_items), 500), cursor)
            )
            result = self._execute_query(nq)
            page = result.get("data", {}).get("next_items_page", {})
            all_items.extend(page.get("items", []))
            cursor = page.get("cursor")

        rows = []
        for item in all_items:
            row: Dict[str, Any] = {"Item Name": item["name"]}
            for cv in item.get("column_values", []):
                title = col_map.get(cv["id"], cv["id"])
                row[title] = cv.get("text", "")
            rows.append(row)

        df = pd.DataFrame(rows) if rows else pd.DataFrame()

        metadata = {
            "board_name": board["name"],
            "board_id": board_id,
            "total_items": len(all_items),
            "columns": [c["title"] for c in columns],
        }

        self.action_log.append(
            {
                "action": "Data extraction complete",
                "board": board["name"],
                "rows": len(df),
                "columns": len(df.columns) if not df.empty else 0,
                "timestamp": time.strftime("%H:%M:%S"),
            }
        )
        return metadata, df

    # ------------------------------------------------------------------ #
    #  Action-log helpers                                                  #
    # ------------------------------------------------------------------ #

    def clear_action_log(self) -> None:
        self.action_log = []

    def get_action_log(self) -> List[Dict[str, Any]]:
        return list(self.action_log)
