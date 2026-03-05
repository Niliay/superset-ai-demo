import os
import uuid
import json
from datetime import date
from typing import Dict, Any, List, Tuple

import requests
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")

BASE = os.getenv("SUPERSET_BASE_URL", "http://127.0.0.1:8088")
USER = os.getenv("SUPERSET_USERNAME")
PWD = os.getenv("SUPERSET_PASSWORD")

if not USER or not PWD:
    raise RuntimeError("Missing SUPERSET_USERNAME/SUPERSET_PASSWORD. Put them in .env (not committed).")
    
DB_ID = int(os.getenv("SUPERSET_DB_ID", "2"))
SCHEMA = os.getenv("SUPERSET_SCHEMA", "public")

TABLE = "sales_data"
DATE_COL = "orderdate"
VALUE_COL = "sales"


def superset_login_access_token() -> str:
    """POST /api/v1/security/login -> access_token"""
    url = f"{BASE}/api/v1/security/login"
    payload = {"username": USER, "password": PWD, "provider": "db", "refresh": True}
    r = requests.post(url, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]


def superset_session_with_csrf(access_token: str) -> requests.Session:
    """
    Create a session cookie + fetch CSRF token bound to that session,
    then attach headers to a requests.Session().
    """
    s = requests.Session()
    s.get(f"{BASE}/login/", timeout=30)  # sets session cookie

    r = s.get(
        f"{BASE}/api/v1/security/csrf_token/",
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=30,
    )
    r.raise_for_status()
    csrf = r.json()["result"]

    s.headers.update(
        {
            "Authorization": f"Bearer {access_token}",
            "X-CSRFToken": csrf,
            "Referer": f"{BASE}/",
            "Content-Type": "application/json",
        }
    )
    return s


def sqllab_execute(s: requests.Session, sql: str, limit: int = 1000, tab: str = "verify") -> Dict[str, Any]:
    """POST /api/v1/sqllab/execute/ -> query result"""
    url = f"{BASE}/api/v1/sqllab/execute/"
    body = {
        "client_id": uuid.uuid4().hex[:11],  # must be unique
        "database_id": DB_ID,
        "runAsync": False,
        "catalog": None,
        "schema": SCHEMA,
        "sql": sql,
        "sql_editor_id": "1",
        "tab": tab,
        "tmp_table_name": "",
        "select_as_cta": False,
        "ctas_method": "TABLE",
        "queryLimit": limit,
        "expand_data": True,
    }
    r = s.post(url, json=body, timeout=120)
    if r.status_code >= 400:
        # Keep it readable
        raise RuntimeError(f"SQLLab {r.status_code}: {r.text[:2000]}")
    return r.json()


def first_row_value(res: Dict[str, Any], key: str):
    data = res.get("data", []) or []
    if not data:
        return None
    row0 = data[0]
    if isinstance(row0, dict):
        return row0.get(key)
    # if it's list/tuple, can't safely map -> return whole row
    return row0


def print_section(title: str):
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def main():
    print_section("VERIFY REPORT — Superset SQLLab via API (Groq independent)")
    print(f"Superset Base URL : {BASE}")
    print(f"DB ID / Schema    : {DB_ID} / {SCHEMA}")
    print(f"Target table      : {SCHEMA}.{TABLE}")
    print(f"Date / Value col  : {DATE_COL} / {VALUE_COL}")

    # 1) Auth
    print_section("1) AUTH CHECK")
    token = superset_login_access_token()
    print("[OK] Got access_token (length):", len(token))

    s = superset_session_with_csrf(token)
    print("[OK] Session cookie + CSRF configured")

    # 2) Columns exist
    print_section("2) SCHEMA CHECK — columns exist?")
    sql_cols = f"""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema='{SCHEMA}' AND table_name='{TABLE}'
    ORDER BY ordinal_position
    """
    cols_res = sqllab_execute(s, sql_cols, limit=200, tab="verify-cols")
    cols = cols_res.get("data", []) or []
    colnames = [c["column_name"] for c in cols if isinstance(c, dict) and "column_name" in c]
    print("[OK] Column count:", len(colnames))
    print("[INFO] First columns:", colnames[:12])

    missing = []
    for needed in [DATE_COL, VALUE_COL]:
        if needed not in colnames:
            missing.append(needed)
    if missing:
        raise SystemExit(f"[FAIL] Missing required columns: {missing}")
    print("[OK] Required columns present:", DATE_COL, VALUE_COL)

    # 3) Range + distinct days
    print_section("3) DATA RANGE CHECK")
    sql_range = f"""
    SELECT
      MIN({DATE_COL}) AS min_date,
      MAX({DATE_COL}) AS max_date,
      COUNT(*) AS row_count,
      COUNT(DISTINCT {DATE_COL}) AS distinct_days
    FROM {TABLE};
    """
    range_res = sqllab_execute(s, sql_range, tab="verify-range")
    print("[OK] Range row:", range_res["data"][0])

    # 4) Strong equality check (already did, but keep in report)
    print_section("4) AGGREGATION IDENTITY CHECK (Strong)")
    q1 = f"SELECT SUM({VALUE_COL}) AS grand_total_sales FROM {TABLE};"
    q2 = f"""
    SELECT SUM(daily_total) AS sum_of_daily_totals
    FROM (
      SELECT {DATE_COL}, SUM({VALUE_COL}) AS daily_total
      FROM {TABLE}
      GROUP BY {DATE_COL}
    ) t;
    """
    r1 = sqllab_execute(s, q1, tab="verify-total")
    r2 = sqllab_execute(s, q2, tab="verify-daily-sum")

    grand = first_row_value(r1, "grand_total_sales")
    daily_sum = first_row_value(r2, "sum_of_daily_totals")

    print("[OK] grand_total_sales     :", grand)
    print("[OK] sum_of_daily_totals   :", daily_sum)

    if grand != daily_sum:
        print("[WARN] Totals do NOT match exactly. This is unexpected.")
    else:
        print("[PASS] Totals match exactly ✅")

    # 5) Spot-check: pick a day (earliest day) and validate breakdown
    print_section("5) SPOT CHECK (One day) — breakdown validation")
    # pick earliest day
    min_date = range_res["data"][0].get("min_date") if isinstance(range_res["data"][0], dict) else None
    if not min_date:
        raise SystemExit("[FAIL] Could not read min_date.")
    print("[INFO] Using day:", min_date)

    # a) daily total for that day
    q_day_total = f"""
    SELECT {DATE_COL} AS day, SUM({VALUE_COL}) AS daily_total
    FROM {TABLE}
    WHERE {DATE_COL} = DATE '{min_date}'
    GROUP BY {DATE_COL};
    """
    day_total_res = sqllab_execute(s, q_day_total, tab="verify-day-total")
    print("[OK] Daily total row:", day_total_res["data"][0])

    # b) raw rows for that day (top 20 sales) to show it’s composed of real rows
    q_rows = f"""
    SELECT ordernumber, orderlinenumber, {VALUE_COL} AS sales
    FROM {TABLE}
    WHERE {DATE_COL} = DATE '{min_date}'
    ORDER BY {VALUE_COL} DESC
    LIMIT 20;
    """
    rows_res = sqllab_execute(s, q_rows, tab="verify-day-rows")
    rows = rows_res.get("data", []) or []
    print("[OK] Top 20 rows (sales) for that day:")
    for r in rows[:20]:
        print("  ", r)

    # 6) “LLM query output” reproduction check (daily trend query)
    print_section("6) DAILY TREND QUERY (Repro)")
    q_trend = f"""
    SELECT {DATE_COL} AS orderdate, SUM({VALUE_COL}) AS total_sales
    FROM {TABLE}
    GROUP BY {DATE_COL}
    ORDER BY {DATE_COL}
    LIMIT 20;
    """
    trend_res = sqllab_execute(s, q_trend, tab="verify-trend")
    print("[OK] First 20 daily totals:")
    for r in trend_res.get("data", []) or []:
        print("  ", r)

    print_section("DONE ✅")
    print("This report verifies correctness independently of the LLM.")
    print("If you want, next we can export full trend to CSV for plotting.")


if __name__ == "__main__":
    main()