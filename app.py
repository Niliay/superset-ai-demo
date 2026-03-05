import os
import re
import uuid
from typing import Any, Dict, List, Tuple

import pandas as pd
import requests
import streamlit as st
from dotenv import load_dotenv
from groq import Groq

# Load .env from project root
load_dotenv(dotenv_path=".env")

# -------------------------
# ENV / CONFIG
# -------------------------
SUPERSET_BASE_URL = os.getenv("SUPERSET_BASE_URL", "http://127.0.0.1:8088").rstrip("/")
SUPERSET_USERNAME = os.getenv("SUPERSET_USERNAME")
SUPERSET_PASSWORD = os.getenv("SUPERSET_PASSWORD")

if not SUPERSET_USERNAME or not SUPERSET_PASSWORD:
    raise RuntimeError("Missing SUPERSET_USERNAME/SUPERSET_PASSWORD. Put them in .env (not committed).")

SUPERSET_DB_ID = int(os.getenv("SUPERSET_DB_ID", "2"))
SUPERSET_SCHEMA = os.getenv("SUPERSET_SCHEMA", "public")

GROQ_API_KEY = os.getenv("GROQ_API_KEY")  # already safe
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")

# -------------------------
# HELPERS
# -------------------------
def superset_login_get_tokens(base_url: str, username: str, password: str) -> Dict[str, str]:
    url = f"{base_url}/api/v1/security/login"
    payload = {"username": username, "password": password, "provider": "db", "refresh": True}
    r = requests.post(url, json=payload, timeout=30)
    r.raise_for_status()
    data = r.json()
    return {"access_token": data["access_token"]}


def superset_get_session_and_csrf(base_url: str, access_token: str) -> requests.Session:
    s = requests.Session()
    # create session cookie
    s.get(f"{base_url}/login/", timeout=30)

    # csrf tied to session + bearer
    r = s.get(
        f"{base_url}/api/v1/security/csrf_token/",
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=30,
    )
    r.raise_for_status()
    csrf = r.json()["result"]

    s.headers.update(
        {
            "Authorization": f"Bearer {access_token}",
            "X-CSRFToken": csrf,
            "Referer": f"{base_url}/",
            "Content-Type": "application/json",
        }
    )
    return s


def get_superset_session() -> requests.Session:
    tokens = superset_login_get_tokens(SUPERSET_BASE_URL, SUPERSET_USERNAME, SUPERSET_PASSWORD)
    return superset_get_session_and_csrf(SUPERSET_BASE_URL, tokens["access_token"])


def superset_sqllab_execute(
    s: requests.Session,
    base_url: str,
    database_id: int,
    schema: str,
    sql: str,
    limit: int = 1000,
    tab: str = "streamlit-chat",
) -> Dict[str, Any]:
    url = f"{base_url}/api/v1/sqllab/execute/"
    body = {
        "client_id": uuid.uuid4().hex[:11],  # UNIQUE each request
        "database_id": database_id,
        "runAsync": False,
        "catalog": None,
        "schema": schema,
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
        txt = r.text or ""
        raise RuntimeError(f"SQLLab error {r.status_code}: {txt[:1200]}")
    r.raise_for_status()
    return r.json()


def is_safe_select(sql: str) -> bool:
    cleaned = re.sub(r"/\*.*?\*/", "", sql, flags=re.S).strip().strip(";")
    low = cleaned.lower()
    forbidden = ["insert", "update", "delete", "drop", "alter", "truncate", "create", "grant", "revoke"]
    if any(re.search(rf"\b{kw}\b", low) for kw in forbidden):
        return False
    return low.startswith("select") or low.startswith("with")


def extract_sql(text: str) -> str:
    m = re.search(r"```sql\s*(.*?)```", text, flags=re.S | re.I)
    if m:
        return m.group(1).strip().strip(";")
    return (text or "").strip().strip(";")


def groq_generate_sql(question: str, schema_hint: str) -> str:
    if not GROQ_API_KEY:
        raise RuntimeError("GROQ_API_KEY missing. Put it in .env as GROQ_API_KEY=...")

    client = Groq(api_key=GROQ_API_KEY)

    instruction = (
        "Generate ONE PostgreSQL query.\n"
        "Rules:\n"
        "- Read-only: ONLY SELECT (or WITH ... SELECT). No DDL/DML.\n"
        "- Prefer simple queries.\n"
        "- Always include a LIMIT 200 unless the query is an aggregate that returns few rows.\n"
        "- Use only tables/columns that exist in the provided schema context.\n"
        "- IMPORTANT: The date column is NOT named 'date'. For sales_data use orderdate (date) or order_ts (timestamp).\n"
        "- If the user asks for daily (e.g., 'gün gün'), group by day using orderdate (or date_trunc('day', order_ts)).\n"
        "- If the user asks for monthly (e.g., 'ay ay'), group by month.\n"
        "- Return ONLY the SQL (no explanation).\n"
    )

    prompt = f"{instruction}\nSchema context:\n{schema_hint}\n\nQuestion:\n{question}\n\nSQL:"

    resp = client.chat.completions.create(
        model=GROQ_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
    )
    return extract_sql(resp.choices[0].message.content or "")


def normalize_rows(data: Any) -> Tuple[List[Dict[str, Any]], List[str]]:
    if not data:
        return [], []

    # dict rows
    if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
        cols = list(data[0].keys())
        return data, cols

    # list rows
    if isinstance(data, list) and len(data) > 0 and isinstance(data[0], (list, tuple)):
        cols = [f"col_{i}" for i in range(len(data[0]))]
        out = []
        for row in data:
            out.append({cols[i]: row[i] if i < len(row) else None for i in range(len(cols))})
        return out, cols

    return [], []


def fetch_table_names(session: requests.Session) -> List[str]:
    tables_sql = f"""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = '{SUPERSET_SCHEMA}'
    ORDER BY table_name
    LIMIT 50
    """
    tables_res = superset_sqllab_execute(
        session,
        SUPERSET_BASE_URL,
        SUPERSET_DB_ID,
        SUPERSET_SCHEMA,
        tables_sql,
        limit=50,
        tab="ui-schema",
    )

    table_names: List[str] = []
    for row in tables_res.get("data", []) or []:
        if isinstance(row, dict) and "table_name" in row:
            table_names.append(row["table_name"])
        elif isinstance(row, (list, tuple)) and row:
            table_names.append(str(row[0]))
    return table_names


def build_schema_hint(table_names: List[str]) -> str:
    # verified columns
    sales_data_cols = [
        "ordernumber", "quantityordered", "priceeach", "orderlinenumber", "sales",
        "order_ts", "orderdate", "status", "qtr_id", "month_id", "year_id",
        "productline", "msrp", "productcode", "customername", "phone",
        "addressline1", "addressline2", "city", "state", "postalcode",
        "country", "territory", "contactlastname", "contactfirstname", "dealsize"
    ]

    hint = (
        f"Database: PostgreSQL\n"
        f"Schema: {SUPERSET_SCHEMA}\n"
        f"Known tables: {', '.join([t for t in table_names if t])}\n\n"
        "Table: sales_data\n"
        f"Columns: {', '.join(sales_data_cols)}\n"
        "Notes: Use orderdate (date) or order_ts (timestamp) for time grouping. Use sales for revenue/amount."
    )
    return hint


def run_verification(session: requests.Session) -> Dict[str, Any]:
    report: Dict[str, Any] = {}

    q_total = "SELECT SUM(sales) AS grand_total_sales FROM sales_data;"
    res_total = superset_sqllab_execute(
        session, SUPERSET_BASE_URL, SUPERSET_DB_ID, SUPERSET_SCHEMA, q_total, limit=1, tab="ui-verify"
    )
    data_total = res_total.get("data", []) or []
    if data_total and isinstance(data_total[0], dict):
        grand_total = float(data_total[0].get("grand_total_sales"))
    elif data_total and isinstance(data_total[0], (list, tuple)):
        grand_total = float(data_total[0][0])
    else:
        grand_total = None

    q_sum_daily = """
    SELECT SUM(daily_total) AS sum_of_daily_totals
    FROM (
      SELECT orderdate, SUM(sales) AS daily_total
      FROM sales_data
      GROUP BY orderdate
    ) t;
    """
    res_daily = superset_sqllab_execute(
        session, SUPERSET_BASE_URL, SUPERSET_DB_ID, SUPERSET_SCHEMA, q_sum_daily, limit=1, tab="ui-verify2"
    )
    data_daily = res_daily.get("data", []) or []
    if data_daily and isinstance(data_daily[0], dict):
        sum_daily = float(data_daily[0].get("sum_of_daily_totals"))
    elif data_daily and isinstance(data_daily[0], (list, tuple)):
        sum_daily = float(data_daily[0][0])
    else:
        sum_daily = None

    report["grand_total_sales"] = grand_total
    report["sum_of_daily_totals"] = sum_daily
    report["totals_match"] = (grand_total == sum_daily)
    return report


def df_preview_markdown(df: pd.DataFrame, n: int = 15) -> str:
    if df.empty:
        return "_Sonuç boş döndü._"
    preview = df.head(n).copy()
    try:
        return preview.to_markdown(index=False)
    except Exception:
        return "```text\n" + preview.to_csv(index=False) + "\n```"


def render_last_result_panel():
    """Shows full dataframe + optional chart for the latest result."""
    if "last_df" not in st.session_state or st.session_state.last_df is None:
        return

    df = st.session_state.last_df
    if not isinstance(df, pd.DataFrame) or df.empty:
        return

    st.subheader("Last Result (full)")
    st.dataframe(df, use_container_width=True, height=360)

    # chart for orderdate
    if "orderdate" in df.columns:
        value_cols = [c for c in df.columns if c != "orderdate"]
        preferred = ["toplam_satışlar", "toplam_satis", "total_sales", "daily_total", "sum", "sales"]
        value_col = next((c for c in preferred if c in df.columns), None) or (value_cols[0] if value_cols else None)

        if value_col:
            chart_df = df.copy()
            chart_df["orderdate"] = pd.to_datetime(chart_df["orderdate"], errors="coerce")
            chart_df = chart_df.dropna(subset=["orderdate"])
            chart_df[value_col] = pd.to_numeric(chart_df[value_col], errors="coerce")
            chart_df = chart_df.dropna(subset=[value_col]).sort_values("orderdate").set_index("orderdate")
            if not chart_df.empty:
                st.line_chart(chart_df[[value_col]])


# -------------------------
# STREAMLIT UI (CHAT)
# -------------------------
st.set_page_config(page_title="Superset AI Assistant Demo (External)", layout="wide")
st.title("Superset AI Assistant Demo (External)")

# session state init
if "messages" not in st.session_state:
    st.session_state.messages = []  # {"role": "user"/"assistant", "content": str}
if "verify_report" not in st.session_state:
    st.session_state.verify_report = None
if "last_df" not in st.session_state:
    st.session_state.last_df = None
if "last_sql" not in st.session_state:
    st.session_state.last_sql = None
if "last_status" not in st.session_state:
    st.session_state.last_status = None

# Sidebar
with st.sidebar:
    st.header("Tools")
    st.caption("LLM bağımsız doğrulama (DB tutarlılık kanıtı)")

    if st.button("Run Verify", use_container_width=True):
        try:
            with st.spinner("Verify çalışıyor..."):
                session = get_superset_session()
                st.session_state.verify_report = run_verification(session)
        except Exception as e:
            st.session_state.verify_report = {"error": str(e)}

    if st.session_state.verify_report:
        st.subheader("Verification")
        st.json(st.session_state.verify_report)
        if st.session_state.verify_report.get("totals_match") is True:
            st.success("✅ DB tutarlı (totals_match: true)")

    st.divider()
    st.subheader("Debug")
    st.write(
        {
            "SUPERSET_BASE_URL": SUPERSET_BASE_URL,
            "SUPERSET_DB_ID": SUPERSET_DB_ID,
            "SUPERSET_SCHEMA": SUPERSET_SCHEMA,
            "GROQ_MODEL": GROQ_MODEL,
            "GROQ_API_KEY_present": bool(GROQ_API_KEY),
        }
    )

    if st.button("Clear Chat", use_container_width=True):
        st.session_state.messages = []
        st.session_state.last_df = None
        st.session_state.last_sql = None
        st.session_state.last_status = None
        st.rerun()

# Render chat history
for m in st.session_state.messages:
    with st.chat_message(m["role"]):
        st.markdown(m["content"])

# Input
user_msg = st.chat_input("Sorunu yaz… (örn: Ülke bazında toplam satışları getir)")

if user_msg:
    # 1) store user msg
    st.session_state.messages.append({"role": "user", "content": user_msg})

    # 2) compute answer, store to history, then rerun so user+assistant appear together
    try:
        with st.spinner("SQL üretiyorum ve Superset’te çalıştırıyorum..."):
            session = get_superset_session()
            table_names = fetch_table_names(session)
            schema_hint = build_schema_hint(table_names)

            sql = groq_generate_sql(user_msg, schema_hint)
            st.session_state.last_sql = sql

            if not is_safe_select(sql):
                raise RuntimeError("Blocked: güvenli SELECT/WITH değil.")

            res = superset_sqllab_execute(
                session,
                SUPERSET_BASE_URL,
                SUPERSET_DB_ID,
                SUPERSET_SCHEMA,
                sql,
                limit=1000,
                tab="ui-chat",
            )
            st.session_state.last_status = res.get("status")

            data = res.get("data", []) or []
            rows, _ = normalize_rows(data)
            df = pd.DataFrame(rows) if rows else pd.DataFrame()
            st.session_state.last_df = df

        history_answer = []
        history_answer.append("✅ Çalıştırdım. İşte sonuç:")
        history_answer.append("\n**SQL:**")
        history_answer.append(f"```sql\n{sql}\n```")
        history_answer.append(f"\n**status:** `{res.get('status')}`")
        history_answer.append("\n**Preview (ilk 15 satır):**")
        history_answer.append(df_preview_markdown(df, n=15))

        st.session_state.messages.append({"role": "assistant", "content": "\n".join(history_answer)})

    except Exception as e:
        st.session_state.messages.append({"role": "assistant", "content": f"❌ Hata: {e}"})

    # Critical: re-render page so the last user+assistant pair show up correctly
    st.rerun()

# Show latest full result (table + chart) as a panel
render_last_result_panel()