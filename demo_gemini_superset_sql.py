import os
import re
import uuid
from typing import Dict, Any

import requests
from dotenv import load_dotenv
from google import genai

load_dotenv()

SUPERSET_BASE_URL = os.getenv("SUPERSET_BASE_URL", "http://127.0.0.1:8088")
SUPERSET_USERNAME = os.getenv("SUPERSET_USERNAME", "admin")
SUPERSET_PASSWORD = os.getenv("SUPERSET_PASSWORD", "admin")
SUPERSET_DB_ID = int(os.getenv("SUPERSET_DB_ID", "2"))
SUPERSET_SCHEMA = os.getenv("SUPERSET_SCHEMA", "public")

GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")


def superset_login_get_tokens(base_url: str, username: str, password: str) -> Dict[str, str]:
    url = f"{base_url}/api/v1/security/login"
    payload = {"username": username, "password": password, "provider": "db", "refresh": True}
    r = requests.post(url, json=payload, timeout=30)
    r.raise_for_status()
    data = r.json()
    return {"access_token": data["access_token"]}


def superset_get_session_and_csrf(base_url: str, access_token: str) -> requests.Session:
    s = requests.Session()
    s.get(f"{base_url}/login/", timeout=30)

    r = s.get(
        f"{base_url}/api/v1/security/csrf_token/",
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=30,
    )
    r.raise_for_status()
    csrf = r.json()["result"]

    s.headers.update({
        "Authorization": f"Bearer {access_token}",
        "X-CSRFToken": csrf,
        "Referer": f"{base_url}/",
        "Content-Type": "application/json",
    })
    return s


def superset_sqllab_execute(
    s: requests.Session,
    base_url: str,
    database_id: int,
    schema: str,
    sql: str,
    limit: int = 1000,
) -> Dict[str, Any]:
    url = f"{base_url}/api/v1/sqllab/execute/"
    body = {
        "client_id": uuid.uuid4().hex[:11],
        "database_id": database_id,
        "runAsync": False,
        "catalog": None,
        "schema": schema,
        "sql": sql,
        "sql_editor_id": "1",
        "tab": "gemini-demo",
        "tmp_table_name": "",
        "select_as_cta": False,
        "ctas_method": "TABLE",
        "queryLimit": limit,
        "expand_data": True,
    }
    r = s.post(url, json=body, timeout=120)
    if r.status_code >= 400:
        print("[!] SQLLab error status:", r.status_code)
        print("[!] SQLLab error body:", r.text)
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


def gemini_generate_sql(question: str, schema_hint: str) -> str:
    key = os.getenv("GEMINI_API_KEY")
    if not key:
        raise SystemExit("GEMINI_API_KEY missing. Put it in .env")

    client = genai.Client(api_key=key)

    instruction = (
        "Generate ONE PostgreSQL query.\n"
        "Rules:\n"
        "- Read-only: ONLY SELECT (or WITH ... SELECT). No DDL/DML.\n"
        "- Prefer simple queries.\n"
        "- Always include a LIMIT 200 unless the query is an aggregate that returns few rows.\n"
        "- Use only tables/columns that exist in the provided schema context.\n"
        "- Return ONLY the SQL (no explanation).\n"
    )

    prompt = f"{instruction}\nSchema context:\n{schema_hint}\n\nQuestion:\n{question}\n\nSQL:"
    resp = client.models.generate_content(model=GEMINI_MODEL, contents=prompt)
    return extract_sql(resp.text)


def main():
    print(f"[i] Superset: {SUPERSET_BASE_URL}")
    tokens = superset_login_get_tokens(SUPERSET_BASE_URL, SUPERSET_USERNAME, SUPERSET_PASSWORD)
    session = superset_get_session_and_csrf(SUPERSET_BASE_URL, tokens["access_token"])

    # tablo isimleri (ipucu)
    tables_sql = f"""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = '{SUPERSET_SCHEMA}'
    ORDER BY table_name
    LIMIT 50
    """
    tables_res = superset_sqllab_execute(session, SUPERSET_BASE_URL, SUPERSET_DB_ID, SUPERSET_SCHEMA, tables_sql, limit=50)

    table_names = []
    for row in tables_res.get("data", []) or []:
        if isinstance(row, dict) and "table_name" in row:
            table_names.append(row["table_name"])
        elif isinstance(row, (list, tuple)) and row:
            table_names.append(row[0])

    schema_hint = f"Schema: {SUPERSET_SCHEMA}\nKnown tables: {', '.join([t for t in table_names if t])}"

    question = input("Question> ").strip()
    if not question:
        return

    print("[i] Generating SQL with Gemini...")
    sql = gemini_generate_sql(question, schema_hint)

    print("\n--- SQL (model output) ---")
    print(sql)
    print("--------------------------\n")

    if not is_safe_select(sql):
        print("[!] Blocked: SQL is not a safe read-only SELECT/WITH query.")
        return

    print("[i] Executing in Superset SQL Lab...")
    res = superset_sqllab_execute(session, SUPERSET_BASE_URL, SUPERSET_DB_ID, SUPERSET_SCHEMA, sql, limit=1000)

    print(f"[i] Superset status: {res.get('status')}")
    data = res.get("data", []) or []
    print(f"[i] Rows returned: {len(data)}")
    print("--- First rows ---")
    for row in data[:10]:
        print(row)


if __name__ == "__main__":
    main()