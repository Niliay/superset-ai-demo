import os
import json
import re
from typing import Dict, Any

import requests
from openai import OpenAI

SUPERSET_BASE_URL = os.getenv("SUPERSET_BASE_URL", "http://127.0.0.1:8088")
SUPERSET_USERNAME = os.getenv("SUPERSET_USERNAME", "admin")
SUPERSET_PASSWORD = os.getenv("SUPERSET_PASSWORD", "admin")
SUPERSET_DB_ID = int(os.getenv("SUPERSET_DB_ID", "2"))
SUPERSET_SCHEMA = os.getenv("SUPERSET_SCHEMA", "public")

OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5")


def superset_login_get_tokens(base_url: str, username: str, password: str) -> Dict[str, str]:
    url = f"{base_url}/api/v1/security/login"
    payload = {"username": username, "password": password, "provider": "db", "refresh": True}
    r = requests.post(url, json=payload, timeout=30)
    r.raise_for_status()
    data = r.json()
    return {"access_token": data["access_token"], "refresh_token": data.get("refresh_token", "")}


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

    # Superset'in bazı sürümleri bu alanları bekliyor
    body = {
        "client_id": "12345678901",       # 11 char id (random da olabilir)
        "database_id": database_id,
        "runAsync": False,
        "catalog": None,
        "schema": schema,
        "sql": sql,
        "sql_editor_id": "1",
        "tab": "chatgpt-demo",
        "tmp_table_name": "",
        "select_as_cta": False,
        "ctas_method": "TABLE",
        "queryLimit": limit,
        "expand_data": True,
    }

    r = s.post(url, json=body, timeout=120)

    # Hata olursa sebebini görelim diye:
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
    return text.strip().strip(";")


def openai_generate_sql(question: str, schema_hint: str) -> str:
    client = OpenAI()

    system = (
        "You are a senior data analyst. Generate ONE PostgreSQL query.\n"
        "Rules:\n"
        "- Read-only: ONLY SELECT (or WITH ... SELECT). No DDL/DML.\n"
        "- Prefer simple queries.\n"
        "- Always include a LIMIT 200 unless the query is an aggregate that returns few rows.\n"
        "- Use only tables/columns that exist in the provided schema context.\n"
        "- Return ONLY the SQL (no explanation).\n"
    )

    user = (
        f"Schema context:\n{schema_hint}\n\n"
        f"Question:\n{question}\n\n"
        "SQL:"
    )

    resp = client.responses.create(
        model=OPENAI_MODEL,
        input=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
    )

    out_text = ""
    for item in getattr(resp, "output", []) or []:
        for c in getattr(item, "content", []) or []:
            t = getattr(c, "type", "")
            if t in ("output_text", "text"):
                out_text += getattr(c, "text", "") or ""

    if not out_text:
        out_text = getattr(resp, "output_text", "") or ""

    return extract_sql(out_text)


def main():
    if not os.getenv("OPENAI_API_KEY"):
        raise SystemExit("OPENAI_API_KEY env var is missing. export OPENAI_API_KEY=...")

    print(f"[i] Superset: {SUPERSET_BASE_URL}")
    print("[i] Logging in to Superset API...")
    tokens = superset_login_get_tokens(SUPERSET_BASE_URL, SUPERSET_USERNAME, SUPERSET_PASSWORD)

    print("[i] Getting session cookie + CSRF...")
    session = superset_get_session_and_csrf(SUPERSET_BASE_URL, tokens["access_token"])

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

    schema_hint = (
        f"Database: PostgreSQL\n"
        f"Schema: {SUPERSET_SCHEMA}\n"
        f"Known tables: {', '.join([t for t in table_names if t])}"
    )

    print("\n[i] Ready. Example questions:")
    print('    "Gün gün toplam satış trendini getir"')
    print('    "En yüksek satış yapılan ilk 10 günü listele"')
    print('    "Kategori bazında toplam satışları getir"\n')

    question = input("Question> ").strip()
    if not question:
        print("[!] Empty question. Exiting.")
        return

    print("\n[i] Generating SQL with OpenAI...")
    sql = openai_generate_sql(question, schema_hint)

    print("\n--- SQL (model output) ---")
    print(sql)
    print("--------------------------\n")

    if not is_safe_select(sql):
        print("[!] Blocked: SQL is not a safe read-only SELECT/WITH query.")
        return

    print("[i] Executing in Superset SQL Lab...")
    res = superset_sqllab_execute(session, SUPERSET_BASE_URL, SUPERSET_DB_ID, SUPERSET_SCHEMA, sql, limit=1000)

    print(f"\n[i] Superset status: {res.get('status')}")
    data = res.get("data", []) or []
    print(f"[i] Rows returned: {len(data)}")

    print("\n--- First rows ---")
    for row in data[:10]:
        print(row)


if __name__ == "__main__":
    main()
