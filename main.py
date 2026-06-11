"""
game_discord_workflow.py
========================
Single-file rebuild of the n8n flow:  Schedule -> SQL (Neon/Postgres) -> Switch -> Discord.

INSTALL:
    pip install psycopg2-binary requests python-dotenv schedule

CONFIGURE (create a file named ".env" next to this script):
    DATABASE_URL=postgresql://user:pass@host/dbname?sslmode=require
    DISCORD_WEBHOOK_0=https://discord.com/api/webhooks/xxx/yyy
    DISCORD_WEBHOOK_1=https://discord.com/api/webhooks/xxx/yyy
    DISCORD_WEBHOOK_2=https://discord.com/api/webhooks/xxx/yyy
    SCHEDULE_MINUTES=5
    SEED_SAMPLE_DATA=true

RUN:
    python game_discord_workflow.py --inspect   # list your game's tables/columns
    python game_discord_workflow.py --once       # run the workflow one time (testing)
    python game_discord_workflow.py              # run on a schedule forever
"""
import os
import sys
import json
import time

import psycopg2
from psycopg2.extras import RealDictCursor
import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # .env support is optional; env vars still work

import schedule


# ===========================================================================
# CONFIG
# ===========================================================================
DATABASE_URL = os.getenv("DATABASE_URL")
DISCORD_WEBHOOKS = [
    os.getenv("DISCORD_WEBHOOK_0"),
    os.getenv("DISCORD_WEBHOOK_1"),
    os.getenv("DISCORD_WEBHOOK_2"),
]
SCHEDULE_MINUTES = int(os.getenv("SCHEDULE_MINUTES", "5"))
SEED_SAMPLE_DATA = os.getenv("SEED_SAMPLE_DATA", "false").lower() == "true"


def validate():
    if not DATABASE_URL:
        raise SystemExit("DATABASE_URL is not set. Create a .env file (see the header of this script).")
    if not any(DISCORD_WEBHOOKS):
        raise SystemExit("No DISCORD_WEBHOOK_* set. Add at least one to your .env file.")


# ===========================================================================
# DATABASE  == n8n "Execute a SQL query" node
# ===========================================================================
def get_connection():
    return psycopg2.connect(DATABASE_URL)


def run_query(sql, params=None):
    """Run a SELECT, return rows as a list of dicts."""
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params or ())
            rows = cur.fetchall()
    return [dict(r) for r in rows]


def execute(sql, params=None):
    """Run a write statement (INSERT/UPDATE/DDL) and commit."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
        conn.commit()


def seed_sample_data():
    """Create + seed a demo 'players' table so the app runs out of the box.
    Delete this once you point at your real game table."""
    execute(
        """
        CREATE TABLE IF NOT EXISTS players (
            id          SERIAL PRIMARY KEY,
            username    TEXT NOT NULL,
            score       INTEGER NOT NULL DEFAULT 0,
            notified_at TIMESTAMPTZ
        );
        """
    )
    if run_query("SELECT COUNT(*) AS n FROM players;")[0]["n"] == 0:
        execute(
            """
            INSERT INTO players (username, score) VALUES
                ('Alice', 1500), ('Bob', 420), ('Carol', 90),
                ('Dave', 1100), ('Eve', 10);
            """
        )
        print("Seeded sample 'players' table.")


# ===========================================================================
# DISCORD  == n8n "Send a message" nodes
# ===========================================================================
def send_message(branch_index, content, embed=None):
    """Post to the webhook for the given Switch branch (0, 1, 2...)."""
    url = DISCORD_WEBHOOKS[branch_index]
    if not url:
        print(f"[skip] No webhook configured for branch {branch_index}")
        return

    payload = {"content": content}
    if embed:
        payload["embeds"] = [embed]

    resp = requests.post(url, json=payload, timeout=15)
    if resp.status_code not in (200, 204):
        print(f"[error] Discord branch {branch_index}: {resp.status_code} {resp.text}")
    else:
        print(f"[ok] sent to branch {branch_index}: {content[:60]}")


# ===========================================================================
# WORKFLOW  == "Switch" node (Rules) + orchestration
# Customize SQL_QUERY and route() to match YOUR game.
# ===========================================================================
SQL_QUERY = """
    SELECT id, username, score
    FROM players
    ORDER BY score DESC;
"""


def route(row):
    """The Switch. First match wins. Return (branch_index, message) or None to skip."""
    score = row["score"]

    if score >= 1000:
        return 0, f"🏆 **{row['username']}** is on fire with {score} points!"
    if score >= 100:
        return 1, f"⭐ {row['username']} is doing well: {score} points."
    return 2, f"👋 {row['username']} just getting started ({score} points)."


def run_once():
    print("--- workflow run start ---")
    rows = run_query(SQL_QUERY)
    print(f"Query returned {len(rows)} row(s).")
    for row in rows:
        decision = route(row)
        if decision is None:
            continue
        branch_index, message = decision
        send_message(branch_index, message)
    print("--- workflow run end ---")


# ===========================================================================
# INSPECTOR  -- discover your game's real tables/columns
# ===========================================================================
def inspect():
    validate()
    tables = run_query(
        """
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public' ORDER BY table_name;
        """
    )
    if not tables:
        print("No tables in 'public' schema. Is this the right database?")
        return

    print(f"Found {len(tables)} table(s):\n")
    for t in tables:
        name = t["table_name"]
        print("=" * 60)
        print(f"TABLE: {name}")
        columns = run_query(
            """
            SELECT column_name, data_type FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position;
            """,
            (name,),
        )
        print("  columns:")
        for c in columns:
            print(f"    - {c['column_name']} ({c['data_type']})")
        count = run_query(f'SELECT COUNT(*) AS n FROM "{name}";')[0]["n"]
        print(f"  rows: {count}")
        print("  sample:")
        for row in run_query(f'SELECT * FROM "{name}" LIMIT 3;'):
            print("    " + json.dumps(row, default=str, ensure_ascii=False))
        print()


# ===========================================================================
# ENTRYPOINT  == n8n "Schedule Trigger" node
# ===========================================================================
def main():
    if "--inspect" in sys.argv:
        inspect()
        return

    validate()
    if SEED_SAMPLE_DATA:
        seed_sample_data()

    if "--once" in sys.argv:
        run_once()
        return

    print(f"Scheduler started: every {SCHEDULE_MINUTES} min. Ctrl+C to stop.")
    run_once()  # run immediately, then on the interval
    schedule.every(SCHEDULE_MINUTES).minutes.do(run_once)
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped.")
