"""Inserts fake banking activity into Postgres so Debezium has changes to capture."""
import random
import time

import psycopg2

conn = psycopg2.connect(host="localhost", port=5433, dbname="banking", user="postgres", password="postgres")
cur = conn.cursor()

account_ids = []
for i in range(5):
    cur.execute("INSERT INTO customers (name, email) VALUES (%s, %s) RETURNING id",
                (f"Customer {i}", f"customer{i}_{int(time.time())}@bank.test"))
    cur.execute("INSERT INTO accounts (customer_id, balance) VALUES (%s, 1000) RETURNING id", (cur.fetchone()[0],))
    account_ids.append(cur.fetchone()[0])
conn.commit()

while True:  # Ctrl+C to stop
    acc = random.choice(account_ids)
    kind = random.choice(["deposit", "withdrawal"])
    amount = round(random.uniform(5, 200), 2)
    sign = 1 if kind == "deposit" else -1
    try:
        cur.execute("UPDATE accounts SET balance = balance + %s WHERE id = %s", (sign * amount, acc))
        cur.execute("INSERT INTO transactions (account_id, type, amount) VALUES (%s, %s, %s)", (acc, kind, amount))
        conn.commit()
    except psycopg2.Error:  # e.g. constraint failure: skip this tx, keep generating
        conn.rollback()
    time.sleep(1)
