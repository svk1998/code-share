# “””
VoC Z-Score Anomaly Pipeline

Flow:

1. Insert new weekly VoC counts
1. Fetch baseline (μ, σ) from DB
1. Compute Z-score
1. Classify alert level
1. Log result + mark anomaly if needed
1. Recompute baseline (rolling 12-week, excluding anomalies)

Requirements:
pip install mysql-connector-python python-dotenv
“””

import os
import logging
from datetime import date, timedelta
from dataclasses import dataclass
from typing import Optional

import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
level=logging.INFO,
format=”%(asctime)s [%(levelname)s] %(message)s”
)
log = logging.getLogger(**name**)

# ─────────────────────────────────────────────

# Config

# ─────────────────────────────────────────────

DB_CONFIG = {
“host”:     os.getenv(“DB_HOST”, “localhost”),
“port”:     int(os.getenv(“DB_PORT”, 3306)),
“user”:     os.getenv(“DB_USER”, “root”),
“password”: os.getenv(“DB_PASSWORD”, “”),
“database”: os.getenv(“DB_NAME”, “voc_db”),
}

# Thresholds

Z_CRITICAL = 3.0
Z_WARNING  = 2.0
WINDOW_WEEKS = 12  # rolling baseline window

# ─────────────────────────────────────────────

# Data classes

# ─────────────────────────────────────────────

@dataclass
class VoCEntry:
week_start: date   # always Monday
product:    str
category:   str
count:      int

@dataclass
class Baseline:
mu:    float
sigma: float
n_weeks: int

@dataclass
class ZResult:
zscore:      float
alert_level: str   # “normal” | “warning” | “critical”
is_anomaly:  bool

# ─────────────────────────────────────────────

# DB connection

# ─────────────────────────────────────────────

def get_connection():
return mysql.connector.connect(**DB_CONFIG)

# ─────────────────────────────────────────────

# Step 1 — Insert raw weekly count

# ─────────────────────────────────────────────

def insert_voc_count(entry: VoCEntry, conn) -> None:
“””
Upsert weekly VoC count.
is_anomaly defaults to FALSE — updated later after Z-score check.
“””
sql = “””
INSERT INTO voc_weekly_counts
(week_start, product, category, count, is_anomaly)
VALUES (%s, %s, %s, %s, FALSE)
ON DUPLICATE KEY UPDATE
count = VALUES(count),
is_anomaly = FALSE
“””
with conn.cursor() as cur:
cur.execute(sql, (entry.week_start, entry.product, entry.category, entry.count))
conn.commit()
log.info(f”Inserted: {entry.product}/{entry.category} week={entry.week_start} count={entry.count}”)

# ─────────────────────────────────────────────

# Step 2 — Fetch baseline

# ─────────────────────────────────────────────

def fetch_baseline(product: str, category: str, conn) -> Optional[Baseline]:
“””
Returns stored μ and σ for this product+category.
Returns None if no baseline exists yet (need ≥ 8 weeks first).
“””
sql = “””
SELECT mu, sigma, n_weeks
FROM voc_baseline
WHERE product = %s AND category = %s
LIMIT 1
“””
with conn.cursor() as cur:
cur.execute(sql, (product, category))
row = cur.fetchone()

```
if not row:
    log.warning(f"No baseline for {product}/{category}. Skipping Z-score.")
    return None

mu, sigma, n_weeks = row

if sigma == 0:
    log.warning(f"σ=0 for {product}/{category}. Not enough variance for Z-score.")
    return None

return Baseline(mu=float(mu), sigma=float(sigma), n_weeks=n_weeks)
```

# ─────────────────────────────────────────────

# Step 3+4 — Compute Z-score + classify

# ─────────────────────────────────────────────

def compute_zscore(count: int, baseline: Baseline) -> ZResult:
“””
Z = (x - μ) / σ
Classifies into normal / warning / critical.
“””
z = (count - baseline.mu) / baseline.sigma

```
abs_z = abs(z)
if abs_z > Z_CRITICAL:
    level = "critical"
    is_anomaly = True
elif abs_z > Z_WARNING:
    level = "warning"
    is_anomaly = False   # warning → log but don't auto-exclude from baseline
else:
    level = "normal"
    is_anomaly = False

return ZResult(zscore=round(z, 4), alert_level=level, is_anomaly=is_anomaly)
```

# ─────────────────────────────────────────────

# Step 5 — Log Z-score + mark anomaly

# ─────────────────────────────────────────────

def log_zscore(entry: VoCEntry, baseline: Baseline, result: ZResult, conn) -> None:
“””
Insert into zscore audit log.
“””
sql = “””
INSERT INTO voc_zscore_log
(week_start, product, category, count,
mu_used, sigma_used, zscore, alert_level)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
“””
with conn.cursor() as cur:
cur.execute(sql, (
entry.week_start, entry.product, entry.category, entry.count,
baseline.mu, baseline.sigma, result.zscore, result.alert_level,
))
conn.commit()
log.info(f”Z-score logged: Z={result.zscore} level={result.alert_level}”)

def mark_anomaly(entry: VoCEntry, conn) -> None:
“””
Flag this week as anomaly in raw counts table.
This excludes it from future baseline recomputes.
“””
sql = “””
UPDATE voc_weekly_counts
SET is_anomaly = TRUE
WHERE week_start = %s AND product = %s AND category = %s
“””
with conn.cursor() as cur:
cur.execute(sql, (entry.week_start, entry.product, entry.category))
conn.commit()
log.warning(
f”ANOMALY FLAGGED: {entry.product}/{entry.category} “
f”week={entry.week_start} count={entry.count}”
)

# ─────────────────────────────────────────────

# Step 6 — Recompute baseline (rolling window)

# ─────────────────────────────────────────────

def recompute_baseline(product: str, category: str, conn) -> None:
“””
Recomputes μ and σ from the last WINDOW_WEEKS non-anomalous weeks.
Skips if fewer than 8 clean weeks are available.
Upserts into voc_baseline.
“””
cutoff = date.today() - timedelta(weeks=WINDOW_WEEKS)

```
# Fetch clean weeks in window
sql_fetch = """
    SELECT count
    FROM voc_weekly_counts
    WHERE product = %s
      AND category = %s
      AND is_anomaly = FALSE
      AND week_start >= %s
    ORDER BY week_start ASC
"""
with conn.cursor() as cur:
    cur.execute(sql_fetch, (product, category, cutoff))
    rows = cur.fetchall()

counts = [r[0] for r in rows]

if len(counts) < 8:
    log.warning(
        f"Only {len(counts)} clean weeks for {product}/{category}. "
        f"Need ≥ 8 to recompute baseline. Skipping."
    )
    return

mu    = sum(counts) / len(counts)
sigma = (sum((x - mu) ** 2 for x in counts) / len(counts)) ** 0.5

# Recompute window_start / window_end
sql_dates = """
    SELECT MIN(week_start), MAX(week_start)
    FROM voc_weekly_counts
    WHERE product = %s
      AND category = %s
      AND is_anomaly = FALSE
      AND week_start >= %s
"""
with conn.cursor() as cur:
    cur.execute(sql_dates, (product, category, cutoff))
    window_start, window_end = cur.fetchone()

sql_upsert = """
    INSERT INTO voc_baseline
        (product, category, mu, sigma, window_start, window_end, n_weeks)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        mu           = VALUES(mu),
        sigma        = VALUES(sigma),
        window_start = VALUES(window_start),
        window_end   = VALUES(window_end),
        n_weeks      = VALUES(n_weeks)
"""
with conn.cursor() as cur:
    cur.execute(sql_upsert, (
        product, category,
        round(mu, 4), round(sigma, 4),
        window_start, window_end,
        len(counts),
    ))
conn.commit()
log.info(
    f"Baseline updated: {product}/{category} "
    f"μ={mu:.2f} σ={sigma:.2f} n={len(counts)} "
    f"window={window_start}→{window_end}"
)
```

# ─────────────────────────────────────────────

# Master pipeline — call this every week

# ─────────────────────────────────────────────

def process_weekly_voc(entry: VoCEntry) -> dict:
“””
Full pipeline for one VoC entry.
Returns a summary dict with Z-score result.
“””
result_summary = {
“entry”:    entry,
“baseline”: None,
“result”:   None,
}

```
try:
    conn = get_connection()

    # 1. Insert raw count
    insert_voc_count(entry, conn)

    # 2. Fetch baseline
    baseline = fetch_baseline(entry.product, entry.category, conn)
    if baseline is None:
        # No baseline yet — just insert, skip scoring
        # Still try to recompute baseline in case we just crossed 8 weeks
        recompute_baseline(entry.product, entry.category, conn)
        return result_summary

    result_summary["baseline"] = baseline

    # 3+4. Compute Z + classify
    result = compute_zscore(entry.count, baseline)
    result_summary["result"] = result

    # 5. Log + optionally mark anomaly
    log_zscore(entry, baseline, result, conn)

    if result.is_anomaly:
        mark_anomaly(entry, conn)
        log.warning(
            f"[ALERT] {result.alert_level.upper()} — "
            f"{entry.product}/{entry.category} Z={result.zscore}"
        )
    else:
        # 6. Normal or warning → safe to roll into baseline
        recompute_baseline(entry.product, entry.category, conn)

    return result_summary

except Error as e:
    log.error(f"DB error: {e}")
    raise
finally:
    if conn.is_connected():
        conn.close()
```

# ─────────────────────────────────────────────

# Batch processing — full week across all

# product + category combos

# ─────────────────────────────────────────────

def process_weekly_batch(entries: list[VoCEntry]) -> None:
“””
Process a full week of VoC data across all products and categories.
Use this as your Monday morning cron entry point.
“””
log.info(f”Starting weekly batch — {len(entries)} entries”)

```
for entry in entries:
    summary = process_weekly_voc(entry)
    result  = summary.get("result")

    if result:
        emoji = {"normal": "✅", "warning": "⚠️", "critical": "🚨"}.get(result.alert_level, "")
        print(
            f"{emoji}  {entry.product:<15} {entry.category:<15} "
            f"count={entry.count:<5} Z={result.zscore:>7.3f}  [{result.alert_level.upper()}]"
        )
    else:
        print(f"⏳  {entry.product:<15} {entry.category:<15} count={entry.count}  [NO BASELINE YET]")

log.info("Batch complete.")
```

# ─────────────────────────────────────────────

# Example usage

# ─────────────────────────────────────────────

if **name** == “**main**”:
this_monday = date.today() - timedelta(days=date.today().weekday())

```
# Simulated incoming data for this week
# Replace with your actual ETL / query result
incoming = [
    VoCEntry(this_monday, "App",     "Complaint",       48),  # likely spike
    VoCEntry(this_monday, "App",     "Feature Request", 15),
    VoCEntry(this_monday, "App",     "Praise",          20),
    VoCEntry(this_monday, "App",     "Bug",             9),
    VoCEntry(this_monday, "Website", "Complaint",       12),
    VoCEntry(this_monday, "Website", "Bug",             5),
]

process_weekly_batch(incoming)
```
-- 1. Raw weekly VoC counts (source of truth)
CREATE TABLE voc_weekly_counts (
  id           INT AUTO_INCREMENT PRIMARY KEY,
  week_start   DATE NOT NULL,          -- always Monday
  product      VARCHAR(100),
  category     VARCHAR(100),           -- Complaint, Bug, etc.
  count        INT NOT NULL DEFAULT 0,
  is_anomaly   BOOLEAN DEFAULT FALSE,  -- flag BEFORE inserting into baseline
  created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_week_cat (week_start, product, category)
);

-- 2. Baseline stats (μ and σ per category)
CREATE TABLE voc_baseline (
  id            INT AUTO_INCREMENT PRIMARY KEY,
  product       VARCHAR(100),
  category      VARCHAR(100),
  mu            DECIMAL(10,4) NOT NULL,
  sigma         DECIMAL(10,4) NOT NULL,
  window_start  DATE NOT NULL,         -- oldest week in window
  window_end    DATE NOT NULL,         -- newest week in window
  n_weeks       INT NOT NULL,          -- how many weeks in this baseline
  updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_prod_cat (product, category)
);

-- 3. Z-score log (audit trail per week)
CREATE TABLE voc_zscore_log (
  id           INT AUTO_INCREMENT PRIMARY KEY,
  week_start   DATE NOT NULL,
  product      VARCHAR(100),
  category     VARCHAR(100),
  count        INT,
  mu_used      DECIMAL(10,4),
  sigma_used   DECIMAL(10,4),
  zscore       DECIMAL(10,4),
  alert_level  ENUM('normal','warning','critical'),
  created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

