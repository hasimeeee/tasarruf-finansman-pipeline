"""
Tasarruf Finansman - ETL Pipeline
Hafta 2: Staging -> Star Schema
Transform fonksiyonları: src/transformers.py
"""

import yaml
import psycopg2
from psycopg2.extras import execute_values
from datetime import date, timedelta
import time
import logging
import os
from dateutil.relativedelta import relativedelta
from transformers import (
    transform_dim_date_record,
    transform_dim_plan_record,
    transform_dim_member_record,
    transform_fact_payment_record,
    transform_fact_lottery_record,
)

# LOGLAMA: hem konsol hem dosya
os.makedirs("logs", exist_ok=True)

log = logging.getLogger("etl_pipeline")
log.setLevel(logging.INFO)

# Konsol handler
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

# Dosya handler
fh = logging.FileHandler("logs/pipeline.log", encoding="utf-8")
fh.setLevel(logging.INFO)
fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

log.addHandler(ch)
log.addHandler(fh)

# CONFIG & BAĞLANTI
import os
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
with open(os.path.join(BASE_DIR, "config.yaml"), "r") as f:
    config = yaml.safe_load(f)

_db = config["database"].copy()
if "name" in _db:
    _db["dbname"] = _db.pop("name")
DB = _db


def get_conn():
    return psycopg2.connect(**DB)


def log_pipeline_run(conn, stage: str, status: str, rows: int = 0,
                     duration: float = 0.0, error: str = None):
    """pipeline_runs tablosuna kayıt atar."""
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO pipeline_runs (stage, status, rows_inserted, duration_sec, error_msg)
        VALUES (%s, %s, %s, %s, %s)
    """, (stage, status, rows, duration, error))
    conn.commit()


# LOAD FONKSİYONLARI

def load_dim_date(conn):
    log.info("dim_date yukleniyor...")
    start = date(2021, 1, 1)
    end   = date(2028, 12, 31)

    records = []
    current = start
    while current <= end:
        records.append(transform_dim_date_record(current))
        current += timedelta(days=1)

    cur = conn.cursor()
    cur.execute("DELETE FROM dim_date")
    execute_values(cur, """
        INSERT INTO dim_date
        (date_key, full_date, day, month, quarter, year,
         day_of_week, is_weekend, is_holiday, is_ramadan)
        VALUES %s
        ON CONFLICT (date_key) DO NOTHING
    """, records, page_size=1000)
    conn.commit()
    log.info(f"dim_date: {len(records)} gun yuklendi.")
    return len(records)


def load_dim_plan(conn):
    log.info("dim_plan yukleniyor...")
    cur = conn.cursor()

    cur.execute("""
        SELECT DISTINCT plan_id, plan_name, plan_type, duration_months, target_amount
        FROM staging_plans
    """)
    rows = cur.fetchall()

    records = [transform_dim_plan_record(row) for row in rows]

    cur.execute("DELETE FROM dim_plan")
    execute_values(cur, """
        INSERT INTO dim_plan
        (plan_id, plan_name, plan_type, duration_months, target_amount, monthly_installment)
        VALUES %s
    """, records)
    conn.commit()
    log.info(f"dim_plan: {len(records)} plan yuklendi.")
    return len(records)


def load_dim_member(conn):
    log.info("dim_member yukleniyor...")
    cur = conn.cursor()

    cur.execute("""
        SELECT DISTINCT ON (tc_hash)
            member_id, full_name, tc_hash, city, district,
            birth_year, income, signup_date, status
        FROM staging_members
        WHERE tc_hash IS NOT NULL AND tc_hash != ''
        ORDER BY tc_hash, signup_date DESC
    """)
    rows = cur.fetchall()

    records = [transform_dim_member_record(row) for row in rows]

    cur.execute("DELETE FROM dim_member")
    execute_values(cur, """
        INSERT INTO dim_member
        (member_id, full_name, tc_hash, city, district,
         age_group, income_bracket, signup_date,
         member_status, churn_date,
         valid_from, valid_to, is_current)
        VALUES %s
    """, records, page_size=1000)
    conn.commit()
    log.info(f"dim_member: {len(records)} uye yuklendi.")
    return len(records)


def load_fact_payments(conn):
    log.info("fact_payments yukleniyor...")
    cur = conn.cursor()

    cur.execute("""
        SELECT
            sp.payment_id,
            dm.member_key,
            dp.plan_key,
            TO_CHAR(COALESCE(sp.paid_date, sp.due_date), 'YYYYMMDD')::INT AS date_key,
            sp.installment_no,
            sp.due_amount,
            sp.paid_amount,
            sp.due_date,
            sp.paid_date
        FROM staging_payments sp
        JOIN dim_member dm ON dm.member_id = sp.member_id AND dm.is_current = TRUE
        JOIN dim_plan   dp ON dp.plan_id   = sp.plan_id
        WHERE sp.payment_id IS NOT NULL
    """)
    rows = cur.fetchall()

    records = [transform_fact_payment_record(row) for row in rows]

    cur.execute("DELETE FROM fact_payments")
    execute_values(cur, """
        INSERT INTO fact_payments
        (payment_id, member_key, plan_key, date_key,
         installment_no, due_amount, paid_amount,
         days_late, payment_status)
        VALUES %s
        ON CONFLICT (payment_id) DO NOTHING
    """, records, page_size=1000)
    conn.commit()
    log.info(f"fact_payments: {len(records)} kayit yuklendi.")
    return len(records)


def load_fact_lottery(conn):
    log.info("fact_lottery yukleniyor...")
    cur = conn.cursor()

    cur.execute("""
        SELECT
            sl.lottery_id,
            dm.member_key,
            dp.plan_key,
            TO_CHAR(sl.lottery_date, 'YYYYMMDD')::INT AS date_key,
            sl.lottery_round,
            sl.is_winner,
            sl.member_id,
            sl.lottery_date
        FROM staging_lottery sl
        JOIN dim_member dm ON dm.member_id = sl.member_id AND dm.is_current = TRUE
        JOIN dim_plan   dp ON dp.plan_id   = sl.plan_id
        WHERE sl.lottery_id IS NOT NULL
    """)
    rows = cur.fetchall()

    # Her üye için kura tarihine kadar ödenen taksit sayısını hesapla
    cur.execute("""
        SELECT
            member_id,
            due_date,
            payment_status
        FROM staging_payments
        WHERE payment_status IN ('odendi', 'kismi', 'gecikmeli')
    """)
    payments = cur.fetchall()

    # member_id → [(due_date, status)] lookup
    from collections import defaultdict
    member_payments = defaultdict(list)
    for member_id, due_date, status in payments:
        member_payments[member_id].append(due_date)

    cur.execute("DELETE FROM fact_lottery")

    records = []
    for row in rows:
        lottery_id, member_key, plan_key, date_key, \
        lottery_round, is_winner, member_id, lottery_date = row

        # Kura tarihine kadar kaç taksit ödendi
        paid_before_lottery = sum(
            1 for d in member_payments[member_id]
            if d <= lottery_date
        )

        # Kura tarihine kadar kaç ay geçmiş
        from dateutil.relativedelta import relativedelta
        months_elapsed = (
            (lottery_date.year - 2022) * 12 + lottery_date.month
        ) - (
            (2022 - 2022) * 12 + 1
        )
        months_elapsed = max(1, months_elapsed)

        ratio = round(paid_before_lottery / months_elapsed, 4)
        ratio = min(ratio, 1.0)  # 1.0'ı geçemez

        records.append((
            lottery_id, member_key, plan_key, date_key,
            lottery_round, is_winner, ratio
        ))

    execute_values(cur, """
        INSERT INTO fact_lottery
        (lottery_id, member_key, plan_key, date_key,
         lottery_round, is_winner, cumulative_paid_ratio)
        VALUES %s
        ON CONFLICT (lottery_id) DO NOTHING
    """, records, page_size=1000)

    conn.commit()
    log.info(f"fact_lottery: {len(records)} kayit yuklendi.")
    return len(records)


# ANA PIPELINE

def run_pipeline():
    log.info("=" * 50)
    log.info("ETL Pipeline basliyor...")
    t0 = time.time()

    conn = get_conn()

    cur = conn.cursor()
    cur.execute("""
    TRUNCATE fact_lottery, fact_payments, 
             dim_member, dim_plan, dim_date 
    RESTART IDENTITY CASCADE
    """)
    conn.commit()
    log.info("Tüm tablolar temizlendi.")

    steps = [
        ("dim_date",      load_dim_date),
        ("dim_plan",      load_dim_plan),
        ("dim_member",    load_dim_member),
        ("fact_payments", load_fact_payments),
        ("fact_lottery",  load_fact_lottery),
    ]

    for stage, fn in steps:
        t1 = time.time()
        try:
            rows = fn(conn)
            dur  = round(time.time() - t1, 2)
            log_pipeline_run(conn, stage, "success", rows, dur)
            log.info(f"[OK] {stage}: {rows} satir, {dur} sn")
        except Exception as e:
            conn.rollback()
            log.error(f"[HATA] {stage}: {e}")
            log_pipeline_run(conn, stage, "failed", error=str(e))

    conn.close()
    total = round(time.time() - t0, 2)
    log.info(f"Pipeline tamamlandi. Toplam sure: {total} sn")
    log.info("=" * 50)


if __name__ == "__main__":
    run_pipeline()