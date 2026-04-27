"""
Tasarruf Finansman - ETL Pipeline
Hafta 2: Staging -> Star Schema
Transform fonksiyonları: src/transformers.py
"""

import psycopg2
from psycopg2.extras import execute_values
from datetime import date, timedelta
import time
import sys
import os
from dateutil.relativedelta import relativedelta
from transformers import (
    transform_dim_date_record,
    transform_dim_plan_record,
    transform_dim_member_record,
    transform_fact_payment_record,
    transform_fact_lottery_record,
)

sys.path.append(os.path.dirname(__file__))
from config_loader import load_config
from utils.logger import get_logger

log = get_logger("etl_pipeline")

config = load_config()
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
        FROM staging.plans
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
        FROM staging.members
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

def load_dim_member_scd2(conn):
    log.info("dim_member SCD' yukleniyor...")
    cur = conn.cursor()
    today = date.today()

    inserted = 0
    updated = 0
    skipped = 0
    #stagingden temiz üyelerı çeker
    cur.execute("""
        SELECT DISTINCT ON (tc_hash)
            member_id, full_name, tc_hash, city, district,
            birth_year, income, signup_date, status
        FROM staging.members
        WHERE tc_hash IS NOT NULL AND tc_hash != ''
        ORDER BY tc_hash, signup_date DESC
    """)
    staging_rows = cur.fetchall()
    log.info(f"Staging'den {len(staging_rows)} kayit alindi.")
    # 2. dim_member'daki aktif kayıtları tek sorguda çek  ← BURAYA
    cur.execute("""
        SELECT member_id, member_status
        FROM dim_member
        WHERE is_current = TRUE
    """)
    existing_members = {row[0]: row[1] for row in cur.fetchall()}
    log.info(f"dim_member'dan {len(existing_members)} aktif kayit alindi.")
    for row in staging_rows:
        member_id = row[0]
        existing = existing_members.get(member_id)
        #dim_member'da bu üye var mı?
        if existing is None:
            # İHTİMAL 1: Yeni üye → direkt ekle
            record = transform_dim_member_record(row)
            cur.execute("""
                INSERT INTO dim_member
                (member_id, full_name, tc_hash, city, district,
                 age_group, income_bracket, signup_date,
                 member_status, churn_date,
                 valid_from, valid_to, is_current)
                VALUES %s
            """, (record,))
            inserted += 1

        elif existing != row[8]:   # existing[1] değil, direkt existing
            # İHTİMAL 2: Statü değişmiş
            # ADIM 1 → Eski kaydı kapat
            cur.execute("""
                UPDATE dim_member
                SET valid_to = %s, is_current = FALSE
                WHERE member_id = %s AND is_current = TRUE
            """, (today, member_id))
            
            # ADIM 2 → Yeni kayıt ekle
            record = transform_dim_member_record(row)
            cur.execute("""
                INSERT INTO dim_member
                (member_id, full_name, tc_hash, city, district,
                 age_group, income_bracket, signup_date,
                 member_status, churn_date,
                 valid_from, valid_to, is_current)
                VALUES %s
            """, (record,))
            updated += 1

        else:
            # İHTİMAL 3: Değişiklik yok → dokunma
            skipped += 1

    conn.commit()
    cur.close()
    log.info(f"dim_member SCD2 tamamlandi: {inserted} eklendi, {updated} guncellendi, {skipped} atlandi.")
    return inserted + updated + skipped

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
        FROM staging.payments sp
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
            sl.lottery_date,
            dm.signup_date
        FROM staging.lottery sl
        JOIN dim_member dm ON dm.member_id = sl.member_id AND dm.is_current = TRUE
        JOIN dim_plan   dp ON dp.plan_id   = sl.plan_id
        WHERE sl.lottery_id IS NOT NULL
    """)
    rows = cur.fetchall()

    # Her üye için ödenmiş taksitleri al
    cur.execute("""
        SELECT
            member_id,
            due_date,
            payment_status
        FROM staging.payments
        WHERE payment_status IN ('odendi', 'kismi', 'gecikmeli')
    """)
    payments = cur.fetchall()

    from collections import defaultdict
    member_payments = defaultdict(list)
    for member_id, due_date, status in payments:
        member_payments[member_id].append(due_date)

    cur.execute("DELETE FROM fact_lottery")

    from dateutil.relativedelta import relativedelta

    records = []
    for row in rows:
        (
            lottery_id, member_key, plan_key, date_key,
            lottery_round, is_winner, member_id,
            lottery_date, signup_date
        ) = row

        # Kura tarihine kadar ödenen taksit sayısı
        paid_before_lottery = sum(
            1 for d in member_payments[member_id]
            if d <= lottery_date
        )

        delta = relativedelta(lottery_date, signup_date)
        months_elapsed = max(
            1,
            delta.years * 12 + delta.months + (1 if delta.days > 0 else 0)
        )

        ratio = round(paid_before_lottery / months_elapsed, 4)
        ratio = min(ratio, 1.0)

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
             dim_plan, dim_date 
    RESTART IDENTITY CASCADE
    """)
    conn.commit()
    log.info("Tüm tablolar temizlendi.")

    steps = [
        ("dim_date",      load_dim_date),
        ("dim_plan",      load_dim_plan),
        ("dim_member",    load_dim_member_scd2),
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