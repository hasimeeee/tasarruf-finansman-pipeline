"""
Tasarruf Finansman - ETL Pipeline
Staging -> Star Schema dönüşümü

DÜZELTMELER (Son versiyon):
  1. dim_branch: staging.branches'ta branch_sk INTEGER ama ETL sütun listesinde
     yoktu → branch_sk eklendi, region ve open_date kolonları DDL ile eşleştirildi.
  2. dim_member: branch_sk FK ihlali → staging.branches'taki branch_sk değerleri
     dim_branch'teki branch_sk (SERIAL) ile eşleşmiyordu.
     Çözüm: dim_member yüklemesinde branch_sk NULL bırakıldı (staging.members.branch_sk
     bir branch_id gibi kullanılıyor, gerçek FK değil).
  3. dim_date transform: DDL'de day_name ve month_name kaldırıldı,
     ama transform_dim_date_record hâlâ bunları INSERT ediyordu → kaldırıldı.
  4. Test 5 pipeline log: status 'success'/'failed' (küçük harf) yazılıyor,
     test de küçük harf arıyor → tutarlı.
"""

import os
import sys
import time
import uuid
from collections import defaultdict
from datetime import date, timedelta

import psycopg2
from dateutil.relativedelta import relativedelta
from psycopg2.extras import execute_values

sys.path.append(os.path.dirname(__file__))
from config_loader import load_config
from transformers import (
    transform_dim_date_record,
    transform_dim_plan_record,
    transform_dim_member_record,
    transform_fact_payment_record,
)

try:
    from logger import get_logger
    log = get_logger("etl_pipeline")
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
    log = logging.getLogger("etl_pipeline")

config = load_config()
_db = config["database"].copy()
if "name" in _db:
    _db["dbname"] = _db.pop("name")
DB = _db


def get_conn():
    if "url" in DB:
        return psycopg2.connect(DB["url"])

    return psycopg2.connect(
        host=DB["host"],
        port=DB["port"],
        user=DB["user"],
        password=DB["password"],
        dbname=DB["dbname"]
    )


def log_pipeline_run(conn, pipeline_run_id, stage, status, rows=0, duration=0.0, error=None):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO pipeline_runs
        (pipeline_run_id, pipeline_name, stage, status, rows_inserted, duration_sec, error_msg)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (pipeline_run_id, "etl_pipeline", stage, status, rows, duration, error))
    conn.commit()


# ==========================================
# LOAD FONKSİYONLARI
# ==========================================

def load_dim_date(conn):
    """
    dim_date'i staging'deki gerçek min/max tarihe göre üretir.

    DÜZELTİLDİ: DDL'de day_name ve month_name sütunları kaldırıldı.
    INSERT sütun listesi DDL ile eşleştirildi.
    """
    log.info("dim_date yukleniyor...")
    cur = conn.cursor()

    cur.execute("""
        SELECT
            LEAST(
                MIN(due_date), MIN(paid_date),
                (SELECT MIN(lottery_date) FROM staging.lottery),
                (SELECT MIN(signup_date) FROM staging.members)
            ),
            GREATEST(
                MAX(due_date), MAX(paid_date),
                (SELECT MAX(lottery_date) FROM staging.lottery),
                (SELECT MAX(signup_date) FROM staging.members)
            )
        FROM staging.payments
    """)
    min_date_raw, max_date_raw = cur.fetchone()

    start = (min_date_raw or date(2022, 1, 1)).replace(day=1)
    end   = (max_date_raw or date.today()).replace(day=28) + timedelta(days=4)
    end   = end.replace(day=1) - timedelta(days=1)

    log.info(f"dim_date araligi: {start} → {end}")

    records = []
    current = start
    while current <= end:
        records.append(transform_dim_date_record(current))
        current += timedelta(days=1)

    # DÜZELTİLDİ: DDL'deki sütunlarla tam eşleşme (day_name, month_name YOK)
    execute_values(cur, """
        INSERT INTO dim_date
        (date_key, full_date, day, month, quarter, year,
         day_of_week, is_weekend, is_holiday, is_ramadan)
        VALUES %s
        ON CONFLICT (date_key) DO UPDATE SET
            is_holiday = EXCLUDED.is_holiday,
            is_ramadan = EXCLUDED.is_ramadan
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

    execute_values(cur, """
        INSERT INTO dim_plan
        (plan_id, plan_name, plan_type, duration_months, target_amount, monthly_installment)
        VALUES %s
        ON CONFLICT (plan_id) DO UPDATE SET
            plan_name           = EXCLUDED.plan_name,
            plan_type           = EXCLUDED.plan_type,
            duration_months     = EXCLUDED.duration_months,
            target_amount       = EXCLUDED.target_amount,
            monthly_installment = EXCLUDED.monthly_installment
    """, records)
    conn.commit()
    log.info(f"dim_plan: {len(records)} plan yuklendi.")
    return len(records)


def load_dim_branch(conn):
    """
    SCD Type 2 şube yüklemesi.

    DÜZELTİLDİ:
      - INSERT sütun listesi DDL ile eşleştirildi:
        branch_sk (SERIAL, otomatik), branch_id, branch_name, city,
        region, open_date, is_current, valid_from, valid_to
      - staging.branches'ta branch_sk sütunu var ama bu bir surrogate key değil,
        dışarıdan gelen ID — dim_branch'e yazılmıyor (SERIAL otomatik atanır).
      - duplicate key sorunu: idx_dim_branch_current unique index varsa
        WHERE NOT EXISTS koşulu zaten engeller, INSERT güvenli.
    """
    log.info("dim_branch yukleniyor...")
    cur = conn.cursor()

    # 1️⃣ Değişen kayıtları KAPAT (SCD2)
    cur.execute("""
        UPDATE dim_branch d
        SET valid_to   = CURRENT_DATE,
            is_current = FALSE
        FROM staging.branches s
        WHERE d.branch_id  = s.branch_id
          AND d.is_current = TRUE
          AND (
                d.branch_name != s.branch_name OR
                d.city        != s.city
          )
    """)
    updated = cur.rowcount

    # 2️⃣ Aktif versiyonu olmayan staging kayıtlarını ekle
    cur.execute("""
        INSERT INTO dim_branch
            (branch_id, branch_name, city, region, open_date, is_current, valid_from)
        SELECT
            s.branch_id,
            s.branch_name,
            s.city,
            s.region,
            s.open_date,
            TRUE,
            CURRENT_DATE
        FROM staging.branches s
        WHERE NOT EXISTS (
            SELECT 1 FROM dim_branch d
            WHERE d.branch_id  = s.branch_id
              AND d.is_current = TRUE
        )
    """)
    inserted = cur.rowcount

    conn.commit()
    log.info(f"dim_branch: {inserted} eklendi, {updated} guncellendi")
    return inserted + updated


def load_dim_member_scd2(conn):
    """
    SCD Type 2 üye yüklemesi.

    DÜZELTİLDİ (v2):
      - branch_sk FK: staging.members.branch_id → dim_branch.branch_sk JOIN ile
        gerçek surrogate key eşlemesi yapılıyor. dim_branch yüklendikten SONRA
        çalıştırılmalı (pipeline sırası zaten branch → member).
      - Eşleşemeyen üyelerde branch_sk NULL kalır (LEFT JOIN).
    """
    log.info("dim_member SCD2 yukleniyor...")
    cur = conn.cursor()
    today = date.today()

    inserted = 0
    updated  = 0
    skipped  = 0

    # staging.members.branch_sk (üretici tarafından atanmış INTEGER) ile
    # staging.branches.branch_sk eşleşir → staging.branches.branch_id alınır
    # → dim_branch'te is_current=TRUE olan kaydın branch_sk (SERIAL) bulunur.
    cur.execute("""
    SELECT *
    FROM (
        SELECT
            sm.member_id,
            sm.full_name,
            sm.tc_hash,
            sm.city,
            sm.district,
            sm.birth_date,
            sm.income,
            sm.signup_date,
            sm.member_status,

            db.branch_key AS branch_sk,

            ROW_NUMBER() OVER (
                PARTITION BY sm.tc_hash
                ORDER BY sm.signup_date DESC
            ) AS rn

        FROM staging.members sm

        LEFT JOIN staging.branches sb
            ON sb.branch_sk = sm.branch_sk

        LEFT JOIN dim_branch db
            ON db.branch_id = sb.branch_id
           AND db.is_current = TRUE

        WHERE sm.tc_hash IS NOT NULL
          AND sm.tc_hash != ''
    ) t
    WHERE rn = 1
    """)

    staging_rows = cur.fetchall()
    log.info(f"Staging'den {len(staging_rows)} temiz kayıt alindi.")

    cur.execute("""
        SELECT tc_hash, member_status
        FROM dim_member
        WHERE is_current = TRUE
    """)

    existing_members = {row[0]: row[1] for row in cur.fetchall()}
    log.info(f"dim_member'dan {len(existing_members)} aktif kayit alindi.")

    for row in staging_rows:
        (
            member_id,
            full_name,
            tc_hash,
            city,
            district,
            birth_date,
            income,
            signup_date,
            new_status,
            branch_sk,
            rn,
        ) = row

        existing_status = existing_members.get(tc_hash)

        if existing_status is None:
            record = transform_dim_member_record(row, valid_from=signup_date)

            # branch_sk: dim_branch'ten JOIN ile gelen gerçek surrogate key
            # dim_branch'te eşleşme yoksa NULL (LEFT JOIN garantisi)
            cur.execute("""
                INSERT INTO dim_member
                (member_id, full_name, tc_hash, city, district,
                 age_group, income_bracket, signup_date,
                 member_status, member_segment, churn_date,
                 valid_from, valid_to, is_current, branch_sk)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, record[:14] + (branch_sk,))

            inserted += 1

        elif existing_status != new_status:

            cur.execute("""
                UPDATE dim_member
                SET valid_to = %s,
                    is_current = FALSE
                WHERE tc_hash = %s
                  AND is_current = TRUE
            """, (today, tc_hash))

            record = transform_dim_member_record(row, valid_from=today)

            cur.execute("""
                INSERT INTO dim_member
                (member_id, full_name, tc_hash, city, district,
                 age_group, income_bracket, signup_date,
                 member_status, member_segment, churn_date,
                 valid_from, valid_to, is_current, branch_sk)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, record[:14] + (branch_sk,))

            updated += 1

        else:
            skipped += 1

    conn.commit()
    cur.close()

    log.info(
        f"dim_member SCD2 tamamlandi: "
        f"{inserted} eklendi, {updated} guncellendi, {skipped} atlandi."
    )

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
          AND sp.paid_amount >= 0
    """)

    rows = cur.fetchall()

    cur.execute("""
        SELECT COUNT(*)
        FROM staging.payments
        WHERE paid_amount < 0
    """)
    neg_count = cur.fetchone()[0]
    if neg_count > 0:
        log.warning(f"Negatif ödeme sayisi (yuklenmedi): {neg_count}")

    records = [transform_fact_payment_record(row) for row in rows]

    execute_values(cur, """
        INSERT INTO fact_payments
        (payment_id, member_key, plan_key, date_key,
         installment_no, due_amount, paid_amount,
         days_late, payment_status)
        VALUES %s
        ON CONFLICT (payment_id) DO UPDATE SET
            due_amount     = EXCLUDED.due_amount,
            paid_amount    = EXCLUDED.paid_amount,
            days_late      = EXCLUDED.days_late,
            payment_status = EXCLUDED.payment_status
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

    cur.execute("""
        SELECT member_id, due_date
        FROM staging.payments
        WHERE payment_status IN ('odendi', 'kismi', 'gecikmeli')
    """)
    member_payments = defaultdict(list)
    for member_id, due_date in cur.fetchall():
        member_payments[member_id].append(due_date)

    cur.execute("DELETE FROM fact_lottery")

    records = []
    for row in rows:
        (
            lottery_id, member_key, plan_key, date_key,
            lottery_round, is_winner, member_id,
            lottery_date, signup_date
        ) = row

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
        ON CONFLICT (lottery_id) DO UPDATE SET
            cumulative_paid_ratio = EXCLUDED.cumulative_paid_ratio
    """, records, page_size=1000)

    conn.commit()
    log.info(f"fact_lottery: {len(records)} kayit yuklendi.")
    return len(records)
def fix_terk_members(conn):
    """terk statüsündeki üyelerin is_current'ını false yapar"""
    cur = conn.cursor()
    cur.execute("""
        UPDATE dim_member
        SET is_current = false,
            valid_to = CURRENT_DATE
        WHERE member_status = 'terk'
          AND is_current = true
    """)
    updated = cur.rowcount
    conn.commit()
    cur.close()
    log.info(f"fix_terk_members: {updated} kayit guncellendi.")
    return updated

# ==========================================
# DATA QUALITY RAPORU — Satır Kaybı Özeti
# ==========================================

def log_row_loss_report(conn):
    cur = conn.cursor()
    log.info("=" * 50)
    log.info("SATIR KAYBI RAPORU")

    cur.execute("SELECT COUNT(*) FROM staging.members")
    staging_members_total = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM staging.members WHERE tc_hash IS NULL OR tc_hash = ''")
    null_tc = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*) FROM (
            SELECT tc_hash FROM staging.members
            WHERE tc_hash IS NOT NULL AND tc_hash != ''
            GROUP BY tc_hash HAVING COUNT(*) > 1
        ) t
    """)
    duplicate_members = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM dim_member WHERE is_current = TRUE")
    dim_member_count = cur.fetchone()[0]

    log.info(f"staging.members  → toplam     : {staging_members_total}")
    log.info(f"  - NULL tc_hash               : {null_tc}")
    log.info(f"  - Duplicate                  : {duplicate_members}")
    log.info(f"  dim_member                   : {dim_member_count}")

    cur.execute("SELECT COUNT(*) FROM staging.payments")
    staging_pay_total = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*) FROM staging.payments sp
        WHERE NOT EXISTS (
            SELECT 1 FROM dim_member dm
            WHERE dm.member_id = sp.member_id AND dm.is_current = TRUE
        )
    """)
    fk_mismatch_pay = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM fact_payments")
    fact_pay_count = cur.fetchone()[0]

    log.info(f"staging.payments → toplam     : {staging_pay_total}")
    log.info(f"  - FK mismatch               : {fk_mismatch_pay}")
    log.info(f"  fact_payments               : {fact_pay_count}")

    cur.execute("""
        SELECT
            COUNT(*) FILTER (WHERE paid_date < due_date) AS early_payments,
            COUNT(*) FILTER (WHERE paid_date > due_date) AS late_payments,
            COUNT(*) FILTER (WHERE paid_date IS NULL)    AS unpaid
        FROM staging.payments
    """)

    early, late, unpaid = cur.fetchone()

    log.info(f"Early payments: {early}")
    log.info(f"Late payments: {late}")
    log.info(f"Unpaid: {unpaid}")

    log.info("=" * 50)
    cur.close()


# ==========================================
# ANA PIPELINE
# ==========================================

def run_pipeline():
    log.info("=" * 50)
    log.info("ETL Pipeline basliyor...")
    t0 = time.time()

    pipeline_run_id = str(uuid.uuid4())
    log.info(f"Pipeline run ID: {pipeline_run_id}")

    conn = get_conn()

    steps = [
    ("dim_date",      load_dim_date),
    ("dim_plan",      load_dim_plan),
    ("dim_branch",    load_dim_branch),
    ("dim_member",    load_dim_member_scd2),
    ("fix_terk",      fix_terk_members),   # ← BURAYA EKLE
    ("fact_payments", load_fact_payments),
    ("fact_lottery",  load_fact_lottery),
]

    for stage, fn in steps:
        t1 = time.time()
        try:
            rows = fn(conn)
            dur  = round(time.time() - t1, 2)
            log_pipeline_run(conn, pipeline_run_id, stage, "success", rows, dur)
            log.info(f"[OK] {stage}: {rows} satir, {dur} sn")
        except Exception as e:
            conn.rollback()
            log.error(f"[HATA] {stage}: {e}")
            log_pipeline_run(conn, pipeline_run_id, stage, "failed", error=str(e))

    try:
        log_row_loss_report(conn)
    except Exception as e:
        log.warning(f"Satir kaybi raporu uretilirken hata: {e}")

    conn.close()
    total = round(time.time() - t0, 2)
    log.info(f"Pipeline tamamlandi. Toplam sure: {total} sn")
    log.info("=" * 50)


if __name__ == "__main__":
    run_pipeline()
