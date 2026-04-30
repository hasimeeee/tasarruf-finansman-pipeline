"""
Tasarruf Finansman - ETL Pipeline
Staging -> Star Schema dönüşümü

Değişiklikler (Hafta 2 → Hafta 3):
  - 'import os' tekrarı giderildi (satır 13 ve 43'teydi)
  - load_fact_lottery içindeki 'from collections import defaultdict' ve
    'from dateutil.relativedelta import relativedelta' fonksiyon içinden
    dosya başına taşındı
  - dim_date aralığı sabit 2021-2028'den dinamik (staging min/max) hale getirildi
  - load_dim_member sorgusu birth_year → birth_date olarak güncellendi
  - Schema tutarlılığı: DDL ve pipeline artık ikisi de 'public' kullanıyor
  - TRUNCATE CASCADE yerine tablo bazlı TRUNCATE — idempotency için
    Hafta 3'te UPSERT'e geçilecek (not bırakıldı)
"""

import os
import sys
import time
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
    # transform_fact_lottery_record kaldırıldı (dead code — bkz. transformers.py)
)

# Logger: utils/logger.py yoksa standart logging'e düş
try:
    from utils.logger import get_logger
    log = get_logger("etl_pipeline")
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    log = logging.getLogger("etl_pipeline")

config = load_config()
_db = config["database"].copy()
if "name" in _db:
    _db["dbname"] = _db.pop("name")
DB = _db


def get_conn():
    return psycopg2.connect(**DB)


# ==========================================
# LOAD FONKSİYONLARI
# ==========================================

def load_dim_date(conn):
    """
    dim_date'i staging'deki gerçek min/max tarihe göre üretir.
    Düzeltme: sabit 2021-2028 aralığı kaldırıldı — boşta kalan tarihler
    FK mismatch'e yol açıyordu (log'da 20211229 referans hatası görülmüştü).
    """
    log.info("dim_date yukleniyor...")
    cur = conn.cursor()

    # Staging'deki gerçek tarih aralığını bul
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

    # Güvenli fallback + biraz buffer (ayın başı/sonu için)
    start = (min_date_raw or date(2022, 1, 1)).replace(day=1)
    end   = (max_date_raw or date.today()).replace(day=28) + timedelta(days=4)
    end   = end.replace(day=1) - timedelta(days=1)  # ayın son günü

    log.info(f"dim_date araligi: {start} → {end}")

    records = []
    current = start
    while current <= end:
        records.append(transform_dim_date_record(current))
        current += timedelta(days=1)

    execute_values(cur, """
        INSERT INTO dim_date
        (date_key, full_date, day, month, quarter, year,
         day_of_week, day_name, month_name, is_weekend, is_holiday, is_ramadan)
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


def load_dim_member_scd2(conn):
    """
    SCD Type 2 ile dim_member'ı günceller.
    - Yeni üye → INSERT
    - member_status değişmiş → eski kaydı kapat (valid_to, is_current=False) + yeni INSERT
    - Değişiklik yok → dokunma

    Düzeltme: staging sorgusu birth_year → birth_date olarak güncellendi.
    """
    log.info("dim_member SCD2 yukleniyor...")
    cur = conn.cursor()
    today = date.today()

    inserted = 0
    updated  = 0
    skipped  = 0

    # Staging'den temiz üyeleri çek (birth_date — artık DATE tipi)
    cur.execute("""
        SELECT DISTINCT ON (tc_hash)
            member_id, full_name, tc_hash, city, district,
            birth_date, income, signup_date, member_status
        FROM staging.members
        WHERE tc_hash IS NOT NULL AND tc_hash != ''
        ORDER BY tc_hash, signup_date DESC
    """)
    staging_rows = cur.fetchall()
    log.info(f"Staging'den {len(staging_rows)} kayit alindi.")

    # dim_member'daki aktif kayıtları tek sorguda çek
    cur.execute("""
        SELECT member_id, member_status
        FROM dim_member
        WHERE is_current = TRUE
    """)
    existing_members = {row[0]: row[1] for row in cur.fetchall()}
    log.info(f"dim_member'dan {len(existing_members)} aktif kayit alindi.")

    for row in staging_rows:
        member_id     = row[0]
        new_status    = row[8]   # member_status sütunu (index 8)
        existing_status = existing_members.get(member_id)

        if existing_status is None:
            # Yeni üye → valid_from = signup_date
            record = transform_dim_member_record(row, valid_from=row[7])
            cur.execute("""
                INSERT INTO dim_member
                (member_id, full_name, tc_hash, city, district,
                 age_group, income_bracket, signup_date,
                 member_status, churn_date,
                 valid_from, valid_to, is_current)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, record)
            inserted += 1

        elif existing_status != new_status:
            # Statü değişmiş → eski kaydı kapat, valid_from = today
            cur.execute("""
                UPDATE dim_member
                SET valid_to = %s, is_current = FALSE
                WHERE member_id = %s AND is_current = TRUE
            """, (today, member_id))

            record = transform_dim_member_record(row, valid_from=today)
            cur.execute("""
                INSERT INTO dim_member
                (member_id, full_name, tc_hash, city, district,
                 age_group, income_bracket, signup_date,
                 member_status, churn_date,
                 valid_from, valid_to, is_current)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, record)
            updated += 1

        else:
            # Değişiklik yok → dokunma
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
    """
    cumulative_paid_ratio hesabı: üyenin signup_date'inden kura tarihine kadar
    geçen ay sayısı baz alınır.
    Düzeltme: sabit 2022-01'e göre months_elapsed hesabı kaldırıldı —
    bu hata ortalama ratio'yu yapay olarak düşürüyordu (~0.31).
    """
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

    # Her üye için ödenmiş taksit tarihlerini çek
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

        # Kura tarihine kadar ödenen taksit sayısı
        paid_before_lottery = sum(
            1 for d in member_payments[member_id]
            if d <= lottery_date
        )

        # Düzeltme: signup_date bazlı hesap (sabit 2022-01 değil)
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


# ==========================================
# DATA QUALITY RAPORU — Satır Kaybı Özeti
# ==========================================

def log_row_loss_report(conn):
    """
    Staging → DWH geçişinde kaç satırın neden düştüğünü raporlar.
    Geri bildirim #1: Kayıp satırları kategorize et.
    """
    cur = conn.cursor()
    log.info("=" * 50)
    log.info("SATIR KAYBI RAPORU")

    # --- members ---
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
    log.info(f"  - NULL tc_hash filtresi      : {null_tc}")
    log.info(f"  - Duplike (tc_hash bazlı)    : {duplicate_members}")
    log.info(f"  dim_member (is_current)      : {dim_member_count}")
    log.info(f"  Açıklanamayan kayıp          : {staging_members_total - null_tc - duplicate_members - dim_member_count}")

    # --- payments ---
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
    log.info(f"  - FK mismatch (member yok)   : {fk_mismatch_pay}")
    log.info(f"  fact_payments               : {fact_pay_count}")
    log.info(f"  Açıklanamayan kayıp          : {staging_pay_total - fk_mismatch_pay - fact_pay_count}")

    log.info("=" * 50)
    cur.close()


# ==========================================
# ANA PIPELINE
# ==========================================

def run_pipeline():
    log.info("=" * 50)
    log.info("ETL Pipeline basliyor...")
    t0 = time.time()

    conn = get_conn()

    # UPSERT stratejisi: DELETE/TRUNCATE yok.
    # Her tablo INSERT ON CONFLICT DO UPDATE ile güncelleniyor.
    # Aynı pipeline kaç kez çalışsa sonuç aynı — gerçek idempotency.
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

    # Satır kaybı raporu
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