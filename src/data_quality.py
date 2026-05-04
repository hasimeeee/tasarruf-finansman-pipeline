"""
Tasarruf Finansman — Data Quality Framework
Hafta 4: Rule-based quality checks, iş kuralları, alert sistemi

Kural kategorileri:
  1. Zorunlu alan / NULL kontrolleri
  2. Duplicate kontrolleri
  3. Domain / enum değer kontrolleri
  4. İş kuralları (business rules)       ← YENİ
  5. FK integrity (staging → dim)
  6. Satır sayısı assertion (row count)  ← YENİ
  7. SCD2 tutarlılık

Alert sistemi:
  - FAIL varsa logger.warning ile konsola yazar
  - config.yaml'da [smtp] bölümü varsa e-posta da gönderir

Hafta 4 değişiklikleri (mevcut koda göre):
  - check_business_rules() eklendi:
      BR-01 paid_amount ≤ due_amount
      BR-02 days_late ≥ 0
      BR-03 installment_no ≥ 1
      BR-04 cumulative_paid_ratio ≥ 0.50 (kura düzenliliği)
      BR-05 'odendi' ama paid_amount = 0
      BR-06 'odenmedi' ama paid_date girilmiş
  - check_row_counts() eklendi:
      RC-01/02 minimum staging satır sayısı
      RC-03/04 dim_member / fact_payments yükleme oranı
  - send_alert() eklendi: WARNING log + SMTP e-posta
  - start_run / finish_run: pipeline_name, finished_at ile güncellendi
  - Tutarsız ödeme tarihi kuralı FAIL → WARN'a alındı (erken ödeme geçerli)
  - run_check: tekrarlanan PASS ataması temizlendi
"""

import logging
import smtplib
import sys
import os
from datetime import date
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import psycopg2

sys.path.append(os.path.dirname(__file__))
from config_loader import load_config

# ==========================================
# LOGGER
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ==========================================
# ALERT EŞİKLERİ
# ==========================================
ALERT_THRESHOLDS = {
    "max_fail_count":         0,    # kaç FAIL'den itibaren alert (0 = her FAIL'de)
    "max_null_tc_ratio":      0.10, # %10 üzeri tc_hash NULL → WARN
    "min_member_load_ratio":  0.85, # dim_member yükleme oranı < %85 → FAIL
    "min_payment_load_ratio": 0.85, # fact_payments yükleme oranı < %85 → FAIL
    "min_staging_members":    1000, # staging'de en az bu kadar üye
    "min_staging_payments":   10000,# staging'de en az bu kadar ödeme
}


# ==========================================
# DB BAĞLANTISI
# ==========================================
def get_db_connection():
    config = load_config()
    db = config["database"].copy()
    if "name" in db:
        db["dbname"] = db.pop("name")
    return psycopg2.connect(**db)


# ==========================================
# PIPELINE RUN LOG
# ==========================================
def start_run(conn, pipeline_name: str) -> int:
    """pipeline_runs tablosuna RUNNING kaydı açar, run_id döndürür."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO pipeline_runs (pipeline_name, status, run_at)
            VALUES (%s, 'RUNNING', NOW())
            RETURNING run_id
        """, (pipeline_name,))
        run_id = cur.fetchone()[0]
    conn.commit()
    return run_id


def finish_run(conn, run_id: int, status: str):
    """Pipeline bitişini ve toplam süreyi kaydeder."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE pipeline_runs
            SET status       = %s,
                finished_at  = NOW(),
                duration_sec = EXTRACT(EPOCH FROM (NOW() - run_at))
            WHERE run_id = %s
        """, (status, run_id))
    conn.commit()


def log_check_result(conn, run_id, check_name, table_name,
                     status, metric_value, threshold, detail):
    """Her kural sonucunu quality_check_results tablosuna yazar."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO quality_check_results
                (run_id, check_name, table_name, status,
                 metric_value, threshold, detail)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (run_id, check_name, table_name, status,
              metric_value, threshold, detail))
    conn.commit()


# ==========================================
# ALERT SİSTEMİ
# ==========================================
def send_alert(failed_rules: list, warned_rules: list):
    """
    FAIL varsa:
      1. logger.warning ile konsola yazar
      2. config.yaml'da smtp bölümü varsa e-posta gönderir

    config.yaml örneği:
      smtp:
        host: smtp.gmail.com
        port: 587
        user: alerts@sirket.com
        password: xxxx
        to: data-team@sirket.com
    """
    fail_count = len(failed_rules)

    # --- WARNING log ---
    logger.warning("=" * 55)
    logger.warning(f"ALERT: {fail_count} kural başarısız!")
    for r in failed_rules:
        logger.warning(f"  ✗ FAIL [{r['table']}] {r['rule']} — {r['detail']}")
    for r in warned_rules:
        logger.warning(f"  ⚠ WARN [{r['table']}] {r['rule']} — {r['detail']}")
    logger.warning("=" * 55)

    # --- SMTP e-posta (opsiyonel) ---
    config = load_config()
    smtp_cfg = config.get("smtp")
    if not smtp_cfg:
        logger.info("SMTP yapılandırması bulunamadı — e-posta gönderilmedi.")
        return

    try:
        subject = f"[DATA QUALITY ALERT] {fail_count} FAIL — Tasarruf Finansman Pipeline"

        lines = [
            f"Tarih      : {date.today()}",
            f"FAIL sayısı: {fail_count}",
            "",
            "BAŞARISIZ KURALLAR:",
        ]
        for r in failed_rules:
            lines.append(f"  ✗ [{r['table']}] {r['rule']} — {r['detail']}")
        if warned_rules:
            lines.append("")
            lines.append("UYARILAR:")
            for r in warned_rules:
                lines.append(f"  ⚠ [{r['table']}] {r['rule']} — {r['detail']}")

        msg = MIMEMultipart()
        msg["From"]    = smtp_cfg["user"]
        msg["To"]      = smtp_cfg["to"]
        msg["Subject"] = subject
        msg.attach(MIMEText("\n".join(lines), "plain", "utf-8"))

        with smtplib.SMTP(smtp_cfg["host"], smtp_cfg.get("port", 587)) as server:
            server.ehlo()
            server.starttls()
            server.login(smtp_cfg["user"], smtp_cfg["password"])
            server.sendmail(smtp_cfg["user"], smtp_cfg["to"], msg.as_string())

        logger.info(f"Alert e-postası gönderildi → {smtp_cfg['to']}")

    except Exception as e:
        logger.error(f"E-posta gönderilemedi: {e}")


# ==========================================
# TEMEL KURAL MOTORU
# ==========================================
def run_check(cur, conn, run_id, rule: str, table: str, sql: str,
              threshold: int = 0, warn_only: bool = False) -> dict:
    """
    Tek bir kalite kuralını çalıştırır.
    Sonucu hem log'a hem quality_check_results'a yazar.
    """
    cur.execute(sql)
    count = cur.fetchone()[0]

    if count == 0:
        status = "PASS"
    elif warn_only:
        status = "WARN"
    else:
        status = "FAIL"

    detail = f"{count} sorunlu satır" if count > 0 else "Temiz"
    logger.info(f"[{status}] {rule} — {table}: {detail}")
    log_check_result(conn, run_id, rule, table, status, count, threshold, detail)
    return {"rule": rule, "table": table, "status": status,
            "count": count, "detail": detail}


# ==========================================
# 1. STAGING KALİTE KONTROLLERİ
# ==========================================
def check_staging(cur, conn, run_id) -> list:
    results = []

    # --- staging.members ---
    results.append(run_check(cur, conn, run_id,
        "NULL zorunlu alan", "staging.members",
        """
        SELECT COUNT(*) FROM staging.members
        WHERE member_id     IS NULL
           OR city          IS NULL
           OR income        IS NULL
           OR signup_date   IS NULL
           OR member_status IS NULL
        """
    ))

    results.append(run_check(cur, conn, run_id,
        "NULL tc_hash oranı (bilgi amaçlı)", "staging.members",
        """
        SELECT COUNT(*) FROM staging.members
        WHERE tc_hash IS NULL OR tc_hash = ''
        """,
        warn_only=True
    ))

    results.append(run_check(cur, conn, run_id,
        "Duplicate member_id", "staging.members",
        """
        SELECT COUNT(*) FROM (
            SELECT member_id FROM staging.members
            GROUP BY member_id HAVING COUNT(*) > 1
        ) t
        """
    ))

    results.append(run_check(cur, conn, run_id,
        "Geçersiz member_status", "staging.members",
        """
        SELECT COUNT(*) FROM staging.members
        WHERE member_status NOT IN ('aktif','gecikmeli','pasif','terk')
          AND member_status IS NOT NULL
        """
    ))

    results.append(run_check(cur, conn, run_id,
        "Geçersiz gelir (income <= 0)", "staging.members",
        """
        SELECT COUNT(*) FROM staging.members
        WHERE income <= 0
        """
    ))

    results.append(run_check(cur, conn, run_id,
        "18 yaş altı üye", "staging.members",
        """
        SELECT COUNT(*) FROM staging.members
        WHERE birth_date IS NOT NULL
          AND DATE_PART('year', AGE(birth_date)) < 18
        """
    ))

    results.append(run_check(cur, conn, run_id,
        "NULL email veya phone", "staging.members",
        """
        SELECT COUNT(*) FROM staging.members
        WHERE email IS NULL OR phone IS NULL
        """
    ))

    # --- staging.payments ---
    results.append(run_check(cur, conn, run_id,
        "NULL zorunlu alan", "staging.payments",
        """
        SELECT COUNT(*) FROM staging.payments
        WHERE payment_id IS NULL
           OR member_id  IS NULL
           OR due_date   IS NULL
           OR due_amount IS NULL
        """
    ))

    results.append(run_check(cur, conn, run_id,
        "Duplicate payment_id", "staging.payments",
        """
        SELECT COUNT(*) FROM (
            SELECT payment_id FROM staging.payments
            GROUP BY payment_id HAVING COUNT(*) > 1
        ) t
        """
    ))

    results.append(run_check(cur, conn, run_id,
        "Negatif ödeme tutarı", "staging.payments",
        """
        SELECT COUNT(*) FROM staging.payments
        WHERE paid_amount < 0
        """
    ))

    results.append(run_check(cur, conn, run_id,
        "Geçersiz payment_status", "staging.payments",
        """
        SELECT COUNT(*) FROM staging.payments
        WHERE payment_status NOT IN ('odendi','gecikmeli','kismi','odenmedi')
          AND payment_status IS NOT NULL
        """
    ))

    results.append(run_check(cur, conn, run_id,
        "Tutarsız ödeme tarihi (paid_date < due_date)", "staging.payments",
        """
        SELECT COUNT(*) FROM staging.payments
        WHERE paid_date IS NOT NULL
          AND paid_date < due_date
        """,
        warn_only=True  # erken ödeme geçerli bir iş kuralı olabilir → WARN
    ))

    results.append(run_check(cur, conn, run_id,
        "Orphan payments (member yok)", "staging.payments",
        """
        SELECT COUNT(*) FROM staging.payments p
        WHERE NOT EXISTS (
            SELECT 1 FROM staging.members m
            WHERE m.member_id = p.member_id
        )
        """
    ))

    # --- staging.plans ---
    results.append(run_check(cur, conn, run_id,
        "Geçersiz plan_type", "staging.plans",
        """
        SELECT COUNT(*) FROM staging.plans
        WHERE plan_type NOT IN ('konut','arsa','ticari','arac','isyeri')
        """
    ))

    results.append(run_check(cur, conn, run_id,
        "Geçersiz monthly_installment", "staging.plans",
        """
        SELECT COUNT(*) FROM staging.plans
        WHERE monthly_installment <= 0
           OR monthly_installment IS NULL
        """
    ))

    # --- staging.lottery ---
    results.append(run_check(cur, conn, run_id,
        "Orphan lottery (member yok)", "staging.lottery",
        """
        SELECT COUNT(*) FROM staging.lottery l
        WHERE NOT EXISTS (
            SELECT 1 FROM staging.members m
            WHERE m.member_id = l.member_id
        )
        """
    ))

    return results


# ==========================================
# 2. DWH FK + SCD2 KONTROLLERİ
# ==========================================
def check_dwh(cur, conn, run_id) -> list:
    results = []

    results.append(run_check(cur, conn, run_id,
        "fact_payments → dim_member FK", "fact_payments",
        """
        SELECT COUNT(*) FROM fact_payments fp
        WHERE NOT EXISTS (
            SELECT 1 FROM dim_member dm WHERE dm.member_key = fp.member_key
        )
        """
    ))

    results.append(run_check(cur, conn, run_id,
        "fact_payments → dim_plan FK", "fact_payments",
        """
        SELECT COUNT(*) FROM fact_payments fp
        WHERE NOT EXISTS (
            SELECT 1 FROM dim_plan dp WHERE dp.plan_key = fp.plan_key
        )
        """
    ))

    results.append(run_check(cur, conn, run_id,
        "fact_payments → dim_date FK", "fact_payments",
        """
        SELECT COUNT(*) FROM fact_payments fp
        WHERE NOT EXISTS (
            SELECT 1 FROM dim_date dd WHERE dd.date_key = fp.date_key
        )
        """
    ))

    results.append(run_check(cur, conn, run_id,
        "fact_lottery → dim_member FK", "fact_lottery",
        """
        SELECT COUNT(*) FROM fact_lottery fl
        WHERE NOT EXISTS (
            SELECT 1 FROM dim_member dm WHERE dm.member_key = fl.member_key
        )
        """
    ))

    results.append(run_check(cur, conn, run_id,
        "cumulative_paid_ratio aralık kontrolü (0-1)", "fact_lottery",
        """
        SELECT COUNT(*) FROM fact_lottery
        WHERE cumulative_paid_ratio < 0 OR cumulative_paid_ratio > 1
        """
    ))

    results.append(run_check(cur, conn, run_id,
        "dim_member SCD2 tutarlılık (is_current)", "dim_member",
        """
        SELECT COUNT(*) FROM (
            SELECT member_id FROM dim_member
            WHERE is_current = TRUE
            GROUP BY member_id HAVING COUNT(*) > 1
        ) t
        """
    ))

    return results


# ==========================================
# 3. İŞ KURALLARI — YENİ (Hafta 4)
# ==========================================
def check_business_rules(cur, conn, run_id) -> list:
    """
    Ödeme ve kura mantığına özgü iş kuralları.

    BR-01  paid_amount ≤ due_amount        (fazla ödeme olamaz)
    BR-02  days_late ≥ 0                   (negatif gecikme gün sayısı olamaz)
    BR-03  installment_no ≥ 1              (taksit numarası pozitif olmalı)
    BR-04  cumulative_paid_ratio ≥ 0.50    (kura katılımı ödeme düzenliliği)
    BR-05  'odendi' → paid_amount > 0
    BR-06  'odenmedi' → paid_date IS NULL
    """
    results = []

    results.append(run_check(cur, conn, run_id,
        "BR-01: paid_amount > due_amount (fazla ödeme)", "fact_payments",
        """
        SELECT COUNT(*) FROM fact_payments
        WHERE paid_amount > due_amount
        """,
        warn_only=True  # kısmi veya yuvarlama farkı olabilir → WARN
    ))

    results.append(run_check(cur, conn, run_id,
        "BR-02: days_late < 0 (negatif gecikme)", "fact_payments",
        """
        SELECT COUNT(*) FROM fact_payments
        WHERE days_late < 0
        """
    ))

    results.append(run_check(cur, conn, run_id,
        "BR-03: installment_no < 1 (geçersiz taksit sırası)", "fact_payments",
        """
        SELECT COUNT(*) FROM fact_payments
        WHERE installment_no < 1 OR installment_no IS NULL
        """
    ))

    results.append(run_check(cur, conn, run_id,
        "BR-04: Kura katılımı — cumulative_paid_ratio < 0.50", "fact_lottery",
        """
        SELECT COUNT(*) FROM fact_lottery
        WHERE cumulative_paid_ratio < 0.50
        """,
        warn_only=True  # bilgi amaçlı: hangi üyeler düşük ratio ile kuraya girmiş
    ))

    results.append(run_check(cur, conn, run_id,
        "BR-05: payment_status='odendi' ama paid_amount = 0", "fact_payments",
        """
        SELECT COUNT(*) FROM fact_payments
        WHERE payment_status = 'odendi'
          AND (paid_amount IS NULL OR paid_amount = 0)
        """
    ))

    results.append(run_check(cur, conn, run_id,
        "BR-06: payment_status='odenmedi' ama paid_date girilmiş", "staging.payments",
        """
        SELECT COUNT(*) FROM staging.payments
        WHERE payment_status = 'odenmedi'
          AND paid_date IS NOT NULL
        """
    ))

    return results


# ==========================================
# 4. SATIR SAYISI ASSERTION — YENİ (Hafta 4)
# ==========================================
def check_row_counts(cur, conn, run_id) -> list:
    """
    Minimum satır eşiği ve staging → DWH yükleme oranı kontrolleri.
    Eşikler ALERT_THRESHOLDS sözlüğünden okunur.
    """
    results = []

    # Staging sayılarını bir kez çek
    cur.execute("SELECT COUNT(*) FROM staging.members")
    member_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM staging.payments")
    payment_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM dim_member WHERE is_current = TRUE")
    dim_member_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM fact_payments")
    fact_pay_count = cur.fetchone()[0]

    def _rc(rule, table, value, threshold, pass_cond, detail):
        status = "PASS" if pass_cond else "FAIL"
        logger.info(f"[{status}] {rule} — {table}: {detail}")
        log_check_result(conn, run_id, rule, table, status, value, threshold, detail)
        return {"rule": rule, "table": table, "status": status,
                "count": value, "detail": detail}

    min_m = ALERT_THRESHOLDS["min_staging_members"]
    results.append(_rc(
        "RC-01: staging.members minimum satır", "staging.members",
        member_count, min_m,
        member_count >= min_m,
        f"{member_count} satır (eşik: ≥{min_m})"
    ))

    min_p = ALERT_THRESHOLDS["min_staging_payments"]
    results.append(_rc(
        "RC-02: staging.payments minimum satır", "staging.payments",
        payment_count, min_p,
        payment_count >= min_p,
        f"{payment_count} satır (eşik: ≥{min_p})"
    ))

    member_ratio = dim_member_count / member_count if member_count > 0 else 0
    min_mr = ALERT_THRESHOLDS["min_member_load_ratio"]
    results.append(_rc(
        "RC-03: dim_member yükleme oranı", "dim_member",
        dim_member_count, int(min_mr * member_count),
        member_ratio >= min_mr,
        f"{member_ratio:.1%} ({dim_member_count}/{member_count}, eşik: ≥{min_mr:.0%})"
    ))

    pay_ratio = fact_pay_count / payment_count if payment_count > 0 else 0
    min_pr = ALERT_THRESHOLDS["min_payment_load_ratio"]
    results.append(_rc(
        "RC-04: fact_payments yükleme oranı", "fact_payments",
        fact_pay_count, int(min_pr * payment_count),
        pay_ratio >= min_pr,
        f"{pay_ratio:.1%} ({fact_pay_count}/{payment_count}, eşik: ≥{min_pr:.0%})"
    ))

    return results


# ==========================================
# 5. SATIR KAYBI RAPORU
# ==========================================
def row_loss_report(cur, conn, run_id) -> dict:
    """Staging → DWH geçişinde kaç satırın neden düştüğünü hesaplar."""
    report = {}

    cur.execute("SELECT COUNT(*) FROM staging.members")
    report['staging_members_total'] = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM staging.members WHERE tc_hash IS NULL OR tc_hash = ''")
    report['members_null_tc'] = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*) FROM (
            SELECT tc_hash FROM staging.members
            WHERE tc_hash IS NOT NULL AND tc_hash != ''
            GROUP BY tc_hash HAVING COUNT(*) > 1
        ) t
    """)
    report['members_duplicates'] = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM dim_member WHERE is_current = TRUE")
    report['dim_member_loaded'] = cur.fetchone()[0]

    report['members_unexplained_loss'] = (
        report['staging_members_total']
        - report['members_null_tc']
        - report['members_duplicates']
        - report['dim_member_loaded']
    )

    cur.execute("SELECT COUNT(*) FROM staging.payments")
    report['staging_payments_total'] = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*) FROM staging.payments sp
        WHERE NOT EXISTS (
            SELECT 1 FROM dim_member dm
            WHERE dm.member_id = sp.member_id AND dm.is_current = TRUE
        )
    """)
    report['payments_fk_mismatch'] = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM fact_payments")
    report['fact_payments_loaded'] = cur.fetchone()[0]

    report['payments_unexplained_loss'] = (
        report['staging_payments_total']
        - report['payments_fk_mismatch']
        - report['fact_payments_loaded']
    )

    logger.info("=" * 55)
    logger.info("SATIR KAYBI RAPORU")
    logger.info(f"  staging.members  toplam          : {report['staging_members_total']}")
    logger.info(f"  - NULL tc_hash filtresi           : {report['members_null_tc']}")
    logger.info(f"  - Duplike (tc_hash bazlı)         : {report['members_duplicates']}")
    logger.info(f"  = dim_member yüklenen             : {report['dim_member_loaded']}")
    logger.info(f"  ? Açıklanamayan kayıp             : {report['members_unexplained_loss']}")
    logger.info(f"  staging.payments toplam           : {report['staging_payments_total']}")
    logger.info(f"  - FK mismatch (member bulunamadı) : {report['payments_fk_mismatch']}")
    logger.info(f"  = fact_payments yüklenen          : {report['fact_payments_loaded']}")
    logger.info(f"  ? Açıklanamayan kayıp             : {report['payments_unexplained_loss']}")
    logger.info("=" * 55)

    return report


# ==========================================
# ANA AKIŞ
# ==========================================
if __name__ == "__main__":
    conn = get_db_connection()
    cur  = conn.cursor()

    send_alert(
    failed_rules=[{
        "rule": "TEST FAIL",
        "table": "test_table",
        "detail": "manuel test"
    }],
    warned_rules=[]
)

    print("\n" + "=" * 55)
    print("DATA QUALITY CHECKS BAŞLIYOR")
    print("=" * 55)

    run_id = start_run(conn, "data_quality_framework")

    staging_results  = check_staging(cur, conn, run_id)
    dwh_results      = check_dwh(cur, conn, run_id)
    business_results = check_business_rules(cur, conn, run_id)
    rowcount_results = check_row_counts(cur, conn, run_id)
    report           = row_loss_report(cur, conn, run_id)

    all_results = (staging_results + dwh_results
                   + business_results + rowcount_results)

    passed = sum(1 for r in all_results if r['status'] == 'PASS')
    warned = sum(1 for r in all_results if r['status'] == 'WARN')
    failed = sum(1 for r in all_results if r['status'] == 'FAIL')

    failed_rules = [r for r in all_results if r['status'] == 'FAIL']
    warned_rules = [r for r in all_results if r['status'] == 'WARN']

    final_status = "FAILED" if failed > 0 else ("WARNING" if warned > 0 else "SUCCESS")
    finish_run(conn, run_id, final_status)

    if failed > 0:
        send_alert(failed_rules, warned_rules)

    print(f"\nToplam: {len(all_results)} kural | PASS: {passed} | WARN: {warned} | FAIL: {failed}")

    if warned > 0:
        print("\nUYARILAR:")
        for r in warned_rules:
            print(f"  ⚠ [{r['table']}] {r['rule']} — {r['detail']}")

    if failed > 0:
        print("\nBAŞARISIZ KURALLAR:")
        for r in failed_rules:
            print(f"  ✗ [{r['table']}] {r['rule']} — {r['detail']}")

    cur.close()
    conn.close()

    