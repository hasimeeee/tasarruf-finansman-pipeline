"""
Tasarruf Finansman - Data Quality Kontrolleri

Değişiklikler:
  - staging.subscriptions referansları kaldırıldı (tablo mevcut değil)
  - staging.members sütun adı: 'member_status' (eskiden 'status' karışıklığı vardı)
  - staging.payments sütun adları düzeltildi: due_amount, paid_date, plan_id
    (eskiden subscription_id ve amount_due/amount_paid uyumsuzlukları vardı)
  - check_dwh: fact_subscription → fact_payments olarak güncellendi
  - Satır kaybı raporu eklendi (geri bildirim #1)
  - Tüm check'ler çalışır durumda
"""

import logging
import sys
import os
from datetime import date

sys.path.append(os.path.dirname(__file__))
from config_loader import load_config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_db_connection():
    config = load_config()
    db = config["database"].copy()
    if "name" in db:
        db["dbname"] = db.pop("name")
    import psycopg2
    return psycopg2.connect(**db)


def run_check(cur, rule: str, table: str, sql: str, threshold: int = 0) -> dict:
    """Tek bir kalite kuralını çalıştırır ve sonucu döndürür."""
    cur.execute(sql)
    count = cur.fetchone()[0]
    status = "PASS" if count <= threshold else "FAIL"
    detail = f"{count} sorunlu satır" if count > 0 else "Temiz"
    logger.info(f"[{status}] {rule} — {table}: {detail}")
    return {"rule": rule, "table": table, "status": status, "count": count, "detail": detail}


# ==========================================
# STAGING KALİTE KONTROLLERİ
# ==========================================

def check_staging(cur) -> list:
    results = []

    # --- staging.members ---
    results.append(run_check(cur,
        "NULL zorunlu alan",
        "staging.members",
        """
        SELECT COUNT(*) FROM staging.members
        WHERE member_id IS NULL
           OR city IS NULL
           OR income IS NULL
           OR signup_date IS NULL
           OR member_status IS NULL
        """
        # tc_hash ayrıca kontrol ediliyor — kasıtlı NULL'lar var
    ))

    results.append(run_check(cur,
        "NULL tc_hash oranı (bilgi amaçlı)",
        "staging.members",
        """
        SELECT COUNT(*) FROM staging.members
        WHERE tc_hash IS NULL OR tc_hash = ''
        """
    ))

    results.append(run_check(cur,
        "Duplicate member_id",
        "staging.members",
        """
        SELECT COUNT(*) FROM (
            SELECT member_id
            FROM staging.members
            GROUP BY member_id
            HAVING COUNT(*) > 1
        ) t
        """
    ))

    results.append(run_check(cur,
        "Geçersiz member_status",
        "staging.members",
        """
        SELECT COUNT(*) FROM staging.members
        WHERE member_status NOT IN ('aktif','gecikmeli','pasif','terk')
          AND member_status IS NOT NULL
        """
    ))

    results.append(run_check(cur,
        "Geçersiz gelir (income <= 0)",
        "staging.members",
        """
        SELECT COUNT(*) FROM staging.members
        WHERE income <= 0
        """
    ))

    results.append(run_check(cur,
        "18 yaş altı üye",
        "staging.members",
        """
        SELECT COUNT(*) FROM staging.members
        WHERE birth_date IS NOT NULL
          AND DATE_PART('year', AGE(birth_date)) < 18
        """
    ))

    results.append(run_check(cur,
        "NULL email veya phone",
        "staging.members",
        """
        SELECT COUNT(*) FROM staging.members
        WHERE email IS NULL OR phone IS NULL
        """
    ))

    # --- staging.payments ---
    results.append(run_check(cur,
        "NULL zorunlu alan",
        "staging.payments",
        """
        SELECT COUNT(*) FROM staging.payments
        WHERE payment_id IS NULL
           OR member_id IS NULL
           OR due_date IS NULL
           OR due_amount IS NULL
        """
    ))

    results.append(run_check(cur,
        "Duplicate payment_id",
        "staging.payments",
        """
        SELECT COUNT(*) FROM (
            SELECT payment_id
            FROM staging.payments
            GROUP BY payment_id
            HAVING COUNT(*) > 1
        ) t
        """
    ))

    results.append(run_check(cur,
        "Negatif ödeme tutarı",
        "staging.payments",
        """
        SELECT COUNT(*) FROM staging.payments
        WHERE paid_amount < 0
        """
    ))

    results.append(run_check(cur,
        "Geçersiz payment_status",
        "staging.payments",
        """
        SELECT COUNT(*) FROM staging.payments
        WHERE payment_status NOT IN ('odendi','gecikmeli','kismi','odenmedi')
          AND payment_status IS NOT NULL
        """
    ))

    results.append(run_check(cur,
        "Tutarsız ödeme tarihi (paid_date < due_date)",
        "staging.payments",
        """
        SELECT COUNT(*) FROM staging.payments
        WHERE paid_date IS NOT NULL
          AND paid_date < due_date
        """
    ))

    results.append(run_check(cur,
        "Orphan payments (member yok)",
        "staging.payments",
        """
        SELECT COUNT(*) FROM staging.payments p
        WHERE NOT EXISTS (
            SELECT 1 FROM staging.members m
            WHERE m.member_id = p.member_id
        )
        """
    ))

    # --- staging.plans ---
    results.append(run_check(cur,
        "Geçersiz plan_type",
        "staging.plans",
        """
        SELECT COUNT(*) FROM staging.plans
        WHERE plan_type NOT IN ('konut','arsa','ticari','arac','isyeri')
        """
    ))

    results.append(run_check(cur,
        "Geçersiz monthly_installment",
        "staging.plans",
        """
        SELECT COUNT(*) FROM staging.plans
        WHERE monthly_installment <= 0
           OR monthly_installment IS NULL
        """
    ))

    # --- staging.lottery ---
    results.append(run_check(cur,
        "Orphan lottery (member yok)",
        "staging.lottery",
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
# DWH KALİTE KONTROLLERİ
# ==========================================

def check_dwh(cur) -> list:
    results = []

    results.append(run_check(cur,
        "fact_payments → dim_member FK",
        "fact_payments",
        """
        SELECT COUNT(*) FROM fact_payments fp
        WHERE NOT EXISTS (
            SELECT 1 FROM dim_member dm
            WHERE dm.member_key = fp.member_key
        )
        """
    ))

    results.append(run_check(cur,
        "fact_payments → dim_plan FK",
        "fact_payments",
        """
        SELECT COUNT(*) FROM fact_payments fp
        WHERE NOT EXISTS (
            SELECT 1 FROM dim_plan dp
            WHERE dp.plan_key = fp.plan_key
        )
        """
    ))

    results.append(run_check(cur,
        "fact_payments → dim_date FK",
        "fact_payments",
        """
        SELECT COUNT(*) FROM fact_payments fp
        WHERE NOT EXISTS (
            SELECT 1 FROM dim_date dd
            WHERE dd.date_key = fp.date_key
        )
        """
    ))

    results.append(run_check(cur,
        "fact_lottery → dim_member FK",
        "fact_lottery",
        """
        SELECT COUNT(*) FROM fact_lottery fl
        WHERE NOT EXISTS (
            SELECT 1 FROM dim_member dm
            WHERE dm.member_key = fl.member_key
        )
        """
    ))

    results.append(run_check(cur,
        "cumulative_paid_ratio aralık kontrolü (0-1)",
        "fact_lottery",
        """
        SELECT COUNT(*) FROM fact_lottery
        WHERE cumulative_paid_ratio < 0
           OR cumulative_paid_ratio > 1
        """
    ))

    results.append(run_check(cur,
        "dim_member SCD2 tutarlılık (is_current)",
        "dim_member",
        """
        SELECT COUNT(*) FROM (
            SELECT member_id
            FROM dim_member
            WHERE is_current = TRUE
            GROUP BY member_id
            HAVING COUNT(*) > 1
        ) t
        """
    ))

    return results


# ==========================================
# SATIR KAYBI RAPORU
# ==========================================

def row_loss_report(cur) -> dict:
    """
    Staging → DWH geçişinde kaç satırın neden düştüğünü hesaplar.
    Geri bildirim #1 gereği: NULL filtresi / duplike / FK mismatch ayrımı.
    """
    report = {}

    # --- members ---
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

    # --- payments ---
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

    # Log özeti
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

    print("\n" + "=" * 55)
    print("DATA QUALITY CHECKS BAŞLIYOR")
    print("=" * 55)

    staging_results = check_staging(cur)
    dwh_results     = check_dwh(cur)
    report          = row_loss_report(cur)

    # Özet
    all_results = staging_results + dwh_results
    passed = sum(1 for r in all_results if r['status'] == 'PASS')
    failed = sum(1 for r in all_results if r['status'] == 'FAIL')

    print(f"\nToplam: {len(all_results)} kural | PASS: {passed} | FAIL: {failed}")

    if failed > 0:
        print("\nBAŞARISIZ KURALLAR:")
        for r in all_results:
            if r['status'] == 'FAIL':
                print(f"  ✗ [{r['table']}] {r['rule']} — {r['detail']}")

    cur.close()
    conn.close()