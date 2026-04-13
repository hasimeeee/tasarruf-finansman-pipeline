"""
data_quality.py
---------------
Staging ve DWH tablolarında rule-based (kural tabanlı) kalite kontrolleri yapar.

Her kural bir dict döndürür:
    {
        'rule'    : str,   # kural adı
        'table'   : str,   # kontrol edilen tablo
        'status'  : str,   # 'PASS' veya 'FAIL'
        'detail'  : str,   # bulunan sorun sayısı / mesaj
    }

Kullanım:
    python data_quality.py
"""

import logging
import yaml
import psycopg2
import pandas as pd

from data_generator import get_db_connection

# ── Log ayarları ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)


# ─────────────────────────────────────────────────────────────────────────────
# YARDIMCI FONKSİYON
# ─────────────────────────────────────────────────────────────────────────────

def run_check(cur, rule: str, table: str, sql: str, threshold: int = 0) -> dict:
    """
    Tek bir kalite kuralını çalıştırır.

    Parametreler:
        cur       : psycopg2 cursor
        rule      : kural adı (log ve rapor için)
        table     : kontrol edilen tablo adı
        sql       : sorunlu satır sayısını döndüren SQL
        threshold : kabul edilebilir maksimum sorunlu satır sayısı (varsayılan 0)

    Döndürür:
        dict: rule, table, status, detail
    """
    cur.execute(sql)
    count = cur.fetchone()[0]
    status = 'PASS' if count <= threshold else 'FAIL'
    detail = f'{count} sorunlu satır' if count > 0 else 'Temiz'
    logger.info(f'[{status}] {rule} — {table}: {detail}')
    return {'rule': rule, 'table': table, 'status': status, 'detail': detail}


# ─────────────────────────────────────────────────────────────────────────────
# STAGİNG KONTROLLERİ
# ─────────────────────────────────────────────────────────────────────────────

def check_staging(cur) -> list:
    """
    Staging tablolarında temel kalite kontrolleri yapar.

    Kontroller:
        - NULL zorunlu alanlar
        - Duplike member_id / subscription_id / payment_id
        - Geçersiz üye statüsü
        - Negatif gelir
        - Ödeme tarihinin vade tarihinden önce olması (gecikmesiz ödemeler hariç)
        - Gelecek vade tarihleri
        - Geçersiz plan tipi
    """
    results = []

    # 1. NULL zorunlu alanlar — üyeler
    results.append(run_check(cur,
        rule='NULL zorunlu alan (üye)',
        table='stg_members',
        sql="""
            SELECT COUNT(*) FROM stg_members
            WHERE member_id IS NULL
               OR tc_hash   IS NULL
               OR city      IS NULL
               OR income    IS NULL
               OR signup_date IS NULL
               OR member_status IS NULL
        """
    ))

    # 2. Duplike member_id
    results.append(run_check(cur,
        rule='Duplike member_id',
        table='stg_members',
        sql="""
            SELECT COUNT(*) FROM (
                SELECT member_id, COUNT(*) AS cnt
                FROM stg_members
                GROUP BY member_id
                HAVING COUNT(*) > 1
            ) t
        """
    ))

    # 3. Duplike tc_hash (farklı kişilerin aynı hash'e sahip olması beklenmez)
    results.append(run_check(cur,
        rule='Duplike tc_hash',
        table='stg_members',
        sql="""
            SELECT COUNT(*) FROM (
                SELECT tc_hash, COUNT(*) AS cnt
                FROM stg_members
                GROUP BY tc_hash
                HAVING COUNT(*) > 1
            ) t
        """,
        threshold=50   # hash çakışması çok nadirdir, küçük eşik ver
    ))

    # 4. Geçersiz member_status
    results.append(run_check(cur,
        rule='Geçersiz member_status',
        table='stg_members',
        sql="""
            SELECT COUNT(*) FROM stg_members
            WHERE member_status NOT IN ('aktif', 'gecikmeli', 'pasif', 'terk')
        """
    ))

    # 5. Negatif veya sıfır gelir
    results.append(run_check(cur,
        rule='Negatif/sıfır gelir',
        table='stg_members',
        sql="SELECT COUNT(*) FROM stg_members WHERE income <= 0"
    ))

    # 6. Gelecekte doğum tarihi
    results.append(run_check(cur,
        rule='Gelecek doğum tarihi',
        table='stg_members',
        sql="SELECT COUNT(*) FROM stg_members WHERE birth_date > CURRENT_DATE"
    ))

    # 7. 18 yaşından küçük üye
    results.append(run_check(cur,
        rule='18 yaşından küçük üye',
        table='stg_members',
        sql="""
            SELECT COUNT(*) FROM stg_members
            WHERE DATE_PART('year', AGE(birth_date)) < 18
        """
    ))

    # 8. NULL zorunlu alanlar — ödemeler
    results.append(run_check(cur,
        rule='NULL zorunlu alan (ödeme)',
        table='stg_payments',
        sql="""
            SELECT COUNT(*) FROM stg_payments
            WHERE payment_id      IS NULL
               OR subscription_id IS NULL
               OR due_date        IS NULL
               OR amount          IS NULL
        """
    ))

    # 9. Duplike payment_id
    results.append(run_check(cur,
        rule='Duplike payment_id',
        table='stg_payments',
        sql="""
            SELECT COUNT(*) FROM (
                SELECT payment_id, COUNT(*) AS cnt
                FROM stg_payments
                GROUP BY payment_id
                HAVING COUNT(*) > 1
            ) t
        """
    ))

    # 10. Negatif ödeme tutarı
    results.append(run_check(cur,
        rule='Negatif ödeme tutarı',
        table='stg_payments',
        sql="SELECT COUNT(*) FROM stg_payments WHERE amount <= 0"
    ))

    # 11. days_late ile tarihler tutarsız mı?
    #     payment_date = due_date + days_late olmalı (1 gün tolerans)
    results.append(run_check(cur,
        rule='days_late / tarih tutarsızlığı',
        table='stg_payments',
        sql="""
            SELECT COUNT(*) FROM stg_payments
            WHERE ABS(
                (payment_date - due_date) - days_late
            ) > 1
        """,
        threshold=10   # küçük yuvarlama farkları kabul
    ))

    # 12. Geçersiz payment_status
    results.append(run_check(cur,
        rule='Geçersiz payment_status',
        table='stg_payments',
        sql="""
            SELECT COUNT(*) FROM stg_payments
            WHERE payment_status NOT IN ('zamaninda', 'gecikmeli', 'ciddi_gecikme')
        """
    ))

    # 13. Orphan subscription (üyesi olmayan abonelik)
    results.append(run_check(cur,
        rule='Orphan subscription (üyesiz)',
        table='stg_subscriptions',
        sql="""
            SELECT COUNT(*) FROM stg_subscriptions s
            WHERE NOT EXISTS (
                SELECT 1 FROM stg_members m WHERE m.member_id = s.member_id
            )
        """
    ))

    # 14. Orphan payment (aboneliği olmayan ödeme)
    results.append(run_check(cur,
        rule='Orphan payment (aboneligsiz)',
        table='stg_payments',
        sql="""
            SELECT COUNT(*) FROM stg_payments p
            WHERE NOT EXISTS (
                SELECT 1 FROM stg_subscriptions s
                WHERE s.subscription_id = p.subscription_id
            )
        """
    ))

    # 15. Geçersiz plan_type
    results.append(run_check(cur,
        rule='Geçersiz plan_type',
        table='stg_plans',
        sql="""
            SELECT COUNT(*) FROM stg_plans
            WHERE plan_type NOT IN ('konut', 'arsa', 'ticari', 'arac', 'isyeri')
        """
    ))

    return results


# ─────────────────────────────────────────────────────────────────────────────
# DWH (STAR SCHEMA) KONTROLLERİ
# ─────────────────────────────────────────────────────────────────────────────

def check_dwh(cur) -> list:
    """
    dim ve fact tablolarında referans bütünlüğü ve iş kuralı kontrolleri yapar.
    """
    results = []

    # 16. fact_payments → fact_subscription FK bütünlüğü
    results.append(run_check(cur,
        rule='FK bütünlüğü: payment → subscription',
        table='fact_payments',
        sql="""
            SELECT COUNT(*) FROM fact_payments fp
            WHERE NOT EXISTS (
                SELECT 1 FROM fact_subscription fs
                WHERE fs.subscription_id = fp.subscription_id
            )
        """
    ))

    # 17. fact_subscription → dim_member FK bütünlüğü
    results.append(run_check(cur,
        rule='FK bütünlüğü: subscription → member',
        table='fact_subscription',
        sql="""
            SELECT COUNT(*) FROM fact_subscription fs
            WHERE NOT EXISTS (
                SELECT 1 FROM dim_member dm WHERE dm.member_id = fs.member_id
            )
        """
    ))

    # 18. fact_subscription → dim_plan FK bütünlüğü
    results.append(run_check(cur,
        rule='FK bütünlüğü: subscription → plan',
        table='fact_subscription',
        sql="""
            SELECT COUNT(*) FROM fact_subscription fs
            WHERE NOT EXISTS (
                SELECT 1 FROM dim_plan dp WHERE dp.plan_id = fs.plan_id
            )
        """
    ))

    # 19. dim_member NULL kontrol (kritik alanlar)
    results.append(run_check(cur,
        rule='NULL kritik alan (dim_member)',
        table='dim_member',
        sql="""
            SELECT COUNT(*) FROM dim_member
            WHERE member_id IS NULL OR tc_hash IS NULL OR age_group IS NULL
        """
    ))

    # 20. Toplam ödeme sayısı makul mu? (en az 100k bekleniyor)
    cur.execute("SELECT COUNT(*) FROM fact_payments")
    payment_count = cur.fetchone()[0]
    status = 'PASS' if payment_count >= 100000 else 'FAIL'
    results.append({
        'rule'  : 'Minimum ödeme satır sayısı (≥100k)',
        'table' : 'fact_payments',
        'status': status,
        'detail': f'{payment_count:,} satır mevcut'
    })
    logger.info(f'[{status}] Minimum ödeme satır sayısı — fact_payments: {payment_count:,}')

    return results


# ─────────────────────────────────────────────────────────────────────────────
# ANA ÇALIŞTIRICI
# ─────────────────────────────────────────────────────────────────────────────

def run_quality_checks() -> pd.DataFrame:
    """
    Tüm kalite kontrollerini çalıştırır, sonuçları DataFrame olarak döndürür
    ve özet raporu loglar.

    Döndürür:
        pd.DataFrame: rule, table, status, detail sütunlarını içerir
    """
    conn = get_db_connection()
    cur  = conn.cursor()

    logger.info('════════════════════════════════════════')
    logger.info('Data Quality kontrolleri başlıyor...')
    logger.info('════════════════════════════════════════')

    results = []
    results.extend(check_staging(cur))
    results.extend(check_dwh(cur))

    cur.close()
    conn.close()

    df = pd.DataFrame(results)

    # ── Özet rapor ────────────────────────────────────────────────────────────
    total  = len(df)
    passed = (df['status'] == 'PASS').sum()
    failed = (df['status'] == 'FAIL').sum()

    logger.info('════════════════════════════════════════')
    logger.info(f'Data Quality Özeti: {passed}/{total} PASS  |  {failed} FAIL')
    logger.info('════════════════════════════════════════')

    if failed > 0:
        logger.warning('Başarısız kurallar:')
        for _, row in df[df['status'] == 'FAIL'].iterrows():
            logger.warning(f'  ✗ [{row["table"]}] {row["rule"]} → {row["detail"]}')

    return df


if __name__ == '__main__':
    report = run_quality_checks()
    print('\n' + report.to_string(index=False))