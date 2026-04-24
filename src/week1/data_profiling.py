import psycopg2
import yaml
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def load_config():
    with open("config.yaml", "r") as f:
        return yaml.safe_load(f)


def get_connection():
    config = load_config()["database"].copy()
    if "name" in config:
        config["dbname"] = config.pop("name")
    return psycopg2.connect(**config)


def run_query(cursor, query):
    cursor.execute(query)
    return cursor.fetchall()


def section(title):
    print(f"\n{'='*55}")
    print(f"  {title}")
    print(f"{'='*55}")


# ==========================================
# MEMBERS
# ==========================================
def profile_members(cursor):
    section("ÜYE PROFİLİ")
    total = run_query(cursor, "SELECT COUNT(*) FROM staging_members")[0][0]
    print(f"Toplam üye: {total:,}")

    section("Statü Dağılımı")
    for row in run_query(cursor, """
        SELECT status, COUNT(*) AS adet
        FROM staging_members
        GROUP BY status
        ORDER BY adet DESC
    """):
        print(f"  {row[0]:<12}: {row[1]:>6,}")

    section("Şehir Dağılımı (Top 10)")
    for row in run_query(cursor, """
        SELECT city, COUNT(*) AS adet
        FROM staging_members
        GROUP BY city
        ORDER BY adet DESC
        LIMIT 10
    """):
        print(f"  {row[0]:<15}: {row[1]:>6,}")

    section("Gelir İstatistikleri")
    row = run_query(cursor, """
        SELECT
            ROUND(MIN(income), 2),
            ROUND(MAX(income), 2),
            ROUND(AVG(income), 2),
            ROUND(STDDEV(income), 2)
        FROM staging_members
    """)[0]
    print(f"  Min : {float(row[0]):>12,.2f} TL")
    print(f"  Max : {float(row[1]):>12,.2f} TL")
    print(f"  Ort : {float(row[2]):>12,.2f} TL")
    print(f"  Std : {float(row[3]):>12,.2f} TL")

    section("NULL / Kirli Veri")
    null_tc = run_query(cursor, "SELECT COUNT(*) FROM staging_members WHERE tc_hash IS NULL")[0][0]
    dupes   = run_query(cursor, """
        SELECT COUNT(*) FROM (
            SELECT tc_hash FROM staging_members
            WHERE tc_hash IS NOT NULL
            GROUP BY tc_hash HAVING COUNT(*) > 1
        ) t
    """)[0][0]
    print(f"  NULL tc_hash   : {null_tc:>6,}")
    print(f"  Duplike tc_hash: {dupes:>6,}")


# ==========================================
# PLANS
# ==========================================
def profile_plans(cursor):
    section("PLAN PROFİLİ")
    plans = run_query(cursor, """
        SELECT plan_id, plan_name, duration_months,
               target_amount, monthly_installment
        FROM staging_plans
        ORDER BY plan_id
    """)
    print(f"  {'ID':<6} {'Plan Adı':<28} {'Süre':>5} {'Hedef':>12} {'Taksit':>12}")
    print("  " + "-" * 65)
    for row in plans:
        print(f"  {row[0]:<6} {row[1]:<28} {row[2]:>5} {float(row[3]):>12,.0f} {float(row[4]):>12,.2f}")


# ==========================================
# PAYMENTS
# ==========================================
def profile_payments(cursor):
    section("ÖDEME PROFİLİ")
    total = run_query(cursor, "SELECT COUNT(*) FROM staging_payments")[0][0]
    print(f"  Toplam ödeme kaydı: {total:,}")

    section("Ödeme Durumu Dağılımı")
    for row in run_query(cursor, """
        SELECT payment_status, COUNT(*) AS adet,
               ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS yuzde
        FROM staging_payments
        GROUP BY payment_status
        ORDER BY adet DESC
    """):
        print(f"  {row[0]:<12}: {row[1]:>8,}  ({row[2]:>5}%)")

    section("Gecikme İstatistikleri")
    row = run_query(cursor, """
        SELECT
            ROUND(AVG((paid_date - due_date)), 1)  AS ort_gecikme,
            MAX((paid_date - due_date))             AS max_gecikme,
            COUNT(*) FILTER (WHERE paid_date > due_date) AS gecikme_adedi
        FROM staging_payments
        WHERE paid_date IS NOT NULL
    """)[0]
    print(f"  Ortalama gecikme : {row[0]:>6} gün")
    print(f"  Max gecikme      : {row[1]:>6} gün")
    print(f"  Gecikme adedi    : {row[2]:>6,}")

    section("Kirli Veri Özeti")
    neg = run_query(cursor, "SELECT COUNT(*) FROM staging_payments WHERE paid_amount < 0")[0][0]
    print(f"  Negatif tutar    : {neg:>6,}")


# ==========================================
# LOTTERY
# ==========================================
def profile_lottery(cursor):
    section("KURA PROFİLİ")
    row = run_query(cursor, """
        SELECT
            COUNT(*) AS toplam,
            SUM(CASE WHEN is_winner THEN 1 ELSE 0 END) AS kazanan,
            ROUND(
                SUM(CASE WHEN is_winner THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2
            ) AS kazanma_orani
        FROM staging_lottery
    """)[0]
    print(f"  Toplam kayıt  : {row[0]:>6,}")
    print(f"  Kazanan       : {row[1]:>6,}")
    print(f"  Kazanma oranı : {row[2]:>6}%")

    section("Plana Göre Kura Dağılımı")
    for row in run_query(cursor, """
        SELECT p.plan_name,
               COUNT(*) AS toplam_kura,
               SUM(CASE WHEN l.is_winner THEN 1 ELSE 0 END) AS kazanan
        FROM staging_lottery l
        JOIN staging_plans p ON l.plan_id = p.plan_id
        GROUP BY p.plan_name
        ORDER BY toplam_kura DESC
    """):
        print(f"  {row[0]:<30}: toplam {row[1]:>5,}  kazanan {row[2]:>4,}")

    section("Aylık Kura Katılım Trendi (Top 10)")
    for row in run_query(cursor, """
        SELECT TO_CHAR(lottery_date, 'YYYY-MM') AS ay, COUNT(*) AS katilim
        FROM staging_lottery
        GROUP BY ay
        ORDER BY katilim DESC
        LIMIT 10
    """):
        print(f"  {row[0]}: {row[1]:>5,}")


# ==========================================
# ANA FONKSİYON
# ==========================================
def main():
    logger.info("Profiling başlıyor...")
    conn   = get_connection()
    cursor = conn.cursor()
    try:
        profile_members(cursor)
        profile_plans(cursor)
        profile_payments(cursor)
        profile_lottery(cursor)
        logger.info("Profiling tamamlandı.")
    except Exception as e:
        logger.error(f"Hata: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == '__main__':
    main()

     


       


      
