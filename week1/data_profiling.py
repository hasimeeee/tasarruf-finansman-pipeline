import psycopg2
import yaml
import os
import logging

# Loglama ayarları
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

def load_config():
    # Mevcut dosyanın bulunduğu klasörden bir üst klasöre çıkılır
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    # config.yaml dosyasının yolu oluşturulur
    config_path = os.path.join(base_dir, "config/config.yaml")

    # YAML dosyası okunur ve dictionary olarak döndürülür
    with open(config_path, "r") as f:
        return yaml.safe_load(f)
    
def get_connection():
    """
    PostgreSQL veritabanına bağlantı oluşturur.
    """
    config = load_config()["database"]

    return psycopg2.connect(
        host=config["host"],
        port=config["port"],
        database=config["name"],
        user=config["user"],
        password=config["password"]
    )

def run_query(cursor, query):
     """
    Verilen SQL sorgusunu çalıştırır ve sonucu döndürür.
    """

     cursor.execute(query)
     return cursor.fetchall()

def section(title):
    """
    Konsolda başlıkları daha okunur göstermek için
    bölüm ayırıcı format oluşturur.
    """
    print(f"\n{'='*55}")
    print(f"{title}")
    print(f"{'='*55}")

def profile_members(cursor):
    """
    staging.members tablosu üzerinde
    analizlerini gerçekleştirir.
    """
    section("ÜYE PROFİLİ")
    total = run_query(cursor, "SELECT COUNT(*) FROM staging.members")[0][0]
    print(f"Toplam üye: {total:,}")

    section("Statü Dağılımı")
    for row in run_query(cursor, """
        SELECT member_status, COUNT(*) AS adet
        FROM staging.members
        GROUP BY member_status
        ORDER BY adet DESC
    """):
        print(f"{row[0]:<12}: {row[1]:>6,}")

    section("Şehir Dağılımı (Top 10)")
    for row in run_query(cursor, """
        SELECT city, COUNT(*) AS adet
        FROM staging.members
        GROUP BY city
        ORDER BY adet DESC
        LIMIT 10
    """):
        print(f"{row[0]:<12}: {row[1]:>6,}")

    section("Gelir İstatistikleri")
    row = run_query(cursor, """
        SELECT
            ROUND(MIN(income), 2),
            ROUND(MAX(income), 2),
            ROUND(AVG(income), 2),
            ROUND(STDDEV(income), 2)
        FROM staging.members
    """)[0]
    print(f"Min : {row[0]:>12,.2f} TL")
    print(f"Max : {row[1]:>12,.2f} TL")
    print(f"Ort : {row[2]:>12,.2f} TL")
    print(f"Std : {row[3]:>12,.2f} TL")


def profile_plans(cursor):
    """
    staging.plans tablosu üzerinde
    plan bazlı analizleri gerçekleştirir.
    """
    section("PLAN PROFİLİ")
    plans = run_query(cursor, """
        SELECT plan_id, plan_name, duration_months,
               target_amount, monthly_installment
        FROM staging.plans
        ORDER BY plan_id
    """)
    print(f"{'ID':<6} {'Plan Adı':<28} {'Süre':>5} {'Hedef':>12} {'Taksit':>12}")
    print("-" * 65)
    for row in plans:
        print(f"{row[0]:<6} {row[1]:<28} {row[2]:>5} {row[3]:>12,.0f} {row[4]:>12,.2f}")

    section("Plana Göre Abone Sayısı")
    for row in run_query(cursor, """
        SELECT p.plan_name, COUNT(s.subscription_id) AS abone_sayisi
        FROM staging.plans p
        LEFT JOIN staging.subscriptions s ON p.plan_id = s.plan_id
        GROUP BY p.plan_name
        ORDER BY abone_sayisi DESC
    """):
        print(f"{row[0]:<28}: {row[1]:>6,}")


def profile_payments(cursor):
    """
    staging.payments tablosu üzerinde
    ödeme davranışı analizlerini gerçekleştirir.
    """
    section("ÖDEME PROFİLİ")
    total = run_query(cursor, "SELECT COUNT(*) FROM staging.payments")[0][0]
    print(f"Toplam ödeme kaydı: {total:,}")

    section("Ödeme Durumu Dağılımı")
    for row in run_query(cursor, """
        SELECT payment_status, COUNT(*) AS adet,
               ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS yuzde
        FROM staging.payments
        GROUP BY payment_status
        ORDER BY adet DESC
    """):
        print(f"{row[0]:<12}: {row[1]:>8,}  ({row[2]:>5}%)")

    section("Gecikme İstatistikleri (sadece gecikmeli kayıtlar)")
    row = run_query(cursor, """
        SELECT
            ROUND(AVG(days_late), 1)  AS ort_gecikme,
            MAX(days_late)             AS max_gecikme,
            ROUND(STDDEV(days_late), 1) AS std_gecikme,
            COUNT(*)                   AS gecikme_adedi
        FROM staging.payments
        WHERE days_late > 0
    """)[0]
    print(f"Ortalama gecikme : {row[0]:>6} gün")
    print(f"Max gecikme      : {row[1]:>6} gün")
    print(f"Std sapma        : {row[2]:>6} gün")
    print(f"Gecikme adedi    : {row[3]:>6,}")

    section("Kirli Veri Özeti")
    # Geçersiz tutar (negatif)
    neg = run_query(cursor, """
        SELECT COUNT(*) FROM staging.payments WHERE amount_paid < 0
    """)[0][0]
    # Tutarsız tarih (payment_date < due_date ama days_late > 0)
    incons = run_query(cursor, """
        SELECT COUNT(*) FROM staging.payments
        WHERE payment_date < due_date AND days_late > 0
    """)[0][0]
    print(f"Negatif tutar    : {neg:>6,}")
    print(f"Tutarsız tarih   : {incons:>6,}")

def profile_lottery(cursor):
    """
    staging.lottery tablosu üzerinde
    kura katılım ve kazanma analizlerini gerçekleştirir.
    """
    section("KURA PROFİLİ")
    row = run_query(cursor, """
        SELECT
            COUNT(*) AS toplam,
            SUM(CASE WHEN is_winner THEN 1 ELSE 0 END) AS kazanan,
            ROUND(
                SUM(CASE WHEN is_winner THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
            ) AS kazanma_orani
        FROM staging.lottery
    """)[0]
    print(f"Toplam kayıt  : {row[0]:>6,}")
    print(f"Kazanan       : {row[1]:>6,}")
    print(f"Kazanma oranı : {row[2]:>6}%")

    section("Plana Göre Kura Dağılımı")
    for row in run_query(cursor, """
        SELECT p.plan_name,
               COUNT(*) AS toplam_kura,
               SUM(CASE WHEN l.is_winner THEN 1 ELSE 0 END) AS kazanan
        FROM staging.lottery l
        JOIN staging.plans p ON l.plan_id = p.plan_id
        GROUP BY p.plan_name
        ORDER BY toplam_kura DESC
    """):
        print(f"{row[0]:<28}: toplam {row[1]:>5,}  kazanan {row[2]:>4,}")

    section("Aylık Kura Katılım Trendi (Top 10)")
    for row in run_query(cursor, """
        SELECT
            TO_CHAR(lottery_date, 'YYYY-MM') AS ay,
            COUNT(*) AS katilim
        FROM staging.lottery
        GROUP BY ay
        ORDER BY katilim DESC
        LIMIT 10
    """):
        print(f"{row[0]}: {row[1]:>5,}")


def main():
    """
    Programın ana çalıştırma fonksiyonu.
    Tüm profiling analizlerini sırayla çalıştırır.
    """
    logger.info("Profiling started")

    conn = get_connection()
    cursor = conn.cursor()

    try:
        profile_members(cursor)
        profile_plans(cursor)
        profile_payments(cursor)
        profile_lottery(cursor)

        logger.info("Profiling completed successfully")

    except Exception as e:
        logger.error(f"Hata: {e}")
        raise

    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    main()
      
          

     


       


      
