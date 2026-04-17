import random #rastgele sayı üretmek için
import hashlib #TC numarasını güvenli şifrelemek için
import pandas as pd # veriyi tablo halinde işlemek ve tutmak için
from faker import Faker #türkçe sahte isim, adres, telefon üretmek için
from datetime import datetime, timedelta, date # tarih işlemleri için
from dateutil.relativedelta import relativedelta  # ay bazlı doğru tarih hesabı için 
import psycopg2 #python ve sql arası köprü
from psycopg2.extras import execute_values
import yaml #config.yaml dosyasını okuması için
import logging #pipline çalışırken ne olduğunu takip etmek için
fake = Faker('tr_TR') #feker'ın türkçe veri üretmesi için
random.seed(42) #sbt veri üretimi için test sırasında tutarlılık için
#log ayarları
logging.basicConfig(
    level = logging.INFO,  #genel bilgi mesajları
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
#veritabanı şifresi, şehir listesi vb. ayarları kod içine yazmak yerine config.yaml'dan çekioyuz. Bu sayede ayarlar degısınce koda dokunmadan yaml dosyasını degıstırıyoruz.
from src.config_loader import load_config  

config = load_config()


#şehir dağılımı
CITIES = {
    'Istanbul': 30, 'Ankara': 15, 'Konya': 10,
    'Izmir': 8, 'Bursa': 6, 'Gaziantep': 5,
    'Kayseri': 4, 'Diyarbakir': 3, 'Antalya': 3,
    'Adana': 3, 'Samsun': 2, 'Trabzon': 2,
    'Eskisehir': 2, 'Mersin': 2, 'Diger': 5
}
#plan tipleri
PLAN_TYPES = ['konut', 'arsa', 'ticari', 'arac', 'isyeri']
#üye statüleri
MEMBER_STATUSES = ['aktif', 'gecikmeli', 'pasif', 'terk']
# Türkiye resmi tatilleri (sabit tarihler) — Ramazan hariç
FIXED_HOLIDAYS = [
    (1, 1),   # Yılbaşı
    (4, 23),  # Ulusal Egemenlik
    (5, 1),   # İşçi Bayramı
    (5, 19),  # Atatürk'ü Anma
    (7, 15),  # Demokrasi Bayramı
    (8, 30),  # Zafer Bayramı
    (10, 29), # Cumhuriyet Bayramı
]
# Ramazan Bayramı ve Kurban Bayramı başlangıç tarihleri (2022-2026)
RELIGIOUS_HOLIDAYS = [
    # (yıl, ay, gün, kaç_gün) — Ramazan Bayramı
    (2022, 5, 2, 3),
    (2023, 4, 21, 3),
    (2024, 4, 10, 3),
    (2025, 3, 30, 3),
    (2026, 3, 20, 3),
    # Kurban Bayramı
    (2022, 7, 9, 4),
    (2023, 6, 28, 4),
    (2024, 6, 16, 4),
    (2025, 6, 6, 4),
    (2026, 5, 27, 4),
]
def _gecikme_gun_uret(prob=0.5):
    """
    Gecikme gününü üstel dağılımla üretir.
    Çoğu küçük gecikme, azı büyük gecikme.
    """
    if random.random() > prob:
        return 0
    
    # çoğunluk 1-10 gün, nadiren 30+
    return int(random.expovariate(1/5)) + 1
def build_holiday_set(year_start=2022, year_end=2027):
    """
    2022-2026 arası tüm resmi tatil günlerini bir set olarak döndürür.
    Ödeme gecikmelerinde bayram etkisini simüle etmek için kullanılır.
    """
    holidays = set()
    for year in range(year_start, year_end):
        for month, day in FIXED_HOLIDAYS:
            holidays.add(date(year, month, day))
    for year, month, day, duration in RELIGIOUS_HOLIDAYS:
        for d in range(duration):
            holidays.add(date(year, month, day) + timedelta(days=d))
    return holidays
 
HOLIDAYS = build_holiday_set()
def is_near_holiday(d, window=7):
    """
    Verilen tarih bir tatilden önceki 7 gün içinde mi?
    Evet ise gecikme olasılığı artar.
    """
    for h in HOLIDAYS:
        if 0 <= (h - d).days <= window:
            return True
    return False
def get_db_connection():
    """
    config.yaml'dan alınan bilgilerle PostgreSQL bağlantısı oluşturur.
    Döndürür:
        psycopg2.connection
    """
    return psycopg2.connect(
        host=config['database']['host'],
        port=config['database']['port'],
        dbname=config['database']['name'],
        user=config['database']['user'],
        password=config['database']['password']
    )

def generate_members(num_members): #kaç üye üretileceği config'den gelecek
    """
    üye üretme fonksiyonu 
    """
    members = [] #üretilen üyeler buraya eklenecek
    for i in range(num_members): #döngü 15.000 kez dönecek ve her bir turda yeni üye üretecek
        tc_no = str(random.randint(10000000000, 99999999999))#tc üretir
        tc_hash = hashlib.sha256(tc_no.encode()).hexdigest() #güvenlik için hash'e çevirir
        city = random.choices(
            population=list(CITIES.keys()), #seçim yapılacak şehirleri listeler
            weights=list(CITIES.values()), #şehrin ağırlığı
            k=1 #1 tane seç
        )[0] #liste yerine direkt değer
        district = fake.city()
        birth_date = fake.date_of_birth(minimum_age=18, maximum_age=65)
        income = round(random.choices(
            population=[20000, 35000, 55000, 85000, 150000],
            weights=[15, 30, 25, 20, 10],
            k=1 #bu 5 parametreye göre rastgele verı uretır
        )[0] * random.uniform(0.8, 1.2), 2) #0.8 ile 1.2 arasında sayı üretiyor %20'lik sapma
        #2022-2026 rası tarih üretir
        signup_date = fake.date_between(
            start_date=date(2022,1,1),
            end_date=date(2026,4,1)
        )
        #ağırlıklara göre statü üretir
        member_status = random.choices(
            population=MEMBER_STATUSES,
            weights=[60, 20, 10, 10], #aktid, gecikmeli, pasif, terk
        )[0]
        #iletişim bilgileri
        full_name = fake.name()
        phone = "+90" + str(random.randint(5000000000, 5999999999))
        email = fake.email()
        #  Ürettiğimiz tüm bilgileri bir dictionary olarak members listesine ekler
        members.append({
            'member_id': f'M{i+1:05d}',
            'full_name': full_name,
            'tc_hash': tc_hash,
            'city': city,
            'district': district,
            'birth_date': birth_date,
            'income': income,
            'signup_date': signup_date,
            'member_status': member_status,
            'phone': phone,
            'email': email
        })
        # Kaç üye üretildiğini loglar
    logger.info(f'{num_members} üye üretildi.')
    
    return members
def generate_plans():
    """
    Tasarruf finansman planlarını üretir.
    Döndürür:
        list[dict]
    """
    plans = []
    plan_configs = [
        {'plan_id': 'P001', 'plan_name': 'Konut Cekilisli 48 Ay',  'plan_type': 'konut',  'duration_months': 48,  'target_amount': 3000000},
        {'plan_id': 'P002', 'plan_name': 'Konut Bireysel 120 Ay',  'plan_type': 'konut',  'duration_months': 120, 'target_amount': 5000000},
        {'plan_id': 'P003', 'plan_name': 'Arac Cekilisli 60 Ay',   'plan_type': 'arac',   'duration_months': 60,  'target_amount': 1500000},
        {'plan_id': 'P004', 'plan_name': 'Isyeri Bireysel 48 Ay',  'plan_type': 'isyeri', 'duration_months': 48,  'target_amount': 8000000},
        {'plan_id': 'P005', 'plan_name': 'Konut Uzun Vade 240 Ay', 'plan_type': 'konut',  'duration_months': 240, 'target_amount': 10000000},
    ]
    for plan in plan_configs:
        plan['monthly_installment'] = round(
            plan['target_amount'] / plan['duration_months'], 2
        )
        plans.append(plan)
 
    logger.info(f'{len(plans)} plan üretildi.')
    return plans
def generate_subscriptions(members, plans):
    subscriptions = []
 
    for i, member in enumerate(members):
        income = member['income']
        if income >= 100_000:
            plan = random.choices(plans, weights=[10, 10, 40, 10, 30], k=1)[0]
        elif income >= 50_000:
            plan = random.choices(plans, weights=[20, 20, 30, 20, 10], k=1)[0]
        else:
            plan = random.choices(plans, weights=[30, 30, 10, 25, 5], k=1)[0]

        start_date = member['signup_date']
        expected_end_date = start_date + relativedelta(months=plan['duration_months'])

        kura_won = random.random() < 0.20
        kura_date = None
        if kura_won:
            kura_offset = random.randint(6, min(24, plan['duration_months']))
            kura_date = start_date + relativedelta(months=kura_offset)

        # ↓ BUNLAR FOR'UN İÇİNDE OLMALI — 8 boşluk girinti
        status_map = {
            'aktif': 'aktif',
            'gecikmeli': 'gecikmeli',
            'pasif': 'pasif',
            'terk': 'terk'
        }
        subscription_status = status_map.get(member['member_status'], 'aktif')
        subscriptions.append({
            'subscription_id': f'S{i+1:05d}',
            'member_id': member['member_id'],
            'plan_id': plan['plan_id'],
            'start_date': start_date,
            'expected_end_date': expected_end_date,
            'kura_date': kura_date,
            'kura_won': kura_won,
            'subscription_status': subscription_status,
            '_income': income
        })

    logger.info(f'{len(subscriptions)} abonelik üretildi.')
    return subscriptions

def generate_payments(subscriptions, plans):
    """
    Her abonelik için aylık taksit ödeme kayıtları üretir.
 
    Gerçek hayat düzeltmeleri:
    - Vadeler relativedelta ile hesaplanır (ay bazlı, doğru)
    - Gecikme günleri üstel dağılır (çoğu kısa, az sayısı uzun)
    - Düşük gelirli üyeler daha çok gecikir
    - Bayram öncesi dönemlerde gecikme olasılığı artar
    """
    payments = []
    payment_counter = 1
    plan_lookup = {p['plan_id']: p for p in plans}
    today = date.today()
 
    for sub in subscriptions:
        plan = plan_lookup[sub['plan_id']]
        amount_due = plan['monthly_installment']
        start_date = sub['start_date']
        duration = plan['duration_months']
        income = sub.get('_income', 55000)
 
        if sub['subscription_status'] == 'terk':
            active_months = random.randint(1, min(6, duration))
        else:
            active_months = duration
 
        if income < 30000:
            late_extra = 15
        elif income < 50000:
            late_extra = 8
        else:
            late_extra = 0
 
        base_weights = {
            'aktif':     {'odendi': max(1, 80 - late_extra), 'gecikmeli': 15 + late_extra // 2, 'odenmedi': 5 + late_extra // 2},
            'beklemede': {'odendi': max(1, 50 - late_extra), 'gecikmeli': 30 + late_extra // 2, 'odenmedi': 20 + late_extra // 2},
            'terk':      {'odendi': 30, 'gecikmeli': 20, 'odenmedi': 50},
        }
        weights = base_weights.get(
            sub['subscription_status'],
            {'odendi': 70, 'gecikmeli': 20, 'odenmedi': 10}
        )
 
        for month in range(active_months):
            due_date = start_date + relativedelta(months=month)
 
            if due_date > today:
                break
 
            # Mevsimsel etki: Ocak ve Temmuz'da gecikme biraz artar
            seasonal_boost = 5 if due_date.month in (1, 7) else 0
            holiday_boost = 10 if is_near_holiday(due_date) else 0
            total_boost = seasonal_boost + holiday_boost
 
            w = {
                'odendi':    max(1, weights['odendi'] - total_boost),
                'gecikmeli': weights['gecikmeli'] + total_boost // 2,
                'odenmedi':  weights['odenmedi'] + total_boost // 2,
            }
 
            outcome = random.choices(
                population=list(w.keys()),
                weights=list(w.values()),
                k=1
            )[0]
 
            if outcome == 'odendi':
                days_late = random.randint(-5, 3)
                payment_date = due_date + timedelta(days=days_late)
                amount_paid = amount_due
                payment_status = 'odendi'
 
            elif outcome == 'gecikmeli':
                days_late = _gecikme_gun_uret(0.5)
                payment_date = due_date + timedelta(days=days_late)
                if random.random() < 0.30:
                    amount_paid = round(amount_due * random.uniform(0.5, 0.95), 2)
                    payment_status = 'kismi'
                else:
                    amount_paid = amount_due
                    payment_status = 'gecikmeli'
 
            else:  # odenmedi
                days_late = None
                payment_date = None
                amount_paid = 0
                payment_status = 'odenmedi'
 
            payments.append({
                'payment_id': f'PAY{payment_counter:06d}',
                'subscription_id': sub['subscription_id'],
                'member_id': sub['member_id'],
                'due_date': due_date,
                'payment_date': payment_date,
                'amount_due': amount_due,
                'amount_paid': amount_paid,
                'days_late': days_late,
                'payment_status': payment_status
            })
            payment_counter += 1
 
    logger.info(f'{len(payments)} ödeme kaydı üretildi.')
    return payments

def generate_lottery(subscriptions, plans):
    """
    Abonelik verilerindeki kura bilgisinden staging.lottery kayıtları üretir.
    Her kura kazanan abonelik için 1 kayıt, kazanmayan için 0-2 arası 
    katılım kaydı oluşturur.
    """
    lottery = []
    lottery_counter = 1
    plan_lookup = {p['plan_id']: p for p in plans}

    for sub in subscriptions:
        plan = plan_lookup[sub['plan_id']]

        if sub['kura_won']:
            # Kazanan: tek kayıt, is_winner=True
            lottery_date = sub['kura_date']
            lottery.append({
                'lottery_id':      f'L{lottery_counter:06d}',
                'member_id':       sub['member_id'],
                'plan_id':         sub['plan_id'],
                'subscription_id': sub['subscription_id'],
                'lottery_date':    lottery_date,
                'lottery_round':   random.randint(1, 8),
                'is_winner':       True
            })
            lottery_counter += 1
        else:
            # Kazanmayan: 0-2 arası katılım
            num_entries = random.randint(0, 2)
            for _ in range(num_entries):
                offset_months = random.randint(3, min(18, plan['duration_months']))
                lottery_date = sub['start_date'] + relativedelta(months=offset_months)
                lottery.append({
                    'lottery_id':      f'L{lottery_counter:06d}',
                    'member_id':       sub['member_id'],
                    'plan_id':         sub['plan_id'],
                    'subscription_id': sub['subscription_id'],
                    'lottery_date':    lottery_date,
                    'lottery_round':   random.randint(1, 8),
                    'is_winner':       False
                })
                lottery_counter += 1

    logger.info(f'{len(lottery)} kura kaydı üretildi.')
    return lottery
def inject_dirty_data(members, payments, subscriptions):
    """
    Gerçek hayat veri kirliliklerini simüle eder.
    """
    # %2 üyede tc_hash → Null
    null_tc_count = int(len(members) * 0.08)
    for m in random.sample(members, null_tc_count):
        m['tc_hash'] = None

    # %1 üyeyi duplike eder
    dupe_count = int(len(members) * 0.03)
    dupes = random.sample(members, dupe_count)
    members.extend(dupes)

    # %0.5 ödemede geçersiz tutar
    invalid_amount_count = int(len(payments) * 0.005)
    for p in random.sample(payments, invalid_amount_count):
        p['amount_paid'] = round(random.uniform(-9999, -1), 2)

    # %0.3 ödemede tutarsız tarih
    inconsistent_count = int(len(payments) * 0.003)
    for p in random.sample(payments, inconsistent_count):
        if p['payment_date'] is not None and p['days_late'] is not None:
            p['payment_date'] = p['due_date'] - timedelta(days=random.randint(1, 10))
            p['days_late'] = random.randint(5, 30)

    # %0.2 abonelikte tutarsız tarih
    sub_inconsistent_count = int(len(subscriptions) * 0.002)
    for s in random.sample(subscriptions, sub_inconsistent_count):
        s['expected_end_date'] = s['start_date'] - timedelta(days=random.randint(1, 30))

    return members, payments, subscriptions

def save_to_staging(conn, members, plans, subscriptions, payments, lottery):
    cur = conn.cursor()
    try:
        # ✅ Her çalıştırmada tabloları temizle
        logger.info('Staging tabloları temizleniyor...')
        cur.execute("""
            TRUNCATE TABLE 
                staging.lottery,
                staging.payments,
                staging.subscriptions,
                staging.members,
                staging.plans
            RESTART IDENTITY CASCADE;
        """)
        logger.info('Tablolar temizlendi.')
    except Exception as e:
        logger.error(f'Tablolar temizlenirken hata oluştu: {e}')
        raise

    try:
        logger.info('staging.members yazılıyor...')
        execute_values(cur,
            """
            INSERT INTO staging.members
                (member_id, full_name, tc_hash, city, district,
                 birth_date, income, signup_date, member_status, phone, email)
            VALUES %s
            """,
            [(m['member_id'], m['full_name'], m['tc_hash'], m['city'], m['district'],
              m['birth_date'], m['income'], m['signup_date'], m['member_status'],
              m['phone'], m['email']) for m in members]
        )
        logger.info(f'  -> {len(members)} satır eklendi.')

        logger.info('staging.plans yazılıyor...')
        execute_values(cur,
            """
            INSERT INTO staging.plans
                (plan_id, plan_name, plan_type, duration_months,
                 target_amount, monthly_installment)
            VALUES %s
            """,
            [(p['plan_id'], p['plan_name'], p['plan_type'], p['duration_months'],
              p['target_amount'], p['monthly_installment']) for p in plans]
        )
        logger.info(f'  -> {len(plans)} satır eklendi.')

        logger.info('staging.subscriptions yazılıyor...')
        execute_values(cur,
            """
            INSERT INTO staging.subscriptions
                (subscription_id, member_id, plan_id, start_date,
                 expected_end_date, kura_date, kura_won, subscription_status)
            VALUES %s
            """,
            [(s['subscription_id'], s['member_id'], s['plan_id'], s['start_date'],
              s['expected_end_date'], s['kura_date'], s['kura_won'], s['subscription_status'])
             for s in subscriptions]
        )
        logger.info(f'  -> {len(subscriptions)} satır eklendi.')

        logger.info('staging.payments yazılıyor...')
        execute_values(cur,
            """
            INSERT INTO staging.payments
                (payment_id, subscription_id, member_id, due_date,
                 payment_date, amount_due, amount_paid, days_late, payment_status)
            VALUES %s
            """,
            [(p['payment_id'], p['subscription_id'], p['member_id'], p['due_date'],
              p['payment_date'], p['amount_due'], p['amount_paid'],
              p['days_late'], p['payment_status']) for p in payments]
        )
        logger.info(f'  -> {len(payments)} satır eklendi.')

        logger.info('staging.lottery yazılıyor...')
        execute_values(cur,
            """
            INSERT INTO staging.lottery
                (lottery_id, member_id, plan_id, subscription_id,
                 lottery_date, lottery_round, is_winner)
            VALUES %s
            """,
            [(l['lottery_id'], l['member_id'], l['plan_id'], l['subscription_id'],
              l['lottery_date'], l['lottery_round'], l['is_winner']) for l in lottery]
        )
        logger.info(f'  -> {len(lottery)} satır eklendi.')

        conn.commit()
        logger.info('Tüm staging verileri başarıyla kaydedildi.')

    except Exception as e:
        conn.rollback()
        logger.error(f'Staging yazımında hata: {e}')
        raise
    finally:
        cur.close()


if __name__ == '__main__':
    logger.info('=== FuzulEv Veri Üretimi Başlıyor ===')

    num_members = config.get('data_generation', {}).get('num_members', 15000)

    members       = generate_members(num_members)
    plans         = generate_plans()
    subscriptions = generate_subscriptions(members, plans)
    payments      = generate_payments(subscriptions, plans)
    lottery       = generate_lottery(subscriptions, plans)

    # Kirli veri enjeksiyonu
    members, payments, subscriptions = inject_dirty_data(members, payments, subscriptions)
    conn = get_db_connection()
    try:
        save_to_staging(conn, members, plans, subscriptions, payments, lottery)
    finally:
        conn.close()
        logger.info('Veritabanı bağlantısı kapatıldı.')

    logger.info('=== Pipeline Tamamlandı ===')


 








