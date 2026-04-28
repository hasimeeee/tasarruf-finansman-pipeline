import random
import hashlib
import psycopg2
from psycopg2.extras import execute_values
from faker import Faker
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
import yaml
import logging

fake = Faker('tr_TR')
random.seed(42)
Faker.seed(42)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Config yükle
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Şehir dağılımı
CITIES = {
    'Istanbul': 30, 'Ankara': 15, 'Konya': 10,
    'Izmir': 8, 'Bursa': 6, 'Gaziantep': 5,
    'Kayseri': 4, 'Diyarbakir': 3, 'Antalya': 3,
    'Adana': 3, 'Samsun': 2, 'Trabzon': 2,
    'Eskisehir': 2, 'Mersin': 2, 'Diger': 5
}

# Şube bölge eşlemesi
CITY_REGIONS = {
    'Istanbul': 'Marmara', 'Bursa': 'Marmara', 'Eskisehir': 'Marmara',
    'Ankara': 'Ic Anadolu', 'Konya': 'Ic Anadolu', 'Kayseri': 'Ic Anadolu',
    'Izmir': 'Ege', 'Antalya': 'Akdeniz', 'Mersin': 'Akdeniz', 'Adana': 'Akdeniz',
    'Samsun': 'Karadeniz', 'Trabzon': 'Karadeniz',
    'Gaziantep': 'Guneydogu Anadolu', 'Diyarbakir': 'Guneydogu Anadolu',
    'Diger': 'Diger',
}

MEMBER_STATUSES = ['aktif', 'gecikmeli', 'pasif', 'terk']

FIXED_HOLIDAYS = [
    (1, 1), (4, 23), (5, 1), (5, 19),
    (7, 15), (8, 30), (10, 29),
]

RELIGIOUS_HOLIDAYS = [
    (2022, 5, 2, 3), (2023, 4, 21, 3), (2024, 4, 10, 3),
    (2025, 3, 30, 3), (2026, 3, 20, 3),
    (2022, 7, 9, 4), (2023, 6, 28, 4), (2024, 6, 16, 4),
    (2025, 6, 6, 4),  (2026, 5, 27, 4),
]

def build_holiday_set():
    holidays = set()
    for year in range(2022, 2027):
        for month, day in FIXED_HOLIDAYS:
            holidays.add(date(year, month, day))
    for year, month, day, duration in RELIGIOUS_HOLIDAYS:
        for d in range(duration):
            holidays.add(date(year, month, day) + timedelta(days=d))
    return holidays

HOLIDAYS = build_holiday_set()

def is_near_holiday(d, window=7):
    for h in HOLIDAYS:
        if 0 <= (h - d).days <= window:
            return True
    return False

def _gecikme_gun_uret(prob=0.5):
    if random.random() > prob:
        return 0
    return int(random.expovariate(1/5)) + 1

def get_db_connection():
    db = config["database"].copy()
    if "name" in db:
        db["dbname"] = db.pop("name")
    return psycopg2.connect(**db)


# ==========================================
# 1. MEMBERS
# ==========================================
def generate_members(num_members):
    members = []
    for i in range(num_members):
        tc_no   = str(random.randint(10000000000, 99999999999))
        tc_hash = hashlib.sha256(tc_no.encode()).hexdigest()
        city    = random.choices(
            population=list(CITIES.keys()),
            weights=list(CITIES.values()), k=1
        )[0]
        district   = fake.city()
        birth_date = fake.date_of_birth(minimum_age=18, maximum_age=65)  # DATE tipinde
        phone      = fake.phone_number()
        email      = fake.email()
        income     = round(random.choices(
            population=[20000, 35000, 55000, 85000, 150000],
            weights=[15, 30, 25, 20, 10], k=1
        )[0] * random.uniform(0.8, 1.2), 2)
        signup_date    = fake.date_between(start_date=date(2022, 1, 1), end_date=date(2026, 4, 1))
        # Düzeltme: sütun adı "status" değil "member_status" — staging.members ile uyumlu
        member_status  = random.choices(
            population=MEMBER_STATUSES,
            weights=[60, 20, 10, 10]
        )[0]
        full_name = fake.name()

        members.append({
            'member_id':     f'M{i+1:05d}',
            'full_name':     full_name,
            'tc_hash':       tc_hash,
            'city':          city,
            'district':      district,
            'birth_date':    birth_date,    # DATE — staging.members ile uyumlu
            'income':        income,
            'signup_date':   signup_date,
            'member_status': member_status, # sütun adı standardize edildi
            'phone':         phone,
            'email':         email,
        })

    logger.info(f'{num_members} üye üretildi.')
    return members


# ==========================================
# 2. PLANS
# ==========================================
def generate_plans():
    plan_configs = [
        {'plan_id': 'P001', 'plan_name': 'Konut Cekilisli 48 Ay',  'plan_type': 'konut',  'duration_months': 48,  'target_amount': 3000000},
        {'plan_id': 'P002', 'plan_name': 'Konut Bireysel 120 Ay',  'plan_type': 'konut',  'duration_months': 120, 'target_amount': 5000000},
        {'plan_id': 'P003', 'plan_name': 'Arac Cekilisli 60 Ay',   'plan_type': 'arac',   'duration_months': 60,  'target_amount': 1500000},
        {'plan_id': 'P004', 'plan_name': 'Isyeri Bireysel 48 Ay',  'plan_type': 'isyeri', 'duration_months': 48,  'target_amount': 8000000},
        {'plan_id': 'P005', 'plan_name': 'Konut Uzun Vade 240 Ay', 'plan_type': 'konut',  'duration_months': 240, 'target_amount': 10000000},
    ]
    plans = []
    for p in plan_configs:
        p['monthly_installment'] = round(p['target_amount'] / p['duration_months'], 2)
        plans.append(p)
    logger.info(f'{len(plans)} plan üretildi.')
    return plans


# ==========================================
# 3. BRANCHES (Hafta 3-4 için eklendi)
# ==========================================
def generate_branches():
    """
    Her şehir için 1 şube üretir.
    ddl.sql → dim_branch ve staging.branches ile uyumlu.
    """
    branches = []
    for i, (city, _) in enumerate(CITIES.items(), start=1):
        region = CITY_REGIONS.get(city, 'Diger')
        open_date = fake.date_between(start_date=date(2010, 1, 1), end_date=date(2021, 12, 31))
        branches.append({
            'branch_id':   f'B{i:03d}',
            'branch_name': f'{city} Subesi',
            'city':        city,
            'region':      region,
            'open_date':   open_date,
        })
    logger.info(f'{len(branches)} şube üretildi.')
    return branches


# ==========================================
# 4. PAYMENTS
# ==========================================
def generate_payments(members, plans):
    payments = []
    payment_counter = 1
    today = date.today()

    for member in members:
        income = member['income']
        if income >= 100_000:
            plan = random.choices(plans, weights=[10, 10, 40, 10, 30], k=1)[0]
        elif income >= 50_000:
            plan = random.choices(plans, weights=[20, 20, 30, 20, 10], k=1)[0]
        else:
            plan = random.choices(plans, weights=[30, 30, 10, 25, 5], k=1)[0]

        plan_id    = plan['plan_id']
        amount_due = plan['monthly_installment']
        start_date = member['signup_date']
        duration   = plan['duration_months']
        status     = member['member_status']   # düzeltildi: 'status' → 'member_status'

        active_months = random.randint(1, min(6, duration)) if status == 'terk' else duration

        late_extra = 15 if income < 30000 else (8 if income < 50000 else 0)
        base_weights = {
            'aktif':     {'odendi': max(1, 80 - late_extra), 'gecikmeli': 15 + late_extra // 2, 'odenmedi': 5 + late_extra // 2},
            'gecikmeli': {'odendi': max(1, 50 - late_extra), 'gecikmeli': 30 + late_extra // 2, 'odenmedi': 20 + late_extra // 2},
            'terk':      {'odendi': 30, 'gecikmeli': 20, 'odenmedi': 50},
        }
        weights = base_weights.get(status, {'odendi': 70, 'gecikmeli': 20, 'odenmedi': 10})

        for month in range(active_months):
            due_date = start_date + relativedelta(months=month)
            if due_date > today:
                break

            seasonal_boost = 5 if due_date.month in (1, 7) else 0
            holiday_boost  = 10 if is_near_holiday(due_date) else 0
            total_boost    = seasonal_boost + holiday_boost

            w = {
                'odendi':    max(1, weights['odendi'] - total_boost),
                'gecikmeli': weights['gecikmeli'] + total_boost // 2,
                'odenmedi':  weights['odenmedi'] + total_boost // 2,
            }
            outcome = random.choices(list(w.keys()), weights=list(w.values()), k=1)[0]

            if outcome == 'odendi':
                days_late    = random.randint(-5, 3)
                payment_date = due_date + timedelta(days=days_late)
                amount_paid  = amount_due
                pay_status   = 'odendi'
            elif outcome == 'gecikmeli':
                days_late    = _gecikme_gun_uret(0.5)
                payment_date = due_date + timedelta(days=days_late)
                if random.random() < 0.30:
                    amount_paid = round(amount_due * random.uniform(0.5, 0.95), 2)
                    pay_status  = 'kismi'
                else:
                    amount_paid = amount_due
                    pay_status  = 'gecikmeli'
            else:
                payment_date = None
                amount_paid  = 0
                pay_status   = 'odenmedi'

            payments.append({
                'payment_id':     f'PAY{payment_counter:06d}',
                'member_id':      member['member_id'],
                'plan_id':        plan_id,
                'installment_no': month + 1,
                'due_date':       due_date,
                'paid_date':      payment_date,
                'due_amount':     amount_due,
                'paid_amount':    amount_paid,
                'payment_status': pay_status,
            })
            payment_counter += 1

    logger.info(f'{len(payments)} ödeme kaydı üretildi.')
    return payments


# ==========================================
# 5. LOTTERY
# ==========================================
def generate_lottery(members, plans):
    lottery = []
    lottery_counter = 1

    for member in members:
        income = member['income']
        if income >= 100_000:
            plan = random.choices(plans, weights=[10, 10, 40, 10, 30], k=1)[0]
        elif income >= 50_000:
            plan = random.choices(plans, weights=[20, 20, 30, 20, 10], k=1)[0]
        else:
            plan = random.choices(plans, weights=[30, 30, 10, 25, 5], k=1)[0]

        start_date = member['signup_date']
        kura_won   = random.random() < 0.20

        if kura_won:
            kura_offset  = random.randint(6, min(24, plan['duration_months']))
            lottery_date = start_date + relativedelta(months=kura_offset)
            lottery.append({
                'lottery_id':    f'L{lottery_counter:06d}',
                'member_id':     member['member_id'],
                'plan_id':       plan['plan_id'],
                'lottery_date':  lottery_date,
                'lottery_round': random.randint(1, 8),
                'is_winner':     True,
            })
            lottery_counter += 1
        else:
            num_entries = random.randint(0, 2)
            for _ in range(num_entries):
                offset       = random.randint(3, min(18, plan['duration_months']))
                lottery_date = start_date + relativedelta(months=offset)
                lottery.append({
                    'lottery_id':    f'L{lottery_counter:06d}',
                    'member_id':     member['member_id'],
                    'plan_id':       plan['plan_id'],
                    'lottery_date':  lottery_date,
                    'lottery_round': random.randint(1, 8),
                    'is_winner':     False,
                })
                lottery_counter += 1

    logger.info(f'{len(lottery)} kura kaydı üretildi.')
    return lottery


# ==========================================
# 6. KİRLİ VERİ ENJEKSİYONU
# ==========================================
def inject_dirty_data(members, payments):
    # %8 üyede tc_hash → None
    for m in random.sample(members, int(len(members) * 0.08)):
        m['tc_hash'] = None

    # %3 üye duplike
    dupes = random.sample(members, int(len(members) * 0.03))
    members.extend(dupes)

    # %0.5 ödemede negatif tutar
    for p in random.sample(payments, int(len(payments) * 0.005)):
        p['paid_amount'] = round(random.uniform(-9999, -1), 2)

    # %0.3 ödemede tutarsız tarih (paid_date < due_date)
    for p in random.sample(payments, int(len(payments) * 0.003)):
        if p['paid_date'] is not None:
            p['paid_date'] = p['due_date'] - timedelta(days=random.randint(1, 10))

    return members, payments


# ==========================================
# 7. STAGING'E YAZ
# ==========================================
def save_to_staging(conn, members, plans, payments, lottery, branches):
    cur = conn.cursor()

    logger.info('Staging tabloları temizleniyor...')
    cur.execute("""
        TRUNCATE staging.members, staging.plans, staging.payments,
                 staging.lottery, staging.branches
        RESTART IDENTITY CASCADE
    """)
    conn.commit()

    # --- members ---
    logger.info('staging.members yazılıyor...')
    execute_values(cur,
        """INSERT INTO staging.members
           (member_id, full_name, tc_hash, city, district,
            birth_date, income, signup_date, member_status,
            phone, email)
           VALUES %s""",
        [(m['member_id'], m['full_name'], m['tc_hash'], m['city'], m['district'],
          m['birth_date'], m['income'], m['signup_date'], m['member_status'],
          m['phone'], m['email'])
         for m in members]
    )
    logger.info(f'  -> {len(members)} satır eklendi.')

    # --- plans ---
    logger.info('staging.plans yazılıyor...')
    execute_values(cur,
        """INSERT INTO staging.plans
           (plan_id, plan_name, plan_type, duration_months, target_amount, monthly_installment)
           VALUES %s""",
        [(p['plan_id'], p['plan_name'], p['plan_type'], p['duration_months'],
          p['target_amount'], p['monthly_installment']) for p in plans]
    )
    logger.info(f'  -> {len(plans)} satır eklendi.')

    # --- payments ---
    logger.info('staging.payments yazılıyor...')
    execute_values(cur,
        """INSERT INTO staging.payments
           (payment_id, member_id, plan_id, installment_no, due_date, paid_date,
            due_amount, paid_amount, payment_status)
           VALUES %s""",
        [(p['payment_id'], p['member_id'], p['plan_id'], p['installment_no'],
          p['due_date'], p['paid_date'], p['due_amount'], p['paid_amount'],
          p['payment_status'])
         for p in payments]
    )
    logger.info(f'  -> {len(payments)} satır eklendi.')

    # --- lottery ---
    logger.info('staging.lottery yazılıyor...')
    execute_values(cur,
        """INSERT INTO staging.lottery
           (lottery_id, member_id, plan_id, lottery_date, lottery_round, is_winner)
           VALUES %s""",
        [(l['lottery_id'], l['member_id'], l['plan_id'], l['lottery_date'],
          l['lottery_round'], l['is_winner']) for l in lottery]
    )
    logger.info(f'  -> {len(lottery)} satır eklendi.')

    # --- branches ---
    logger.info('staging.branches yazılıyor...')
    execute_values(cur,
        """INSERT INTO staging.branches
           (branch_id, branch_name, city, region, open_date)
           VALUES %s""",
        [(b['branch_id'], b['branch_name'], b['city'], b['region'], b['open_date'])
         for b in branches]
    )
    logger.info(f'  -> {len(branches)} satır eklendi.')

    conn.commit()
    cur.close()
    logger.info('Tüm staging verileri kaydedildi.')


# ==========================================
# ANA AKIŞ
# ==========================================
if __name__ == '__main__':
    logger.info('=== Veri Üretimi Başlıyor ===')

    num_members = config.get('data_generation', {}).get('num_members', 15000)

    members  = generate_members(num_members)
    plans    = generate_plans()
    branches = generate_branches()
    payments = generate_payments(members, plans)
    lottery  = generate_lottery(members, plans)

    members, payments = inject_dirty_data(members, payments)

    conn = get_db_connection()
    try:
        save_to_staging(conn, members, plans, payments, lottery, branches)
    finally:
        conn.close()
        logger.info('Veritabanı bağlantısı kapatıldı.')

    logger.info('=== Veri Üretimi Tamamlandı ===')