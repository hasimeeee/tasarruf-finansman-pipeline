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
        birth_date = fake.date_of_birth(minimum_age=18, maximum_age=65)
        birth_year = birth_date.year
        income     = round(random.choices(
            population=[20000, 35000, 55000, 85000, 150000],
            weights=[15, 30, 25, 20, 10], k=1
        )[0] * random.uniform(0.8, 1.2), 2)
        signup_date   = fake.date_between(start_date=date(2022,1,1), end_date=date(2026,4,1))
        member_status = random.choices(
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
            'birth_year':    birth_year,
            'income':        income,
            'signup_date':   signup_date,
            'status':        member_status,
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
# 3. PAYMENTS
# ==========================================
def generate_payments(members, plans):
    payments = []
    payment_counter = 1
    plan_lookup = {p['plan_id']: p for p in plans}
    today = date.today()

    for i, member in enumerate(members):
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
        status     = member['status']

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
                days_late    = None
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
# 4. LOTTERY
# ==========================================
def generate_lottery(members, plans):
    lottery = []
    lottery_counter = 1
    plan_lookup = {p['plan_id']: p for p in plans}

    for i, member in enumerate(members):
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
            kura_offset = random.randint(6, min(24, plan['duration_months']))
            lottery_date = start_date + relativedelta(months=kura_offset)
            lottery.append({
                'lottery_id':   f'L{lottery_counter:06d}',
                'member_id':    member['member_id'],
                'plan_id':      plan['plan_id'],
                'lottery_date': lottery_date,
                'lottery_round': random.randint(1, 8),
                'is_winner':    True,
            })
            lottery_counter += 1
        else:
            num_entries = random.randint(0, 2)
            for _ in range(num_entries):
                offset = random.randint(3, min(18, plan['duration_months']))
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
# 5. KİRLİ VERİ ENJEKSİYONU
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

    # %0.3 ödemede tutarsız tarih
    for p in random.sample(payments, int(len(payments) * 0.003)):
        if p['paid_date'] is not None:
            p['paid_date'] = p['due_date'] - timedelta(days=random.randint(1, 10))

    return members, payments

# ==========================================
# 6. STAGING'E YAZ
# ==========================================
def save_to_staging(conn, members, plans, payments, lottery):
    cur = conn.cursor()

    logger.info('Staging tabloları temizleniyor...')
    cur.execute("TRUNCATE staging_members, staging_plans, staging_payments, staging_lottery RESTART IDENTITY CASCADE")
    conn.commit()

    logger.info('staging_members yazılıyor...')
    execute_values(cur,
        """INSERT INTO staging_members
           (member_id, full_name, tc_hash, city, district, birth_year, income, signup_date, status)
           VALUES %s""",
        [(m['member_id'], m['full_name'], m['tc_hash'], m['city'], m['district'],
          m['birth_year'], m['income'], m['signup_date'], m['status']) for m in members]
    )
    logger.info(f'  -> {len(members)} satır eklendi.')

    logger.info('staging_plans yazılıyor...')
    execute_values(cur,
        """INSERT INTO staging_plans
           (plan_id, plan_name, plan_type, duration_months, target_amount, monthly_installment)
           VALUES %s""",
        [(p['plan_id'], p['plan_name'], p['plan_type'], p['duration_months'],
          p['target_amount'], p['monthly_installment']) for p in plans]
    )
    logger.info(f'  -> {len(plans)} satır eklendi.')

    logger.info('staging_payments yazılıyor...')
    execute_values(cur,
        """INSERT INTO staging_payments
           (payment_id, member_id, plan_id, installment_no, due_date, paid_date,
            due_amount, paid_amount, payment_status)
           VALUES %s""",
        [(p['payment_id'], p['member_id'], p['plan_id'], p['installment_no'],
          p['due_date'], p['paid_date'], p['due_amount'], p['paid_amount'],
          p['payment_status']) for p in payments]
    )
    logger.info(f'  -> {len(payments)} satır eklendi.')

    logger.info('staging_lottery yazılıyor...')
    execute_values(cur,
        """INSERT INTO staging_lottery
           (lottery_id, member_id, plan_id, lottery_date, lottery_round, is_winner)
           VALUES %s""",
        [(l['lottery_id'], l['member_id'], l['plan_id'], l['lottery_date'],
          l['lottery_round'], l['is_winner']) for l in lottery]
    )
    logger.info(f'  -> {len(lottery)} satır eklendi.')

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
    payments = generate_payments(members, plans)
    lottery  = generate_lottery(members, plans)

    members, payments = inject_dirty_data(members, payments)

    conn = get_db_connection()
    try:
        save_to_staging(conn, members, plans, payments, lottery)
    finally:
        conn.close()
        logger.info('Veritabanı bağlantısı kapatıldı.')

    logger.info('=== Veri Üretimi Tamamlandı ===')
 








