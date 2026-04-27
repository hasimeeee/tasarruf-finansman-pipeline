"""
Tasarruf Finansman - Transform Fonksiyonları
Staging verisini star schema için hazırlar.
Her fonksiyon ham veri alır, temizlenmiş/dönüştürülmüş veri döndürür.
"""

from datetime import date


# LOOKUP TABLOLARI

AGE_GROUPS = [
    (18, 25, "18-25"),
    (26, 35, "26-35"),
    (36, 45, "36-45"),
    (46, 55, "46-55"),
    (56, 200, "56+"),
]

INCOME_BRACKETS = [
    (0,      20000,   "Dusuk"),
    (20001,  50000,   "Orta-Alt"),
    (50001,  100000,  "Orta"),
    (100001, 200000,  "Orta-Ust"),
    (200001, 9999999, "Yuksek"),
]

RAMADAN_RANGES = [
    (date(2022, 4, 2),  date(2022, 5, 1)),
    (date(2023, 3, 23), date(2023, 4, 20)),
    (date(2024, 3, 11), date(2024, 4, 9)),
    (date(2025, 3, 1),  date(2025, 3, 29)),
    (date(2026, 2, 18), date(2026, 3, 19)),
]

FIXED_HOLIDAYS = {
    (1, 1), (4, 23), (5, 1), (5, 19), (7, 15), (8, 30), (10, 29)
}

DAY_NAMES_TR = {
    0: "Pazartesi", 1: "Sali", 2: "Carsamba",
    3: "Persembe",  4: "Cuma", 5: "Cumartesi", 6: "Pazar"
}


# YARDIMCI FONKSİYONLAR

def get_age_group(birth_year: int) -> str:
    """Doğum yılından yaş grubunu döndürür."""
    age = date.today().year - (birth_year or 1980)
    for lo, hi, label in AGE_GROUPS:
        if lo <= age <= hi:
            return label
    return "Bilinmiyor"


def get_income_bracket(income: float) -> str:
    """Gelir miktarından gelir dilimini döndürür."""
    income = float(income or 0)
    for lo, hi, label in INCOME_BRACKETS:
        if lo <= income <= hi:
            return label
    return "Bilinmiyor"


def is_ramadan(d: date) -> bool:
    """Verilen tarihin ramazan ayına denk gelip gelmediğini kontrol eder."""
    return any(start <= d <= end for start, end in RAMADAN_RANGES)


def is_holiday(d: date) -> bool:
    """Verilen tarihin resmi tatil olup olmadığını kontrol eder."""
    return (d.month, d.day) in FIXED_HOLIDAYS


def get_day_name(d: date) -> str:
    """Tarihin Türkçe gün adını döndürür."""
    return DAY_NAMES_TR[d.weekday()]


def get_quarter(month: int) -> int:
    """Aydan çeyreği döndürür."""
    return (month - 1) // 3 + 1


def calc_days_late(due_date, paid_date) -> int:
    """
    Gecikme gününü hesaplar.
    Ödenmemişse 0 döner.
    """
    if paid_date and due_date:
        return max(0, (paid_date - due_date).days)
    return 0


def derive_payment_status(paid_date, days_late: int) -> str:
    """
    Ödeme durumunu türetir.
    paid_date yoksa → odenmedi
    days_late > 0   → gecikmeli
    aksi halde      → zamaninda
    """
    if paid_date is None:
        return "odenmedi"
    elif days_late > 0:
        return "gecikmeli"
    else:
        return "zamaninda"

# TRANSFORM FONKSİYONLARI

def transform_dim_date_record(d: date) -> tuple:
    """
    Tek bir tarihi dim_date satırına dönüştürür.
    Döndürür: (date_key, full_date, day, month, quarter, year,
               day_of_week, is_weekend, is_holiday, is_ramadan)
    """
    return (
        int(d.strftime("%Y%m%d")),
        d,
        d.day,
        d.month,
        get_quarter(d.month),
        d.year,
        get_day_name(d),
        d.weekday() >= 5,
        is_holiday(d),
        is_ramadan(d),
    )


def transform_dim_plan_record(row: tuple) -> tuple:
    """
    staging_plans satırını dim_plan satırına dönüştürür.
    Giriş: (plan_id, plan_name, plan_type, duration_months, target_amount)
    Döndürür: (plan_id, plan_name, plan_type, duration_months,
               target_amount, monthly_installment)
    """
    plan_id, plan_name, plan_type, duration_months, target_amount = row
    monthly = round(float(target_amount) / int(duration_months), 2) if duration_months else 0
    return (plan_id, plan_name, plan_type, duration_months, target_amount, monthly)


def transform_dim_member_record(row: tuple) -> tuple:
    """
    staging_members satırını dim_member satırına dönüştürür.
    Giriş: (member_id, full_name, tc_hash, city, district,
            birth_year, income, signup_date, status)
    Döndürür: (member_id, full_name, tc_hash, city, district,
               age_group, income_bracket, signup_date,
               member_status, churn_date, valid_from, valid_to, is_current)
    """
    member_id, full_name, tc_hash, city, district, \
    birth_year, income, signup_date, status = row

    today     = date.today()
    ag        = get_age_group(birth_year or 1980)
    ib        = get_income_bracket(income or 0)
    churn     = today if status == "terk" else None
    valid_from = signup_date or today

    return (
        member_id, full_name, tc_hash, city, district,
        ag, ib, signup_date, status, churn,
        valid_from, None, True
    )


def transform_fact_payment_record(row: tuple) -> tuple:
    """
    staging_payments + JOIN sonucunu fact_payments satırına dönüştürür.
    Giriş: (payment_id, member_key, plan_key, date_key,
            installment_no, due_amount, paid_amount, due_date, paid_date)
    Döndürür: (payment_id, member_key, plan_key, date_key,
               installment_no, due_amount, paid_amount,
               days_late, payment_status)
    """
    payment_id, member_key, plan_key, date_key, \
    installment_no, due_amount, paid_amount, due_date, paid_date = row

    days_late = calc_days_late(due_date, paid_date)
    status    = derive_payment_status(paid_date, days_late)

    return (
        payment_id, member_key, plan_key, date_key,
        installment_no, due_amount, paid_amount,
        days_late, status
    )


def transform_fact_lottery_record(row: tuple) -> tuple:
    """
    staging_lottery + JOIN sonucunu fact_lottery satırına dönüştürür.
    cumulative_paid_ratio ilerleyen haftalarda hesaplanacak, şimdilik NULL.
    Giriş: (lottery_id, member_key, plan_key, date_key, lottery_round, is_winner)
    Döndürür: (lottery_id, member_key, plan_key, date_key,
               lottery_round, is_winner, cumulative_paid_ratio)
    """
    lottery_id, member_key, plan_key, date_key, lottery_round, is_winner = row
    return (lottery_id, member_key, plan_key, date_key, lottery_round, is_winner, None)