"""
Tasarruf Finansman - Transform Fonksiyonları
Staging verisini star schema için hazırlar.
Her fonksiyon ham veri alır, temizlenmiş/dönüştürülmüş veri döndürür.

Değişiklikler:
  - transform_fact_lottery_record kaldırıldı (dead code — etl_pipeline.py
    kendi inline hesaplama yapıyordu, bu fonksiyon None döndürüyordu)
  - get_age_group artık birth_year (int) veya birth_date (date) her ikisini
    de kabul ediyor — staging.members artık DATE tipinde tutuyor
  - transform_dim_member_record güncellenmiş sütun sırasıyla uyumlu
"""

from datetime import date


# ==========================================
# LOOKUP TABLOLARI
# ==========================================

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


# ==========================================
# YARDIMCI FONKSİYONLAR
# ==========================================

def get_age_group(birth_value) -> str:
    """
    Doğum yılı (int) veya doğum tarihi (date) alır, yaş grubunu döndürür.
    staging.members artık DATE tipinde saklıyor; her iki tip de destekleniyor.
    """
    today = date.today()
    if isinstance(birth_value, date):
        # Gerçek yaş hesabı — ay/gün farkını da hesaba katar
        age = today.year - birth_value.year - (
            1 if (today.month, today.day) < (birth_value.month, birth_value.day) else 0
        )
    elif isinstance(birth_value, int):
        age = today.year - birth_value
    else:
        age = today.year - 1980  # fallback

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


# ==========================================
# TRANSFORM FONKSİYONLARI
# ==========================================

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
        get_day_name(d),        # VARCHAR(15) — DDL ile uyumlu
        d.weekday() >= 5,
        is_holiday(d),
        is_ramadan(d),
    )


def transform_dim_plan_record(row: tuple) -> tuple:
    """
    staging.plans satırını dim_plan satırına dönüştürür.
    Giriş: (plan_id, plan_name, plan_type, duration_months, target_amount)
    Döndürür: (plan_id, plan_name, plan_type, duration_months,
               target_amount, monthly_installment)
    """
    plan_id, plan_name, plan_type, duration_months, target_amount = row
    monthly = round(float(target_amount) / int(duration_months), 2) if duration_months else 0
    return (plan_id, plan_name, plan_type, duration_months, target_amount, monthly)


def transform_dim_member_record(row: tuple) -> tuple:
    """
    staging.members satırını dim_member satırına dönüştürür.

    Giriş: (member_id, full_name, tc_hash, city, district,
            birth_date, income, signup_date, member_status)
    NOT: etl_pipeline.py sorgusu bu sırayla döndürüyor — değiştirme.

    Döndürür: (member_id, full_name, tc_hash, city, district,
               age_group, income_bracket, signup_date,
               member_status, churn_date, valid_from, valid_to, is_current)
    """
    (member_id, full_name, tc_hash, city, district,
     birth_date, income, signup_date, member_status) = row

    today      = date.today()
    ag         = get_age_group(birth_date)          # DATE veya int her ikisi de çalışır
    ib         = get_income_bracket(income or 0)
    churn      = today if member_status == "terk" else None
    valid_from = signup_date or today

    return (
        member_id, full_name, tc_hash, city, district,
        ag, ib, signup_date, member_status, churn,
        valid_from, None, True
    )


def transform_fact_payment_record(row: tuple) -> tuple:
    """
    staging.payments + JOIN sonucunu fact_payments satırına dönüştürür.
    Giriş: (payment_id, member_key, plan_key, date_key,
            installment_no, due_amount, paid_amount, due_date, paid_date)
    Döndürür: (payment_id, member_key, plan_key, date_key,
               installment_no, due_amount, paid_amount,
               days_late, payment_status)
    """
    (payment_id, member_key, plan_key, date_key,
     installment_no, due_amount, paid_amount, due_date, paid_date) = row

    days_late = calc_days_late(due_date, paid_date)
    status    = derive_payment_status(paid_date, days_late)

    return (
        payment_id, member_key, plan_key, date_key,
        installment_no, due_amount, paid_amount,
        days_late, status
    )
