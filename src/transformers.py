"""
Tasarruf Finansman - Transform Fonksiyonları
Staging verisini star schema için hazırlar.
Her fonksiyon ham veri alır, temizlenmiş/dönüştürülmüş veri döndürür.
 
Değişiklikler:
  - transform_fact_lottery_record kaldırıldı (dead code)
  - get_age_group artık birth_year (int) veya birth_date (date) kabul ediyor
  - transform_dim_member_record güncellenmiş sütun sırasıyla uyumlu
  - FIX: get_age_group bilinmeyen tip için sessiz fallback yerine ValueError fırlatıyor

PLACEHOLDER NOTLARI:
  - member_segment: DDL'de VARCHAR(30) olarak tanımlı.
    Hafta 7'de K-Means kümeleme ile hesaplanacak (bkz. README — "Hafta 7 — K-Means ile member_segment").
    Faz 1'de intentionally NULL bırakılıyor; transform_dim_member_record bu sütunu doldurmaz.
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
    # FIX: 2026 tarihleri tahminidir — resmi açıklama çıkınca güncellenmeli
    # Diyanet İşleri Başkanlığı: https://www.diyanet.gov.tr
    (date(2026, 2, 18), date(2026, 3, 19)),
]
 
FIXED_HOLIDAYS = {
    (1, 1), (4, 23), (5, 1), (5, 19), (7, 15), (8, 30), (10, 29)
}
 
DAY_NAMES_TR = {
    0: "Pazartesi", 1: "Sali", 2: "Carsamba",
    3: "Persembe",  4: "Cuma", 5: "Cumartesi", 6: "Pazar"
}
 
MONTH_NAMES_TR = {
    1: "Ocak",    2: "Subat",   3: "Mart",
    4: "Nisan",   5: "Mayis",   6: "Haziran",
    7: "Temmuz",  8: "Agustos", 9: "Eylul",
    10: "Ekim",   11: "Kasim",  12: "Aralik"
}
 
 
# ==========================================
# YARDIMCI FONKSİYONLAR
# ==========================================
 
def get_age_group(birth_value) -> str:
    """
    Doğum yılı (int) veya doğum tarihi (date) alır, yaş grubunu döndürür.

    FIX: Önceki versiyonda bilinmeyen tip için age = today.year - 1980 sabit
    fallback dönüyordu — kirli staging verisinde sessizce yanlış segment üretiliyordu.
    Artık None veya beklenmeyen tip için ValueError fırlatılıyor; ETL pipeline
    bu hatayı yakalayıp satırı rows_rejected'e saymalı.
    """
    today = date.today()

    if birth_value is None:
        raise ValueError("get_age_group: birth_value None olamaz")

    if isinstance(birth_value, date):
        age = today.year - birth_value.year - (
            1 if (today.month, today.day) < (birth_value.month, birth_value.day) else 0
        )
    elif isinstance(birth_value, int):
        age = today.year - birth_value
    else:
        raise ValueError(
            f"get_age_group: beklenmeyen tip {type(birth_value).__name__!r} "
            f"(beklenen: date veya int)"
        )

    for lo, hi, label in AGE_GROUPS:
        if lo <= age <= hi:
            return label

    # Yaş aralık dışıysa (18 altı veya 200 üstü) "Bilinmiyor" döndür — bu normal bir iş durumu
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
    """Gecikme gününü hesaplar. Ödenmemişse 0 döner."""
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
        get_day_name(d),
        d.weekday() >= 5,
        is_holiday(d),
        is_ramadan(d),
    )
 
 
def transform_dim_plan_record(row: tuple) -> tuple:
    """
    staging.plans satırını dim_plan satırına dönüştürür.
    Giriş: (plan_id, plan_name, plan_type, duration_months, target_amount)
    """
    plan_id, plan_name, plan_type, duration_months, target_amount = row
    monthly = round(float(target_amount) / int(duration_months), 2) if duration_months else 0
    return (plan_id, plan_name, plan_type, duration_months, target_amount, monthly)
 
 
def transform_dim_member_record(row: tuple, valid_from=None):
    """
    SCD2 transformer - schema resilient version.
    Giriş: staging.members satırı (en az 9 sütun beklenir).
    """
    if len(row) < 9:
        raise ValueError(f"Geçersiz staging şeması: {len(row)} sütun (en az 9 bekleniyor)")

    (member_id, full_name, tc_hash, city, district,
     birth_date, income, signup_date, member_status, branch_sk, *_) = row

    today = date.today()

    # FIX: get_age_group artık ValueError fırlatabilir — çağıran ETL bu hatayı yakalar
    ag    = get_age_group(birth_date)
    ib    = get_income_bracket(income or 0)
    churn = today if member_status == "terk" else None
    vf    = valid_from if valid_from is not None else (signup_date or today)

    return (
    member_id, full_name, tc_hash, city, district,
    ag, ib, signup_date, member_status,
    None,
    churn,
    vf, None, member_status != 'terk', branch_sk  # ← düzeltildi
)
 
 
def transform_fact_payment_record(row: tuple) -> tuple:
    """
    staging.payments + JOIN sonucunu fact_payments satırına dönüştürür.
    Giriş: (payment_id, member_key, plan_key, date_key,
            installment_no, due_amount, paid_amount, due_date, paid_date)
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