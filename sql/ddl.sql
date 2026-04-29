-- ==========================================
-- STAR SCHEMA (DWH) — DWH Tabloları
-- ==========================================
-- Düzeltmeler:
--   - dwh.* schema'sı kaldırıldı → public schema kullanılıyor
--     (etl_pipeline.py schema belirtmeden yazar; ikisi artık uyumlu)
--   - dim_date: day_of_week VARCHAR(15) olarak güncellendi (transformer 'Pazartesi' gibi
--     Türkçe string döndürüyor; eski DDL'de SMALLINT vardı)
--   - dim_member: birth_year kaldırıldı (staging'de artık yok)
--   - dim_branch eklendi (generator & staging ile uyumlu hale getirilecek Hafta 3-4)
--   - fact_payments / fact_lottery: subscription_id kaldırıldı (pipeline kullanmıyor)
-- ==========================================

-- Pipeline run log tablosu
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          SERIAL PRIMARY KEY,
    run_at          TIMESTAMP DEFAULT NOW(),
    stage           VARCHAR(50),
    status          VARCHAR(20),
    rows_inserted   INTEGER,
    duration_sec    NUMERIC(8, 2),
    error_msg       TEXT
);

-- ==========================================
-- DİMENSİON TABLOLARI
-- ==========================================

-- Dimension: Tarih
CREATE TABLE IF NOT EXISTS dim_date (
    date_key        INTEGER PRIMARY KEY,    -- YYYYMMDD formatında (örn: 20240315)
    full_date       DATE NOT NULL,
    day             SMALLINT,
    month           SMALLINT,
    quarter         SMALLINT,
    year            SMALLINT,
    day_of_week     VARCHAR(15),            -- Türkçe gün adı: Pazartesi … Pazar
    day_name        VARCHAR(15),            -- Türkçe gün adı (day_of_week ile aynı)
    month_name      VARCHAR(15),            -- Türkçe ay adı: Ocak … Aralik
    is_weekend      BOOLEAN DEFAULT FALSE,
    is_holiday      BOOLEAN DEFAULT FALSE,
    is_ramadan      BOOLEAN DEFAULT FALSE
);

-- Dimension: Üye (SCD Type 2)
CREATE TABLE IF NOT EXISTS dim_member (
    member_key      SERIAL PRIMARY KEY,     -- surrogate key
    member_id       VARCHAR(20) NOT NULL,   -- business key
    full_name       VARCHAR(100),
    tc_hash         VARCHAR(64),
    city            VARCHAR(50),
    district        VARCHAR(50),
    age_group       VARCHAR(20),            -- 18-25 / 26-35 / 36-45 / 46-55 / 56+
    income_bracket  VARCHAR(20),            -- Dusuk / Orta-Alt / Orta / Orta-Ust / Yuksek
    signup_date     DATE,
    member_status   VARCHAR(20),            -- aktif / gecikmeli / pasif / terk
    member_segment  VARCHAR(30),            -- Hafta 7'de K-Means ile doldurulacak
    churn_date      DATE,                   -- terk tarihi (varsa)
    -- SCD Type 2 kolonları
    valid_from      DATE NOT NULL DEFAULT CURRENT_DATE,
    valid_to        DATE,                   -- NULL = güncel kayıt
    is_current      BOOLEAN DEFAULT TRUE
);

-- Dimension: Plan
CREATE TABLE IF NOT EXISTS dim_plan (
    plan_key            SERIAL PRIMARY KEY,
    plan_id             VARCHAR(20) NOT NULL UNIQUE,
    plan_name           VARCHAR(100),
    plan_type           VARCHAR(20),        -- konut / arsa / ticari / arac / isyeri
    duration_months     INTEGER,
    target_amount       NUMERIC(15, 2),
    monthly_installment NUMERIC(12, 2)
);

-- Dimension: Şube (SCD Type 2)
-- Hafta 3-4'te generator & staging ile birlikte doldurulacak
CREATE TABLE IF NOT EXISTS dim_branch (
    branch_key  SERIAL PRIMARY KEY,        -- surrogate key
    branch_id   VARCHAR(20) NOT NULL,      -- business key (UNIQUE kaldırıldı — SCD2'de aynı branch_id birden fazla satırda olabilir)
    branch_name VARCHAR(100),
    city        VARCHAR(50),
    region      VARCHAR(50),
    open_date   DATE,
    -- SCD Type 2 kolonları
    valid_from  DATE NOT NULL DEFAULT CURRENT_DATE,
    valid_to    DATE,                      -- NULL = güncel kayıt
    is_current  BOOLEAN DEFAULT TRUE
);

-- ==========================================
-- FACT TABLOLARI
-- ==========================================

-- Fact: Ödemeler
CREATE TABLE IF NOT EXISTS fact_payments (
    payment_id      VARCHAR(20) PRIMARY KEY,
    member_key      INTEGER REFERENCES dim_member(member_key),
    plan_key        INTEGER REFERENCES dim_plan(plan_key),
    date_key        INTEGER REFERENCES dim_date(date_key),
    installment_no  INTEGER,
    due_amount      NUMERIC(12, 2),
    paid_amount     NUMERIC(12, 2),
    days_late       INTEGER,
    payment_status  VARCHAR(20)             -- odendi / gecikmeli / kismi / odenmedi / zamaninda
);

-- Fact: Kura Çekilişleri
CREATE TABLE IF NOT EXISTS fact_lottery (
    lottery_id              VARCHAR(20) PRIMARY KEY,
    member_key              INTEGER REFERENCES dim_member(member_key),
    plan_key                INTEGER REFERENCES dim_plan(plan_key),
    date_key                INTEGER REFERENCES dim_date(date_key),
    lottery_round           INTEGER,
    is_winner               BOOLEAN DEFAULT FALSE,
    cumulative_paid_ratio   NUMERIC(5, 4)   -- ödenen / toplam taksit (0.00–1.00)
);