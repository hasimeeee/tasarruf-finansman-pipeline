-- ==========================================
-- FuzulEv – Tasarruf Finansman Pipeline
-- DDL: Staging + Star Schema
-- ==========================================

-- ==========================================
-- 0. SCHEMA OLUŞTURMA
-- ==========================================

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dwh;


-- ==========================================
-- 1. STAGING TABLOLARI
-- ==========================================

-- Staging: Üyeler
CREATE TABLE IF NOT EXISTS staging.members (
    id              SERIAL PRIMARY KEY,
    member_id       VARCHAR(20),
    full_name       VARCHAR(100),
    tc_hash         VARCHAR(64),        -- NULL olabilir (kirli veri senaryosu)
    city            VARCHAR(50),
    district        VARCHAR(50),
    birth_date      DATE,
    income          NUMERIC(12, 2),
    signup_date     DATE,
    member_status   VARCHAR(20),        -- aktif / gecikmeli / pasif / terk
    phone           VARCHAR(30),
    email           VARCHAR(100),
    loaded_at       TIMESTAMP DEFAULT NOW()
);

-- Staging: Planlar
CREATE TABLE IF NOT EXISTS staging.plans (
    id                  SERIAL PRIMARY KEY,
    plan_id             VARCHAR(20),
    plan_name           VARCHAR(100),
    plan_type           VARCHAR(20),        -- konut / arsa / ticari / arac / isyeri
    duration_months     INTEGER,
    target_amount       NUMERIC(15, 2),
    monthly_installment NUMERIC(12, 2),
    loaded_at           TIMESTAMP DEFAULT NOW()
);

-- Staging: Abonelikler
CREATE TABLE IF NOT EXISTS staging.subscriptions (
    id                  SERIAL PRIMARY KEY,
    subscription_id     VARCHAR(15),
    member_id           VARCHAR(20),
    plan_id             VARCHAR(20),
    start_date          DATE,
    expected_end_date   DATE,
    kura_date           DATE,               -- NULL ise kura kazanılmadı
    kura_won            BOOLEAN DEFAULT FALSE,
    subscription_status VARCHAR(20),        -- aktif / beklemede / terk
    loaded_at           TIMESTAMP DEFAULT NOW()
);

-- Staging: Ödemeler
CREATE TABLE IF NOT EXISTS staging.payments (
    id                  SERIAL PRIMARY KEY,
    payment_id          VARCHAR(20),
    subscription_id     VARCHAR(15),
    member_id           VARCHAR(20),
    due_date            DATE,
    payment_date        DATE,               -- NULL ise ödenmedi
    amount_due          NUMERIC(12, 2),
    amount_paid         NUMERIC(12, 2),
    days_late           INTEGER,            -- NULL ise ödenmedi; negatif = erken ödeme
    payment_status      VARCHAR(20),        -- odendi / gecikmeli / kismi / odenmedi
    loaded_at           TIMESTAMP DEFAULT NOW()
);

-- Staging: Kura Çekilişleri
CREATE TABLE IF NOT EXISTS staging.lottery (
    id              SERIAL PRIMARY KEY,
    lottery_id      VARCHAR(20),
    member_id       VARCHAR(20),
    plan_id         VARCHAR(20),
    subscription_id VARCHAR(15),
    lottery_date    DATE,
    lottery_round   INTEGER,
    is_winner       BOOLEAN DEFAULT FALSE,
    loaded_at       TIMESTAMP DEFAULT NOW()
);


-- ==========================================
-- 2. STAR SCHEMA (DWH)
-- ==========================================

-- Dimension: Tarih
CREATE TABLE IF NOT EXISTS dwh.dim_date (
    date_key        INTEGER PRIMARY KEY,    -- YYYYMMDD formatında (örn: 20240315)
    full_date       DATE NOT NULL,
    day             SMALLINT,
    month           SMALLINT,
    quarter         SMALLINT,
    year            SMALLINT,
    day_of_week     SMALLINT,               -- 1=Pazartesi … 7=Pazar
    day_name        VARCHAR(15),
    month_name      VARCHAR(15),
    is_weekend      BOOLEAN DEFAULT FALSE,
    is_holiday      BOOLEAN DEFAULT FALSE,
    is_ramadan      BOOLEAN DEFAULT FALSE
);

-- Dimension: Üye (SCD Type 2)
CREATE TABLE IF NOT EXISTS dwh.dim_member (
    member_key      SERIAL PRIMARY KEY,     -- surrogate key
    member_id       VARCHAR(20) NOT NULL,   -- business key
    full_name       VARCHAR(100),
    tc_hash         VARCHAR(64),
    city            VARCHAR(50),
    district        VARCHAR(50),
    age_group       VARCHAR(20),            -- 18-25 / 26-35 / 36-45 / 46-55 / 56+
    income_bracket  VARCHAR(20),            -- dusuk / orta / yuksek / premium
    signup_date     DATE,
    member_status   VARCHAR(20),
    member_segment  VARCHAR(30),            -- Hafta 7'de K-Means ile doldurulacak
    churn_date      DATE,                   -- terk tarihi (varsa)
    -- SCD Type 2 kolonları
    valid_from      DATE NOT NULL DEFAULT CURRENT_DATE,
    valid_to        DATE,                   -- NULL = güncel kayıt
    is_current      BOOLEAN DEFAULT TRUE
);

-- Dimension: Plan
CREATE TABLE IF NOT EXISTS dwh.dim_plan (
    plan_key            SERIAL PRIMARY KEY,
    plan_id             VARCHAR(20) NOT NULL,
    plan_name           VARCHAR(100),
    plan_type           VARCHAR(20),        -- konut / arsa / ticari / arac / isyeri
    duration_months     INTEGER,
    target_amount       NUMERIC(15, 2),
    monthly_installment NUMERIC(12, 2)
);

-- Dimension: Şube
CREATE TABLE IF NOT EXISTS dwh.dim_branch (
    branch_key  SERIAL PRIMARY KEY,
    branch_id   VARCHAR(20) NOT NULL,
    branch_name VARCHAR(100),
    city        VARCHAR(50),
    region      VARCHAR(50),
    open_date   DATE
);

-- Fact: Ödemeler
CREATE TABLE IF NOT EXISTS dwh.fact_payments (
    payment_id      VARCHAR(20) PRIMARY KEY,
    member_key      INTEGER REFERENCES dwh.dim_member(member_key),
    plan_key        INTEGER REFERENCES dwh.dim_plan(plan_key),
    date_key        INTEGER REFERENCES dwh.dim_date(date_key),
    subscription_id VARCHAR(15),
    installment_no  INTEGER,
    due_amount      NUMERIC(12, 2),
    paid_amount     NUMERIC(12, 2),
    days_late       INTEGER,
    payment_status  VARCHAR(20)             -- odendi / gecikmeli / kismi / odenmedi
);

-- Fact: Kura Çekilişleri
CREATE TABLE IF NOT EXISTS dwh.fact_lottery (
    lottery_id              VARCHAR(20) PRIMARY KEY,
    member_key              INTEGER REFERENCES dwh.dim_member(member_key),
    plan_key                INTEGER REFERENCES dwh.dim_plan(plan_key),
    date_key                INTEGER REFERENCES dwh.dim_date(date_key),
    subscription_id         VARCHAR(15),
    lottery_round           INTEGER,
    is_winner               BOOLEAN DEFAULT FALSE,
    cumulative_paid_ratio   NUMERIC(5, 4)   -- ödenen / toplam taksit (0.00–1.00)
);


-- ==========================================
-- 3. PIPELINE LOG TABLOSU
-- ==========================================

CREATE TABLE IF NOT EXISTS staging.pipeline_runs (
    run_id          SERIAL PRIMARY KEY,
    run_start       TIMESTAMP NOT NULL,
    run_end         TIMESTAMP,
    status          VARCHAR(20),            -- running / success / failed
    rows_members    INTEGER,
    rows_plans      INTEGER,
    rows_subs       INTEGER,
    rows_payments   INTEGER,
    rows_lottery    INTEGER,
    error_message   TEXT,
    duration_sec    NUMERIC(10, 2)
);


-- ==========================================
-- 4. DOĞRULAMA SORGULARI
-- ==========================================

SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema IN ('staging', 'dwh')
ORDER BY table_schema, table_name;