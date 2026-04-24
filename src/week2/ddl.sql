--  STAR SCHEMA (DWH)

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


