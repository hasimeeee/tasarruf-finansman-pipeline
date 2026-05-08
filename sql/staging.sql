CREATE SCHEMA IF NOT EXISTS staging;
-- ==========================================
-- 1. STAGING TABLOLARI
-- ==========================================

CREATE TABLE IF NOT EXISTS staging.members (
    id              SERIAL PRIMARY KEY,
    member_id       VARCHAR(20) UNIQUE,
    full_name       VARCHAR(100),
    tc_hash         VARCHAR(64),
    city            VARCHAR(50),
    district        VARCHAR(50),
    birth_date      DATE,
    income          NUMERIC(12, 2),
    signup_date     DATE,
    member_status   VARCHAR(20),
    phone           VARCHAR(30),
    email           VARCHAR(100),
    branch_sk       INTEGER,
    loaded_at       TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.plans (
    id                  SERIAL PRIMARY KEY,
    plan_id             VARCHAR(20),
    plan_name           VARCHAR(100),
    plan_type           VARCHAR(20),
    duration_months     INTEGER,
    target_amount       NUMERIC(15, 2),
    monthly_installment NUMERIC(12, 2),
    loaded_at           TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.payments (
    id              SERIAL PRIMARY KEY,
    payment_id      VARCHAR(20) UNIQUE,
    member_id       VARCHAR(20),
    plan_id         VARCHAR(20),
    installment_no  INTEGER,
    due_date        DATE,
    paid_date       DATE,
    due_amount      NUMERIC(12, 2),
    paid_amount     NUMERIC(12, 2),
    payment_status  VARCHAR(20),
    loaded_at       TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.lottery (
    id              SERIAL PRIMARY KEY,
    lottery_id      VARCHAR(20) UNIQUE,
    member_id       VARCHAR(20),
    plan_id         VARCHAR(20),
    lottery_date    DATE,
    lottery_round   INTEGER,
    is_winner       BOOLEAN DEFAULT FALSE,
    loaded_at       TIMESTAMP DEFAULT NOW()
);

-- FIX: branch_sk SERIAL olarak değiştirildi.
-- Önceki versiyonda INTEGER (sekansız) idi; dim_branch.branch_sk SERIAL ile
-- manuel değer girildiğinde sekans counter çakışması riski vardı.
-- Mevcut DB'de değiştirmek için:
--   ALTER TABLE staging.branches ALTER COLUMN branch_sk SET DEFAULT nextval('dim_branch_branch_sk_seq');
CREATE TABLE IF NOT EXISTS staging.branches (
    id          SERIAL PRIMARY KEY,
    branch_sk   INTEGER,                    -- dim_branch.branch_sk'ya referans (manuel veya ETL atar)
    branch_id   VARCHAR(20) UNIQUE,
    branch_name VARCHAR(100),
    city        VARCHAR(50),
    region      VARCHAR(50),
    open_date   DATE,
    loaded_at   TIMESTAMP DEFAULT NOW()
);