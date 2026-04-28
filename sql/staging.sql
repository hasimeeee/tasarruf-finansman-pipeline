CREATE SCHEMA IF NOT EXISTS staging;
-- ==========================================
-- 1. STAGING TABLOLARI
-- ==========================================

-- Staging: Üyeler
CREATE TABLE IF NOT EXISTS staging.members (
    id              SERIAL PRIMARY KEY,
    member_id       VARCHAR(20) UNIQUE,
    full_name       VARCHAR(100),
    tc_hash         VARCHAR(64),            -- NULL olabilir (kirli veri senaryosu)
    city            VARCHAR(50),
    district        VARCHAR(50),
    birth_date      DATE,                   -- DATE tipinde (eskiden birth_year INT idi)
    income          NUMERIC(12, 2),
    signup_date     DATE,
    member_status   VARCHAR(20),            -- aktif / gecikmeli / pasif / terk
    phone           VARCHAR(30),            -- eklendi
    email           VARCHAR(100),           -- eklendi
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


-- Staging: Ödemeler
CREATE TABLE IF NOT EXISTS staging.payments (
    id              SERIAL PRIMARY KEY,
    payment_id      VARCHAR(20) UNIQUE,
    member_id       VARCHAR(20),
    plan_id         VARCHAR(20),
    installment_no  INTEGER,
    due_date        DATE,
    paid_date       DATE,                   -- NULL ise ödenmedi
    due_amount      NUMERIC(12, 2),
    paid_amount     NUMERIC(12, 2),
    payment_status  VARCHAR(20),            -- odendi / gecikmeli / kismi / odenmedi
    loaded_at       TIMESTAMP DEFAULT NOW()
);

-- Staging: Kura Çekilişleri
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

CREATE TABLE IF NOT EXISTS staging.branches (
    id          SERIAL PRIMARY KEY,
    branch_id   VARCHAR(20) UNIQUE,
    branch_name VARCHAR(100),
    city        VARCHAR(50),
    region      VARCHAR(50),
    open_date   DATE,
    loaded_at   TIMESTAMP DEFAULT NOW()
);