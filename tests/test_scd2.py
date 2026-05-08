"""
test_scd2.py — Düzeltilmiş versiyon (Faz 1 bug fix)

Düzeltilen hatalar (bu versiyon):
  1. status = 'failed' → 'FAILED': DDL yorumu ve ETL pipeline büyük harf kullanıyor.
     Küçük harfle sorgu hiç eşleşmiyordu — sessiz false positive.
  2. sys.path: conftest.py ile merkezi olarak yönetiliyor (bu dosyadan kaldırıldı).
     Proje yapısı src/ altında değil, kök dizinde; conftest.py rootdir'i pythonpath'e ekler.
  3. psycopg2 IN %s: tuple tek elemanlı olsa da güvenli; mevcut kullanım doğru,
     yorum eklendi.
"""

import pytest
from datetime import date

from etl_pipeline import get_conn, load_dim_member_scd2, run_pipeline

TEST_MEMBER_IDS = ('TEST001', 'TEST002', 'TEST003', 'TEST004')
# NOT: psycopg2'de "WHERE x IN %s" tuple doğru çalışır (tek elemanlıda da).
# Tek eleman için tuple sonuna virgül gerekli: ('X',) — burada 4 eleman var, sorun yok.


def _cleanup(connection):
    """
    Test verilerini staging ve dim_member'dan siler.
    Başında rollback() çağrılır — başarısız test sonrası aborted transaction temizlenir.
    """
    connection.rollback()
    cur = connection.cursor()
    cur.execute("DELETE FROM dim_member WHERE member_id IN %s",       (TEST_MEMBER_IDS,))
    cur.execute("DELETE FROM staging.members WHERE member_id IN %s",  (TEST_MEMBER_IDS,))
    connection.commit()


@pytest.fixture
def conn():
    connection = get_conn()
    _cleanup(connection)
    yield connection
    _cleanup(connection)
    connection.close()


def _insert_member(cur, conn, member_id, full_name, tc_hash, city, district,
                   birth_date, income, signup_date, member_status, phone, email):
    cur.execute("""
        INSERT INTO staging.members
        (member_id, full_name, tc_hash, city, district,
         birth_date, income, signup_date, member_status,
         phone, email, branch_sk)
        VALUES
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL)
    """, (member_id, full_name, tc_hash, city, district,
          birth_date, income, signup_date, member_status, phone, email))
    conn.commit()


# ==========================================
# TEST 1 — Yeni üye dim_member'a eklenir
# ==========================================
def test_yeni_uye_eklenir(conn):
    cur = conn.cursor()
    _insert_member(cur, conn,
        'TEST001', 'Test Kisi', 'abc123hash', 'Istanbul', 'Kadikoy',
        '1990-01-01', 50000, '2024-01-01', 'aktif',
        '05001234567', 'test@test.com')

    load_dim_member_scd2(conn)

    cur.execute("""
        SELECT COUNT(*) FROM dim_member
        WHERE member_id = 'TEST001' AND is_current = TRUE
    """)
    assert cur.fetchone()[0] == 1, "Yeni üye dim_member'a eklenmedi!"


# ==========================================
# TEST 2 — Statü değişince eski kayıt kapanır
# ==========================================
def test_statu_degisince_eski_kayit_kapanir(conn):
    cur = conn.cursor()
    _insert_member(cur, conn,
        'TEST002', 'Test Kisi2', 'abc456hash', 'Ankara', 'Cankaya',
        '1985-05-15', 60000, '2023-01-01', 'aktif',
        '05009876543', 'test2@test.com')

    load_dim_member_scd2(conn)

    cur.execute("UPDATE staging.members SET member_status = 'gecikmeli' WHERE member_id = 'TEST002'")
    conn.commit()
    load_dim_member_scd2(conn)

    cur.execute("""
        SELECT COUNT(*) FROM dim_member
        WHERE member_id = 'TEST002'
          AND member_status = 'aktif'
          AND is_current = FALSE
    """)
    assert cur.fetchone()[0] == 1, "Eski kayıt kapatılmadı!"


# ==========================================
# TEST 3 — Statü değişince yeni kayıt açılır
# ==========================================
def test_statu_degisince_yeni_kayit_acilir(conn):
    cur = conn.cursor()
    _insert_member(cur, conn,
        'TEST003', 'Test Kisi3', 'abc789hash', 'Istanbul', 'Bakirkoy',
        '1995-03-20', 70000, '2024-01-01', 'aktif',
        '05001234567', 'test3@test.com')

    load_dim_member_scd2(conn)

    cur.execute("UPDATE staging.members SET member_status = 'gecikmeli' WHERE member_id = 'TEST003'")
    conn.commit()
    load_dim_member_scd2(conn)

    cur.execute("""
        SELECT COUNT(*) FROM dim_member
        WHERE member_id = 'TEST003'
          AND member_status = 'gecikmeli'
          AND is_current = TRUE
    """)
    assert cur.fetchone()[0] == 1, "Yeni kayıt oluşturulmadı!"


# ==========================================
# TEST 4 — İdempotency: aynı veri iki kez yüklenince duplike oluşmaz
# ==========================================
def test_idempotency(conn):
    cur = conn.cursor()
    _insert_member(cur, conn,
        'TEST004', 'Test Kisi4', 'test004uniquehash', 'Istanbul', 'Bakirkoy',
        '1995-03-20', 70000, '2024-01-01', 'aktif',
        '05001234567', 'test4@test.com')

    load_dim_member_scd2(conn)
    load_dim_member_scd2(conn)  # aynı veriyle ikinci kez

    cur.execute("SELECT COUNT(*) FROM dim_member WHERE member_id = 'TEST004'")
    assert cur.fetchone()[0] == 1, "Duplike oluştu — idempotency bozuldu!"


# ==========================================
# TEST 5 — Uçtan uca: pipeline çalışır, log yazar, hata vermez
# ==========================================
def test_pipeline_run_log_tablosuna_yazma(conn):
    """
    Uçtan uca test: pipeline baştan sona çalışır,
    pipeline_runs tablosuna yazar ve hiçbir stage 'FAILED' bitmez.
    """
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM pipeline_runs")
    before = cur.fetchone()[0]
    conn.commit()

    run_pipeline()

    new_conn = get_conn()
    new_cur  = new_conn.cursor()

    new_cur.execute("SELECT COUNT(*) FROM pipeline_runs")
    after = new_cur.fetchone()[0]
    assert after > before, "Pipeline run log tablosuna yazılmadı!"

    # FIX: 'failed' → 'FAILED' — DDL ve ETL pipeline büyük harf kullanıyor
    new_cur.execute("""
        SELECT stage FROM pipeline_runs
        WHERE status = 'FAILED'
          AND run_id > %s
    """, (before,))
    failed_stages = new_cur.fetchall()
    new_conn.close()

    assert len(failed_stages) == 0, f"Başarısız stage'ler: {failed_stages}"