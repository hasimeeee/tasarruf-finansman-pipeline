import pytest
import sys
import os
from datetime import date

sys.path.append(os.path.join(os.path.dirname(__file__), "../src"))
from etl_pipeline import get_conn, load_dim_member_scd2, run_pipeline

TEST_MEMBER_IDS = ('TEST001', 'TEST002', 'TEST003', 'TEST004')

def _cleanup(connection):
    """Test verilerini staging ve dim_member'dan siler."""
    cur = connection.cursor()
    cur.execute("""
        DELETE FROM dim_member
        WHERE member_id IN %s
    """, (TEST_MEMBER_IDS,))
    cur.execute("""
        DELETE FROM staging.members
        WHERE member_id IN %s
    """, (TEST_MEMBER_IDS,))
    connection.commit()


@pytest.fixture
def conn():
    connection = get_conn()
    _cleanup(connection)   # test başında temizle — önceki çalıştırma kalıntısı olabilir
    yield connection
    _cleanup(connection)   # test bitince temizle
    connection.close()


# ==========================================
# TEST 1 — Yeni üye dim_member'a eklenir
# ==========================================
def test_yeni_uye_eklenir(conn):
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO staging.members
        (member_id, full_name, tc_hash, city, district,
         birth_date, income, signup_date, member_status,
         phone, email)
        VALUES
        ('TEST001', 'Test Kisi', 'abc123hash', 'Istanbul', 'Kadikoy',
         '1990-01-01', 50000, '2024-01-01', 'aktif',
         '05001234567', 'test@test.com')
        ON CONFLICT (member_id) DO NOTHING
    """)
    conn.commit()

    load_dim_member_scd2(conn)

    cur.execute("""
        SELECT COUNT(*) FROM dim_member
        WHERE member_id = 'TEST001' AND is_current = TRUE
    """)
    count = cur.fetchone()[0]

    assert count == 1, "Yeni üye dim_member'a eklenmedi!"


# ==========================================
# TEST 2 — Statü değişince eski kayıt kapanır
# ==========================================
def test_statu_degisince_eski_kayit_kapanir(conn):
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO staging.members
        (member_id, full_name, tc_hash, city, district,
         birth_date, income, signup_date, member_status,
         phone, email)
        VALUES
        ('TEST002', 'Test Kisi2', 'abc456hash', 'Ankara', 'Cankaya',
         '1985-05-15', 60000, '2023-01-01', 'aktif',
         '05009876543', 'test2@test.com')
        ON CONFLICT (member_id) DO NOTHING
    """)
    conn.commit()
    load_dim_member_scd2(conn)

    cur.execute("""
        UPDATE staging.members
        SET member_status = 'gecikmeli'
        WHERE member_id = 'TEST002'
    """)
    conn.commit()
    load_dim_member_scd2(conn)

    cur.execute("""
        SELECT COUNT(*) FROM dim_member
        WHERE member_id = 'TEST002'
          AND member_status = 'aktif'
          AND is_current = FALSE
    """)
    count = cur.fetchone()[0]

    assert count == 1, "Eski kayıt kapatılmadı!"


# ==========================================
# TEST 3 — Statü değişince yeni kayıt açılır
# ==========================================
def test_statu_degisince_yeni_kayit_acilir(conn):
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO staging.members
        (member_id, full_name, tc_hash, city, district,
         birth_date, income, signup_date, member_status,
         phone, email)
        VALUES
        ('TEST003', 'Test Kisi3', 'abc789hash', 'Istanbul', 'Bakirkoy',
         '1995-03-20', 70000, '2024-01-01', 'aktif',
         '05001234567', 'test3@test.com')
        ON CONFLICT (member_id) DO NOTHING
    """)
    conn.commit()
    load_dim_member_scd2(conn)

    cur.execute("""
        UPDATE staging.members
        SET member_status = 'gecikmeli'
        WHERE member_id = 'TEST003'
    """)
    conn.commit()
    load_dim_member_scd2(conn)

    cur.execute("""
        SELECT COUNT(*) FROM dim_member
        WHERE member_id = 'TEST003'
          AND member_status = 'gecikmeli'
          AND is_current = TRUE
    """)
    count = cur.fetchone()[0]

    assert count == 1, "Yeni kayıt oluşturulmadı!"


# ==========================================
# TEST 4 — İdempotency: aynı veri iki kez yüklenince duplike oluşmaz
# ==========================================
def test_idempotency(conn):
    cur = conn.cursor()

    for _ in range(2):
        cur.execute("""
            INSERT INTO staging.members
            (member_id, full_name, tc_hash, city, district,
             birth_date, income, signup_date, member_status,
             phone, email)
            VALUES
            ('TEST004', 'Test Kisi4', 'test004uniquehash', 'Istanbul', 'Bakirkoy',
             '1995-03-20', 70000, '2024-01-01', 'aktif',
             '05001234567', 'test4@test.com')
            ON CONFLICT (member_id) DO NOTHING
        """)
        conn.commit()
        load_dim_member_scd2(conn)

    cur.execute("""
        SELECT COUNT(*) FROM dim_member
        WHERE member_id = 'TEST004'
    """)
    count = cur.fetchone()[0]

    assert count == 1, "Duplike oluştu — idempotency bozuldu!"


# ==========================================
# TEST 5 — Uçtan uca: pipeline çalışır, log yazar, hata vermez
# ==========================================
def test_pipeline_run_log_tablosuna_yazma(conn):
    """
    Uçtan uca test: pipeline baştan sona çalışır,
    pipeline_runs tablosuna yazar ve hiçbir stage 'failed' bitmez.
    """
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM pipeline_runs")
    before = cur.fetchone()[0]
    conn.commit()

    run_pipeline()

    # run_pipeline() kendi connection'ını açıp kapatıyor.
    # Aynı conn üzerinden okumak eski snapshot'ı görür,
    # bu yüzden yeni connection açarak taze veriyi okuyoruz.
    new_conn = get_conn()
    new_cur = new_conn.cursor()

    new_cur.execute("SELECT COUNT(*) FROM pipeline_runs")
    after = new_cur.fetchone()[0]
    assert after > before, "Pipeline run log tablosuna yazılmadı!"

    new_cur.execute("""
        SELECT stage FROM pipeline_runs
        WHERE status = 'failed'
        ORDER BY run_at DESC
        LIMIT 6
    """)
    failed_stages = new_cur.fetchall()
    new_conn.close()

    assert len(failed_stages) == 0, f"Başarısız stage'ler: {failed_stages}"