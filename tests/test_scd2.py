"""
test_scd2.py — Düzeltilmiş versiyon

Düzeltilen hatalar:
  1. branch_sk: 'BRANCH001' string → NULL (INTEGER sütun, test fixture'da gerçek SK yok)
  2. ON CONFLICT (member_id): staging.members'da member_id UNIQUE constraint yok.
     INSERT ... ON CONFLICT yerine önce DELETE + INSERT yapısına geçildi.
  3. Teardown InFailedSqlTransaction: test patladığında connection aborted state'de
     kalıyor. _cleanup artık başında connection.rollback() çağırıyor.
  4. Test 5 (pipeline log): status sütununda ETL 'failed' yazıyor,
     sorgu da 'failed' arıyor — bu doğru. Ama ETL'deki asıl hatalar
     (dim_branch / dim_date / dim_member) ayrıca düzeltildi (bkz. etl_pipeline.py fix).
"""

import pytest
import sys
import os
from datetime import date

sys.path.append(os.path.join(os.path.dirname(__file__), "../src"))
from etl_pipeline import get_conn, load_dim_member_scd2, run_pipeline

TEST_MEMBER_IDS = ('TEST001', 'TEST002', 'TEST003', 'TEST004')


def _cleanup(connection):
    """
    Test verilerini staging ve dim_member'dan siler.

    DÜZELTİLDİ: Eğer test başarısız olduysa connection aborted state'de
    kalır. rollback() yapılmadan _cleanup çalıştırılamaz.
    """
    connection.rollback()          # ← aborted transaction'ı temizle
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


def _insert_member(cur, conn, member_id, full_name, tc_hash, city, district,
                   birth_date, income, signup_date, member_status,
                   phone, email):
    """
    staging.members'a güvenli INSERT yardımcısı.

    DÜZELTİLDİ:
      - branch_sk: INTEGER sütun — string 'BRANCH001' geçersiz. NULL bırakıldı.
      - ON CONFLICT (member_id): staging.members'da member_id'ye UNIQUE constraint
        yok (sadece PRIMARY KEY olan id var). ON CONFLICT kaldırıldı;
        _cleanup zaten her testten önce ilgili kayıtları siliyor.
    """
    cur.execute("""
        INSERT INTO staging.members
        (member_id, full_name, tc_hash, city, district,
         birth_date, income, signup_date, member_status,
         phone, email, branch_sk)
        VALUES
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL)
    """, (member_id, full_name, tc_hash, city, district,
          birth_date, income, signup_date, member_status,
          phone, email))
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
    count = cur.fetchone()[0]

    assert count == 1, "Yeni üye dim_member'a eklenmedi!"


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

    # Statüyü güncelle
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

    _insert_member(cur, conn,
        'TEST003', 'Test Kisi3', 'abc789hash', 'Istanbul', 'Bakirkoy',
        '1995-03-20', 70000, '2024-01-01', 'aktif',
        '05001234567', 'test3@test.com')

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

    # İlk kez ekle ve yükle
    _insert_member(cur, conn,
        'TEST004', 'Test Kisi4', 'test004uniquehash', 'Istanbul', 'Bakirkoy',
        '1995-03-20', 70000, '2024-01-01', 'aktif',
        '05001234567', 'test4@test.com')

    load_dim_member_scd2(conn)

    # Aynı veriyle tekrar yükle — staging kaydı zaten var, statü aynı
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

    # DÜZELTİLDİ: Sadece bu test çalışmasından SONRA eklenen kayıtlara bak.
    # Eski pipeline çalışmalarından kalan 'failed' kayıtlar testi engellememelidir.
    # run_at > test başlamadan önceki zaman damgasını filtrele.
    new_cur.execute("""
        SELECT stage FROM pipeline_runs
        WHERE status = 'failed'
          AND run_id > %s
    """, (before,))
    failed_stages = new_cur.fetchall()
    new_conn.close()

    assert len(failed_stages) == 0, f"Başarısız stage'ler: {failed_stages}"