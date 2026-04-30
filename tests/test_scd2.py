import pytest
import sys
import os
from datetime import date

sys.path.append(os.path.join(os.path.dirname(__file__), "../src"))
from etl_pipeline import get_conn, load_dim_member_scd2, run_pipeline
@pytest.fixture
def conn():
    connection = get_conn()
    yield connection
    connection.rollback()
    connection.close()
#1
def test_yeni_uye_eklenir(conn):
    cur = conn.cursor()
    
    # 1. HAZIRLIK — staging'e yeni üye ekle
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
    
    # 2. AKSİYON — SCD2 çalıştır
    load_dim_member_scd2(conn)
    
    # 3. İDDİA — dim_member'da görünmeli
    cur.execute("""
        SELECT COUNT(*) FROM dim_member 
        WHERE member_id = 'TEST001' AND is_current = TRUE
    """)
    count = cur.fetchone()[0]
    
    assert count == 1, "Yeni üye dim_member'a eklenmedi!"

#2
def test_statu_degisince_eski_kayit_kapanir(conn):
    cur = conn.cursor()

    # staging'e yeni üye ekle
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
    # SCD2 çalıştır — dim_member'a kayıt oluşsun
    load_dim_member_scd2(conn)

    #statüyü değiştir
    cur.execute("""
        UPDATE staging.members
        SET member_status = 'gecikmeli'
        WHERE member_id = 'TEST002'
    """)
    conn.commit()
    # SCD2 tekrar çalışttır eski kaydı kapat
    load_dim_member_scd2(conn)

    #eski kaydı kapat
    cur.execute("""
        SELECT COUNT(*) FROM dim_member
        WHERE member_id = 'TEST002'
        AND member_status = 'aktif'
        AND is_current = FALSE
    """)
    count = cur.fetchone()[0]

    assert count == 1, "Eski kayıt kapatılmadı!"

#3
def test_statu_degisince_yeni_kayit_acilir(conn):
    cur = conn.cursor()

    # staging'e yeni üye ekle
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
    # SCD2 çalıştır — dim_member'a kayıt oluşsun
    load_dim_member_scd2(conn)

    #statüyü değiştir
    cur.execute("""
        UPDATE staging.members
        SET member_status = 'gecikmeli'
        WHERE member_id = 'TEST003'
    """)
    conn.commit()
    # SCD2 tekrar çalışttır eski kaydı kapat
    load_dim_member_scd2(conn)

    #eski kaydı kapat
    cur.execute("""
        SELECT COUNT(*) FROM dim_member
        WHERE member_id = 'TEST003'
        AND member_status = 'gecikmeli'
        AND is_current = TRUE
    """)
    count = cur.fetchone()[0]

    assert count == 1, "Yeni kayıt oluşturulmadı!"


#4
def test_idempotency(conn):
    cur = conn.cursor()

    # staging'e yeni üye ekle
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
    # SCD2 çalıştır — dim_member'a kayıt oluşsun
    load_dim_member_scd2(conn)

    # staging tekrar çalışşın
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
    # SCD2 çalıştır — dim_member'a kayıt oluşsun
    load_dim_member_scd2(conn)

    # dim_member'da kaç TEST004 var?
    cur.execute("""
        SELECT COUNT(*) FROM dim_member
        WHERE member_id = 'TEST004'
    """)
    count = cur.fetchone()[0]
    
    assert count == 1, "Duplike oluştu — idempotency bozuldu!"



#5
def test_pipeline_run_log_tablosuna_yazma(conn):
    cur = conn.cursor()
    
    cur.execute("SELECT COUNT(*) FROM pipeline_runs")
    before = cur.fetchone()[0]
    conn.commit()

    run_pipeline()

    # pipeline kendi connection'ını açıp commit'liyor
    # aynı conn üzerinden okumak eski snapshot'ı görüyor
    # yeni connection açarak taze veri oku
    new_conn = get_conn()
    new_cur = new_conn.cursor()
    new_cur.execute("SELECT COUNT(*) FROM pipeline_runs")
    after = new_cur.fetchone()[0]
    new_conn.close()

    assert after > before, "Pipeline run log tablosuna yazılmadı!"