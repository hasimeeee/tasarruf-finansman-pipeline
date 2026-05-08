"""
conftest.py — Proje kök dizininde yaşar.

Neden gerekli:
  - test_scd2.py içinde sys.path.append("../src") vardı; proje src/ alt klasörü
    kullanmıyor, kök dizinde çalışıyor — bu satır import'ları kırıyordu.
  - Tüm sys.path manipülasyonu buraya taşındı. Test dosyaları sadece
    `from etl_pipeline import ...` gibi düz import yapar.

pytest.ini'deki `pythonpath = .` ile birlikte çalışır.
"""

import sys, os

# tests/ içindeyiz: bir üst = proje kökü, oradan src/ ekle
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))