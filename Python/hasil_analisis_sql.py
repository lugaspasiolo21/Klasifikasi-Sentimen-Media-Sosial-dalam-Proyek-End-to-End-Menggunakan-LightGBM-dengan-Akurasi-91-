import pandas as pd
import mysql.connector

# --- 1. PENGATURAN KONEKSI & KUERI ---

db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'social_media_db'
}

output_filename = 'hasil_analisis_sql_lengkap.xlsx'

queries = {
    # <<< TAMBAHAN: Sheet untuk mengekspor semua data >>>
    "Semua_Data_Sentimen": """
        SELECT * FROM sentiments;
    """,
    "Distribusi_Sentimen": """
        SELECT
            CASE
                WHEN category = '1' THEN 'Positif'
                WHEN category = '0' THEN 'Netral'
                WHEN category = '-1' THEN 'Negatif'
            END AS sentiment,
            COUNT(*) AS jumlah,
            CONCAT(ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM sentiments WHERE category IN ('-1','0','1')), 2), '%') AS persentase
        FROM sentiments
        WHERE category IN ('-1','0','1')
        GROUP BY sentiment
        ORDER BY jumlah DESC;
    """,
    "Perbandingan_Platform": """
        SELECT
            platform,
            CASE
                WHEN category = '1' THEN 'Positif'
                WHEN category = '0' THEN 'Netral'
                WHEN category = '-1' THEN 'Negatif'
            END AS sentiment,
            COUNT(*) AS jumlah_sentimen
        FROM sentiments
        WHERE category IN ('-1','0','1')
        GROUP BY platform, sentiment
        ORDER BY platform, jumlah_sentimen DESC;
    """,
    "Komentar_Terpopuler": """
        SELECT
            clean_text,
            platform,
            COUNT(*) AS jumlah_kemunculan
        FROM sentiments
        WHERE clean_text IS NOT NULL AND clean_text != ''
        GROUP BY clean_text, platform
        ORDER BY jumlah_kemunculan DESC
        LIMIT 20;
    """,
    "Panjang_Komentar": """
        SELECT
            platform,
            CASE
                WHEN category = '1' THEN 'Positif'
                WHEN category = '0' THEN 'Netral'
                WHEN category = '-1' THEN 'Negatif'
            END AS sentiment,
            ROUND(AVG(LENGTH(clean_text))) AS rata_rata_panjang_karakter
        FROM sentiments
        WHERE category IN ('-1','0','1')
        GROUP BY platform, sentiment
        ORDER BY platform, rata_rata_panjang_karakter DESC;
    """,
    "Kualitas_Data_Kosong": """
        SELECT
            platform,
            COUNT(*) AS jumlah_total_data,
            SUM(CASE WHEN clean_text IS NULL OR clean_text = '' THEN 1 ELSE 0 END) AS jumlah_komentar_kosong
        FROM sentiments
        GROUP BY platform;
    """
}

# --- 2. PROSES EKSPOR KE EXCEL ---

try:
    conn = mysql.connector.connect(**db_config)
    print("✅ Koneksi ke database berhasil.")
    
    with pd.ExcelWriter(output_filename, engine='openpyxl') as writer:
        print("⚙️  Memulai proses ekspor...")
        for sheet_name, query in queries.items():
            print(f"    -> Menjalankan kueri untuk sheet '{sheet_name}'...")
            df = pd.read_sql(query, conn)
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    
    print(f"\n✅ Semua kueri berhasil diekspor ke file '{output_filename}'.")

except Exception as e:
    print(f"❌ Terjadi error: {e}")

finally:
    if 'conn' in locals() and conn.is_connected():
        conn.close()
        print("Koneksi ditutup.")