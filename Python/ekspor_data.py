
import pandas as pd
import mysql.connector
from mysql.connector import Error

def export_sentiments_to_csv():
    """
    Menghubungkan ke database MySQL, mengambil data dari tabel sentiments,
    dan mengekspornya ke file CSV.
    """
    # --- SESUAIKAN DETAIL KONEKSI DATABASE ANDA DI SINI ---
    db_config = {
        'host': 'localhost',
        'user': 'root',        
        'password': '',        
        'database': 'social_media_db'
    }
    # ----------------------------------------------------

    # Kueri SQL untuk mengambil data yang bersih dan siap untuk ML
    query = """
    SELECT
        clean_text,
        category
    FROM
        sentiments
    WHERE
        clean_text IS NOT NULL
        AND clean_text != ''
        AND category IN ('-1', '0', '1');
    """
    
    nama_file_output = 'data_sentimen_ml.csv'
    
    try:
        print(f"Menghubungkan ke database '{db_config['database']}'...")
        # Membuat koneksi ke database
        conn = mysql.connector.connect(**db_config)
        
        if conn.is_connected():
            print("✅ Koneksi berhasil!")
            
            # Membaca data menggunakan pandas
            print("Mengambil data dari tabel 'sentiments'...")
            df = pd.read_sql(query, conn)
            
            # Menyimpan DataFrame ke file CSV
            # index=False agar tidak ada kolom indeks tambahan di file CSV
            df.to_csv(nama_file_output, index=False)
            
            print(f"✅ Data berhasil diekspor ke '{nama_file_output}' ({len(df)} baris).")
            
    except Error as e:
        print(f"❌ Error saat menghubungkan ke MySQL: {e}")
        
    finally:
        # Menutup koneksi
        if 'conn' in locals() and conn.is_connected():
            conn.close()
            print("Koneksi ditutup.")

# Menjalankan fungsi
if __name__ == '__main__':
    export_sentiments_to_csv()