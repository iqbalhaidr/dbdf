import polars as pl
import time

def generate_large_csv(filename: str = "sample_1m.csv", n: int = 1_000_000):
    print(f"Mulai membuat {n} baris data...")
    start_time = time.perf_counter()
    
    start_id = 1
    padding_length = 985 
    
    # Membuat DataFrame Polars
    df = pl.DataFrame({
        "customer_id": range(start_id, start_id + n),
        "name": [f"Customer {i}".ljust(padding_length, 'X') for i in range(start_id, start_id + n)],
        "email": [f"user{i}@example.com" for i in range(start_id, start_id + n)],
    })
    
    print(f"Menulis data ke {filename}...")
    df.write_csv(filename)
    
    elapsed = time.perf_counter() - start_time
    print(f"Selesai dalam {elapsed:.2f} detik! File {filename} siap digunakan.")

if __name__ == "__main__":
    generate_large_csv()