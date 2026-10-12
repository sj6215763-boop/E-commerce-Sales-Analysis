
import csv
import sqlite3
from pathlib import Path

# Project paths
PROJECT_DIR = Path(__file__).resolve().parent.parent
CSV_FILE = PROJECT_DIR / "Data" / "ecommerce_sales.csv"
DB_FILE = PROJECT_DIR / "Data" / "ecommerce_sales.db"

if not CSV_FILE.exists():
    raise FileNotFoundError(f"CSV file not found: {CSV_FILE}")

# Create database and table
connection = sqlite3.connect(DB_FILE)

try:
    cursor = connection.cursor()

    cursor.execute("DROP TABLE IF EXISTS sales")

    cursor.execute("""
        CREATE TABLE sales (
            Order_ID TEXT,
            Order_Date TEXT,
            Customer_ID TEXT,
            Customer_Name TEXT,
            Region TEXT,
            Category TEXT,
            Product TEXT,
            Quantity INTEGER,
            Sales REAL,
            Discount REAL,
            Profit REAL
        )
    """)

    # Import in batches to limit memory usage
    insert_sql = """
        INSERT INTO sales VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
    """

    total_rows = 0

    with open(CSV_FILE, "r", encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)
        batch = []

        for row in reader:
            batch.append((
                row["Order_ID"],
                row["Order_Date"],
                row["Customer_ID"],
                row["Customer_Name"],
                row["Region"],
                row["Category"],
                row["Product"],
                int(row["Quantity"]),
                float(row["Sales"]),
                float(row["Discount"]),
                float(row["Profit"])
            ))

            if len(batch) >= 10000:
                cursor.executemany(insert_sql, batch)
                total_rows += len(batch)
                connection.commit()
                batch.clear()
                print(f"Imported {total_rows:,} records")

        if batch:
            cursor.executemany(insert_sql, batch)
            total_rows += len(batch)
            connection.commit()

    # Verify row count
    cursor.execute("SELECT COUNT(*) FROM sales")
    count = cursor.fetchone()[0]

    print("\nImport completed successfully!")
    print(f"Total records in database: {count:,}")
    print(f"Database location: {DB_FILE}")

finally:
    connection.close()