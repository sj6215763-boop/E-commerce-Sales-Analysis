
from pathlib import Path
import csv

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Start Spark
spark = (
    SparkSession.builder
    .appName("EcommerceBigDataAnalytics")
    .master("local[2]")
    .config("spark.driver.memory", "2g")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# 2. Dataset settings
TOTAL_RECORDS = 1_000_000
BATCH_SIZE = 100_000

PROJECT_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_DIR / "Data"
OUTPUT_FILE = OUTPUT_DIR / "ecommerce_sales.csv"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

regions = ["West", "South", "North", "East"]

categories = [
    "Electronics", "Furniture", "Clothing", "Accessories"
]

products = [
    "Laptop", "Mobile Phone", "Headphones", "Monitor",
    "Chair", "Table", "Sofa", "Desk",
    "Jacket", "Shoes", "Watch", "Backpack"
]

customers = [
    "Rahul", "Priya", "Amit", "Neha", "Arjun",
    "Sneha", "Rohan", "Pooja", "Vikram", "Anjali"
]

columns = [
    "Order_ID", "Order_Date", "Customer_ID", "Customer_Name",
    "Region", "Category", "Product", "Quantity",
    "Sales", "Discount", "Profit"
]

try:
    # Create a fresh CSV for this run
    with open(
        OUTPUT_FILE, "w", newline="", encoding="utf-8"
    ) as file:
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()

        for start in range(0, TOTAL_RECORDS, BATCH_SIZE):
            count = min(BATCH_SIZE, TOTAL_RECORDS - start)

            df = (
                spark.range(start, start + count)
                .withColumnRenamed("id", "row_id")

                .withColumn(
                    "Order_ID",
                    F.concat(
                        F.lit("O"),
                        F.lpad(
                            (F.col("row_id") + 1).cast("string"),
                            7, "0"
                        )
                    )
                )

                .withColumn(
                    "Order_Date",
                    F.date_add(
                        F.lit("2025-01-01").cast("date"),
                        (F.rand(seed=10) * 365).cast("int")
                    )
                )

                .withColumn(
                    "Customer_ID",
                    F.concat(
                        F.lit("C"),
                        F.lpad(
                            (
                                F.floor(F.rand(seed=20) * 10000) + 1
                            ).cast("string"),
                            5, "0"
                        )
                    )
                )

                .withColumn(
                    "Customer_Name",
                    F.element_at(
                        F.array(*[F.lit(x) for x in customers]),
                        (F.floor(F.rand(seed=25) * 10) + 1).cast("int")
                    )
                )

                .withColumn(
                    "Region",
                    F.element_at(
                        F.array(*[F.lit(x) for x in regions]),
                        (F.floor(F.rand(seed=30) * 4) + 1).cast("int")
                    )
                )

                .withColumn(
                    "Category",
                    F.element_at(
                        F.array(*[F.lit(x) for x in categories]),
                        (F.floor(F.rand(seed=40) * 4) + 1).cast("int")
                    )
                )

                .withColumn(
                    "Product",
                    F.element_at(
                        F.array(*[F.lit(x) for x in products]),
                        (F.floor(F.rand(seed=50) * 12) + 1).cast("int")
                    )
                )

                .withColumn(
                    "Quantity",
                    (F.floor(F.rand(seed=60) * 5) + 1).cast("int")
                )

                .withColumn(
                    "Sales",
                    F.round(
                        F.col("Quantity")
                        * (F.floor(F.rand(seed=70) * 12) + 1)
                        * 1000,
                        2
                    )
                )

                .withColumn(
                    "Discount",
                    F.element_at(
                        F.array(
                            F.lit(0.0), F.lit(0.05), F.lit(0.10),
                            F.lit(0.15), F.lit(0.20)
                        ),
                        (F.floor(F.rand(seed=80) * 5) + 1).cast("int")
                    )
                )

                .withColumn(
                    "Profit",
                    F.round(
                        F.col("Sales")
                        * (1 - F.col("Discount"))
                        * (F.floor(F.rand(seed=90) * 4) + 5)
                        / 100,
                        2
                    )
                )
                .drop("row_id")
            )

            # Stream records to CSV without Spark's Hadoop file writer
            for row in df.toLocalIterator():
                writer.writerow(row.asDict())

            file.flush()
            print(
                f"Generated {start + count:,} "
                f"/ {TOTAL_RECORDS:,} records"
            )

    print("\nDataset generated successfully!")
    print(f"File location: {OUTPUT_FILE}")

finally:
    spark.stop()