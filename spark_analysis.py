# ============================================================
# E-COMMERCE SALES ANALYSIS USING PYSPARK + SPARK SQL
# ============================================================

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# ============================================================
# 1. PROJECT PATHS
# ============================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DATA_FILE = os.path.join(
    BASE_DIR,
    "Data",
    "ecommerce_sales.csv"
)

# Actual Power BI file from your computer
POWERBI_FILE = r"C:\Users\HP\Documents\sales analytics dasboard.pbix.pbix"


# ============================================================
# 2. CHECK DATASET
# ============================================================

print("\n" + "=" * 70)
print("E-COMMERCE SALES ANALYSIS USING PYSPARK")
print("=" * 70)

print("\nProject Directory:")
print(BASE_DIR)

print("\nDataset:")
print(DATA_FILE)

if not os.path.exists(DATA_FILE):
    print("\nERROR: Dataset file not found!")
    print("Expected location:")
    print(DATA_FILE)
    sys.exit(1)

print("\nDataset found successfully!")


# ============================================================
# 3. CREATE SPARK SESSION
# ============================================================

print("\n" + "=" * 70)
print("STARTING SPARK")
print("=" * 70)

spark = (
    SparkSession.builder
    .appName("Ecommerce Sales Analysis")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\nSpark started successfully.")
print("Spark Version:", spark.version)


# ============================================================
# 4. LOAD CSV DATASET
# ============================================================

print("\n" + "=" * 70)
print("LOADING DATASET")
print("=" * 70)

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "yyyy-MM-dd")
    .csv(DATA_FILE)
)

print("\nDataset loaded successfully.")


# ============================================================
# 5. DISPLAY FIRST 10 RECORDS
# ============================================================

print("\n" + "=" * 70)
print("FIRST 10 RECORDS")
print("=" * 70)

df.show(10, truncate=False)


# ============================================================
# 6. DISPLAY COLUMNS
# ============================================================

print("\n" + "=" * 70)
print("COLUMN NAMES")
print("=" * 70)

print(df.columns)


# ============================================================
# 7. DISPLAY SCHEMA
# ============================================================

print("\n" + "=" * 70)
print("DATASET SCHEMA")
print("=" * 70)

df.printSchema()


# ============================================================
# 8. DATASET STATISTICS
# ============================================================

print("\n" + "=" * 70)
print("DATASET STATISTICS")
print("=" * 70)

print("Number of Rows:", df.count())
print("Number of Columns:", len(df.columns))

df.describe().show()


# ============================================================
# 9. NULL VALUE CHECK
# ============================================================

print("\n" + "=" * 70)
print("NULL VALUE CHECK")
print("=" * 70)

for column_name in df.columns:

    null_count = df.filter(
        col(column_name).isNull()
    ).count()

    print(f"{column_name}: {null_count} null values")


# ============================================================
# 10. CREATE TEMPORARY SQL VIEW
# ============================================================

df.createOrReplaceTempView("sales")

print("\nTemporary SQL view 'sales' created successfully.")


# ============================================================
# 11. TOTAL SALES AND TOTAL PROFIT
# ============================================================

print("\n" + "=" * 70)
print("TOTAL SALES AND TOTAL PROFIT")
print("=" * 70)

total_sales_profit = spark.sql("""
    SELECT
        SUM(Sales) AS Total_Sales,
        SUM(Profit) AS Total_Profit
    FROM sales
""")

total_sales_profit.show()


# ============================================================
# 12. REGION-WISE SALES AND PROFIT
# ============================================================

print("\n" + "=" * 70)
print("REGION-WISE SALES AND PROFIT")
print("=" * 70)

region_analysis = spark.sql("""
    SELECT
        Region,
        SUM(Sales) AS Total_Sales,
        SUM(Profit) AS Total_Profit
    FROM sales
    GROUP BY Region
    ORDER BY Total_Sales DESC
""")

region_analysis.show()


# ============================================================
# 13. CATEGORY-WISE SALES AND PROFIT
# ============================================================

print("\n" + "=" * 70)
print("CATEGORY-WISE SALES AND PROFIT")
print("=" * 70)

category_analysis = spark.sql("""
    SELECT
        Category,
        SUM(Sales) AS Total_Sales,
        SUM(Profit) AS Total_Profit
    FROM sales
    GROUP BY Category
    ORDER BY Total_Sales DESC
""")

category_analysis.show()


# ============================================================
# 14. TOP 10 PRODUCTS BY SALES
# ============================================================

print("\n" + "=" * 70)
print("TOP 10 PRODUCTS BY SALES")
print("=" * 70)

top_products = spark.sql("""
    SELECT
        Product,
        SUM(Sales) AS Total_Sales
    FROM sales
    GROUP BY Product
    ORDER BY Total_Sales DESC
    LIMIT 10
""")

top_products.show(truncate=False)


# ============================================================
# 15. TOP 10 PRODUCTS BY PROFIT
# ============================================================

print("\n" + "=" * 70)
print("TOP 10 PRODUCTS BY PROFIT")
print("=" * 70)

top_profit_products = spark.sql("""
    SELECT
        Product,
        SUM(Profit) AS Total_Profit
    FROM sales
    GROUP BY Product
    ORDER BY Total_Profit DESC
    LIMIT 10
""")

top_profit_products.show(truncate=False)


# ============================================================
# 16. LOWEST 10 PRODUCTS BY PROFIT
# ============================================================

print("\n" + "=" * 70)
print("LOWEST 10 PRODUCTS BY PROFIT")
print("=" * 70)

lowest_profit_products = spark.sql("""
    SELECT
        Product,
        SUM(Profit) AS Total_Profit
    FROM sales
    GROUP BY Product
    ORDER BY Total_Profit ASC
    LIMIT 10
""")

lowest_profit_products.show(truncate=False)


# ============================================================
# 17. CATEGORY PERFORMANCE
# ============================================================

print("\n" + "=" * 70)
print("CATEGORY PERFORMANCE")
print("=" * 70)

category_performance = spark.sql("""
    SELECT
        Category,
        COUNT(*) AS Total_Orders,
        SUM(Quantity) AS Total_Quantity,
        SUM(Sales) AS Total_Sales,
        SUM(Profit) AS Total_Profit,
        ROUND(AVG(Discount), 2) AS Avg_Discount
    FROM sales
    GROUP BY Category
    ORDER BY Total_Sales DESC
""")

category_performance.show(truncate=False)


# ============================================================
# 18. REGION PERFORMANCE
# ============================================================

print("\n" + "=" * 70)
print("REGION PERFORMANCE")
print("=" * 70)

region_performance = spark.sql("""
    SELECT
        Region,
        COUNT(*) AS Total_Orders,
        SUM(Quantity) AS Total_Quantity,
        SUM(Sales) AS Total_Sales,
        SUM(Profit) AS Total_Profit,
        ROUND(AVG(Discount), 2) AS Avg_Discount
    FROM sales
    GROUP BY Region
    ORDER BY Total_Sales DESC
""")

region_performance.show(truncate=False)


# ============================================================
# 19. MONTHLY SALES ANALYSIS
# ============================================================

print("\n" + "=" * 70)
print("MONTHLY SALES ANALYSIS")
print("=" * 70)

monthly_sales = spark.sql("""
    SELECT
        YEAR(Order_Date) AS Year,
        MONTH(Order_Date) AS Month,
        SUM(Sales) AS Total_Sales,
        SUM(Profit) AS Total_Profit
    FROM sales
    GROUP BY YEAR(Order_Date), MONTH(Order_Date)
    ORDER BY Year, Month
""")

monthly_sales.show(50, truncate=False)


# ============================================================
# 20. YEARLY SALES ANALYSIS
# ============================================================

print("\n" + "=" * 70)
print("YEARLY SALES ANALYSIS")
print("=" * 70)

yearly_sales = spark.sql("""
    SELECT
        YEAR(Order_Date) AS Year,
        SUM(Sales) AS Total_Sales,
        SUM(Profit) AS Total_Profit
    FROM sales
    GROUP BY YEAR(Order_Date)
    ORDER BY Year
""")

yearly_sales.show(truncate=False)


# ============================================================
# 21. REGION + CATEGORY ANALYSIS
# ============================================================

print("\n" + "=" * 70)
print("REGION + CATEGORY SALES ANALYSIS")
print("=" * 70)

region_category = spark.sql("""
    SELECT
        Region,
        Category,
        SUM(Sales) AS Total_Sales,
        SUM(Profit) AS Total_Profit
    FROM sales
    GROUP BY Region, Category
    ORDER BY Region, Total_Sales DESC
""")

region_category.show(100, truncate=False)


# ============================================================
# 22. PROFIT MARGIN
# ============================================================

print("\n" + "=" * 70)
print("PROFIT MARGIN BY CATEGORY")
print("=" * 70)

profit_margin = spark.sql("""
    SELECT
        Category,
        SUM(Sales) AS Total_Sales,
        SUM(Profit) AS Total_Profit,
        ROUND(
            (SUM(Profit) / SUM(Sales)) * 100,
            2
        ) AS Profit_Margin_Percent
    FROM sales
    GROUP BY Category
    ORDER BY Profit_Margin_Percent DESC
""")

profit_margin.show(truncate=False)


# ============================================================
# 23. LOSS-MAKING TRANSACTIONS
# ============================================================

print("\n" + "=" * 70)
print("LOSS-MAKING TRANSACTIONS")
print("=" * 70)

loss_transactions = spark.sql("""
    SELECT
        COUNT(*) AS Loss_Making_Transactions,
        SUM(Sales) AS Loss_Transaction_Sales,
        SUM(Profit) AS Loss_Transaction_Profit
    FROM sales
    WHERE Profit < 0
""")

loss_transactions.show()


# ============================================================
# 24. LOSS-MAKING PRODUCT DETAILS
# ============================================================

print("\n" + "=" * 70)
print("LOSS-MAKING PRODUCT DETAILS")
print("=" * 70)

loss_products = spark.sql("""
    SELECT
        Product,
        SUM(Sales) AS Total_Sales,
        SUM(Profit) AS Total_Profit
    FROM sales
    WHERE Profit < 0
    GROUP BY Product
    ORDER BY Total_Profit ASC
""")

if loss_products.count() > 0:
    loss_products.show(truncate=False)
else:
    print("No loss-making transactions found.")


# ============================================================
# 25. FINAL PROJECT SUMMARY
# ============================================================

print("\n" + "=" * 70)
print("FINAL PROJECT SUMMARY")
print("=" * 70)

summary = spark.sql("""
    SELECT
        COUNT(*) AS Total_Transactions,
        SUM(Quantity) AS Total_Quantity,
        SUM(Sales) AS Total_Sales,
        SUM(Profit) AS Total_Profit,
        ROUND(AVG(Discount), 2) AS Average_Discount
    FROM sales
""")

summary.show(truncate=False)


# ============================================================
# 26. STOP SPARK
# ============================================================

print("\n" + "=" * 70)
print("STOPPING SPARK")
print("=" * 70)

spark.stop()

print("\nSpark stopped successfully.")


# ============================================================
# 27. OPEN POWER BI DASHBOARD AUTOMATICALLY
# ============================================================

print("\n" + "=" * 70)
print("OPENING POWER BI DASHBOARD")
print("=" * 70)

if os.path.exists(POWERBI_FILE):

    print("\nPower BI file found:")
    print(POWERBI_FILE)

    try:
        os.startfile(POWERBI_FILE)

        print("\nPower BI dashboard is opening automatically...")

    except Exception as error:

        print("\nCould not open Power BI automatically.")
        print("Error:", error)

else:

    print("\nPower BI file was not found.")
    print("Expected location:")
    print(POWERBI_FILE)


# ============================================================
# 28. PROJECT COMPLETED
# ============================================================

print("\n" + "=" * 70)
print("PROJECT COMPLETED SUCCESSFULLY")
print("=" * 70)

print("\nPySpark analysis completed.")
print("Spark SQL analysis completed.")
print("Power BI dashboard launch attempted.")

print("\nThank you!")