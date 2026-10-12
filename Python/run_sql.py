
import sqlite3
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
DB_FILE = PROJECT_DIR / "Data" / "ecommerce_sales.db"

connection = sqlite3.connect(DB_FILE)

queries = {
    "1. Overall Sales and Profit": """
        SELECT
            COUNT(*) AS Total_Orders,
            ROUND(SUM(Sales), 2) AS Total_Sales,
            ROUND(SUM(Profit), 2) AS Total_Profit,
            SUM(Quantity) AS Total_Quantity
        FROM sales;
    """,

    "2. Region-wise Sales": """
        SELECT
            Region,
            ROUND(SUM(Sales), 2) AS Total_Sales,
            ROUND(SUM(Profit), 2) AS Total_Profit
        FROM sales
        GROUP BY Region
        ORDER BY Total_Sales DESC;
    """,

    "3. Category-wise Sales": """
        SELECT
            Category,
            ROUND(SUM(Sales), 2) AS Total_Sales,
            ROUND(SUM(Profit), 2) AS Total_Profit
        FROM sales
        GROUP BY Category
        ORDER BY Total_Sales DESC;
    """,

    "4. Top 10 Products": """
        SELECT
            Product,
            SUM(Quantity) AS Units_Sold,
            ROUND(SUM(Sales), 2) AS Total_Sales
        FROM sales
        GROUP BY Product
        ORDER BY Total_Sales DESC
        LIMIT 10;
    """,

    "5. Monthly Sales": """
        SELECT
            SUBSTR(Order_Date, 1, 7) AS Sales_Month,
            ROUND(SUM(Sales), 2) AS Monthly_Sales,
            ROUND(SUM(Profit), 2) AS Monthly_Profit
        FROM sales
        GROUP BY SUBSTR(Order_Date, 1, 7)
        ORDER BY Sales_Month;
    """
}

try:
    for title, sql in queries.items():
        print("\n" + "=" * 65)
        print(title)
        print("=" * 65)

        cursor = connection.execute(sql)
        print(" | ".join(column[0] for column in cursor.description))

        for row in cursor.fetchall():
            print(" | ".join(str(value) for value in row))
finally:
    connection.close()