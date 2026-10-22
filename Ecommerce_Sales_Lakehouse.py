# Databricks notebook source
print("Databricks E-Commerce Sales Analytics Project")

# COMMAND ----------

from pyspark.sql.functions import *

file_path = "/Volumes/workspace/default/ecommerce_data/ecommerce_sales_analytics_5000.csv"

df = spark.read.csv(
    file_path,
    header=True,
    inferSchema=True
)

print("Total records:", df.count())

# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(df)

# COMMAND ----------

bronze_df = df

bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_ecommerce_sales")

# COMMAND ----------

spark.sql("SHOW TABLES").show()

# COMMAND ----------

from pyspark.sql.functions import col, round

silver_df = (
    bronze_df
    .filter(col("quantity") > 0)
    .filter(col("unit_price") > 0)
    .filter((col("discount") >= 0) & (col("discount") <= 1))
    .filter((col("customer_rating") >= 1) & (col("customer_rating") <= 5))
    .withColumn(
        "calculated_revenue",
        round(
            col("quantity") *
            col("unit_price") *
            (1 - col("discount")),
            2
        )
    )
)

# COMMAND ----------

silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_ecommerce_sales")

# COMMAND ----------

print("Bronze records:", bronze_df.count())
print("Silver records:", silver_df.count())

# COMMAND ----------

from pyspark.sql.functions import abs, max

revenue_check = silver_df.withColumn(
    "revenue_difference",
    round(abs(col("revenue") - col("calculated_revenue")), 2)
)

revenue_check.select(
    max("revenue_difference").alias("max_revenue_difference")
).show()

# COMMAND ----------

gold_daily_sales = spark.sql("""
    SELECT
        order_date,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(quantity) AS total_units,
        ROUND(SUM(calculated_revenue), 2) AS total_revenue,
        ROUND(AVG(customer_rating), 2) AS avg_customer_rating,
        ROUND(AVG(delivery_days), 2) AS avg_delivery_days
    FROM silver_ecommerce_sales
    GROUP BY order_date
    ORDER BY order_date
""")

# COMMAND ----------

gold_daily_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_daily_sales")

# COMMAND ----------

gold_category_sales = spark.sql("""
    SELECT
        product_category,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(quantity) AS total_units,
        ROUND(SUM(calculated_revenue), 2) AS total_revenue,
        ROUND(AVG(discount), 2) AS avg_discount,
        ROUND(AVG(customer_rating), 2) AS avg_customer_rating
    FROM silver_ecommerce_sales
    GROUP BY product_category
    ORDER BY total_revenue DESC
""")

# COMMAND ----------

gold_category_sales = spark.sql("""
    SELECT
        product_category,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(quantity) AS total_units,
        ROUND(SUM(calculated_revenue), 2) AS total_revenue,
        ROUND(AVG(discount), 2) AS avg_discount,
        ROUND(AVG(customer_rating), 2) AS avg_customer_rating
    FROM silver_ecommerce_sales
    GROUP BY product_category
    ORDER BY total_revenue DESC
""")

# COMMAND ----------

gold_category_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_category_sales")

# COMMAND ----------

gold_region_sales = spark.sql("""
    SELECT
        region,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(quantity) AS total_units,
        ROUND(SUM(calculated_revenue), 2) AS total_revenue,
        ROUND(AVG(delivery_days), 2) AS avg_delivery_days,
        ROUND(AVG(customer_rating), 2) AS avg_customer_rating
    FROM silver_ecommerce_sales
    GROUP BY region
    ORDER BY total_revenue DESC
""")

# COMMAND ----------

gold_region_sales.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_region_sales")

# COMMAND ----------

spark.sql("SHOW TABLES").show()

# COMMAND ----------

spark.sql("SHOW TABLES").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(DISTINCT order_id) AS total_orders,
# MAGIC     SUM(quantity) AS total_units_sold,
# MAGIC     ROUND(SUM(calculated_revenue), 2) AS total_revenue,
# MAGIC     ROUND(AVG(calculated_revenue), 2) AS avg_order_revenue,
# MAGIC     ROUND(AVG(customer_rating), 2) AS avg_customer_rating,
# MAGIC     ROUND(AVG(delivery_days), 2) AS avg_delivery_days
# MAGIC FROM silver_ecommerce_sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     product_category,
# MAGIC     total_orders,
# MAGIC     total_units,
# MAGIC     total_revenue,
# MAGIC     avg_discount,
# MAGIC     avg_customer_rating
# MAGIC FROM gold_category_sales
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     region,
# MAGIC     total_orders,
# MAGIC     total_units,
# MAGIC     total_revenue,
# MAGIC     avg_delivery_days,
# MAGIC     avg_customer_rating
# MAGIC FROM gold_region_sales
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     DATE_FORMAT(order_date, 'yyyy-MM') AS month,
# MAGIC     SUM(total_orders) AS total_orders,
# MAGIC     SUM(total_units) AS total_units,
# MAGIC     ROUND(SUM(total_revenue), 2) AS total_revenue
# MAGIC FROM gold_daily_sales
# MAGIC GROUP BY DATE_FORMAT(order_date, 'yyyy-MM')
# MAGIC ORDER BY month;

# COMMAND ----------

