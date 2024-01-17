# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, current_date, to_date
from utils import convert_to_snake_case, extract_store_category, create_date_columns, upsert_to_silver_layer, select_required_columns, transform_data_for_gold_layer

# Create a Spark session
spark = SparkSession.builder.appName("SalesProcessing").getOrCreate()

# 1) Read Customer Data
customer_path = "dbfs:/mnt/mountpointstorageadf/customer"
customer_df = spark.read.format("csv").option("header", "true").load(customer_path)

# 2) Convert column headers to snake case
customer_df = customer_df.toDF(*[convert_to_snake_case(col) for col in customer_df.columns])

# 3) Extract store category from email
customer_df = extract_store_category(customer_df, 'email')

# 4) Create 'created_at' and 'updated_at' columns
customer_df = customer_df.withColumn('created_at', current_date())
customer_df = customer_df.withColumn('updated_at', current_date())

# 5) Write based on upsert to silver layer
silver_layer_path = "silver/sales_view/customer"
upsert_to_silver_layer(customer_df, silver_layer_path, 'sales_view', 'customer', ['customer_id'])

# 6) Read Product Data
product_path = "dbfs:/mnt/mountpointstorageadf/product"
product_df = spark.read.format("csv").option("header", "true").load(product_path)

# 7) Convert column headers to snake case and create 'sub_category' column
product_df = product_df.toDF(*[convert_to_snake_case(col) for col in product_df.columns])
product_df = product_df.withColumn('sub_category', 
                                   when(col('category_id') == 1, 'phone')
                                   .when(col('category_id') == 2, 'laptop')
                                   .when(col('category_id') == 3, 'playstation')
                                   .when(col('category_id') == 4, 'e-device')
                                   .otherwise('unknown'))

# 8) Write to Silver Layer with Delta Format
silver_layer_path_product = "silver/sales_view/product"
upsert_to_silver_layer(product_df, silver_layer_path_product, 'sales_view', 'product', ['product_code'])

# 9) Read Delta Tables from Silver Layer
product_silver_path = "silver/sales_view/product"
store_silver_path = "silver/sales_view/store"
customer_sales_silver_path = "silver/sales_view/customer_sales"

product_df = spark.read.format("delta").table(product_silver_path)
store_df = spark.read.format("delta").table(store_silver_path)
customer_sales_df = spark.read.format("delta").table(customer_sales_silver_path)

# 10) Select Required Columns from Product and Store Tables
gold_df = select_required_columns(product_df, store_df)

# 11) Read Delta Table for Customer Sales
customer_sales_gold_path = "gold/sales_view/store_product_sales_analysis"
customer_sales_gold_df = spark.read.format("delta").table(customer_sales_gold_path)

# 12) Transform Data for StoreProductSalesAnalysis
final_df = transform_data_for_gold_layer(customer_sales_gold_df, gold_df)

# 13) Write to Gold Layer with Overwrite
final_gold_path = "gold/sales_view/store_product_sales_analysis"
final_df.write.format("delta").mode("overwrite").save(final_gold_path)



