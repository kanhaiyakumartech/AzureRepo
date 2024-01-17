# Databricks notebook source
from pyspark.sql.functions import split, regexp_extract, current_date
from delta.tables import DeltaTable

def convert_to_snake_case(column_name):
    return column_name.lower().replace(' ', '_')

def extract_store_category(df, email_column):
    return df.withColumn('store_category', 
                        regexp_extract(df[email_column], r'@(.+?)\.', 1)
                       )

def create_date_columns(df, date_columns):
    for col in date_columns:
        df = df.withColumn(col, to_date(df[col], 'yyyy-MM-dd'))
    return df

def upsert_to_silver_layer(df, path, db_name, tb_name, mergecol):
    base_path = path + f"/{db_name}/{tb_name}"
    mapped_col = " AND ".join([f"old.{x} = new.{x}" for x in mergecol])
    
    if not DeltaTable.isDeltaTable(spark, f"{base_path}"):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        df.write.mode("overwrite").format("delta").option("path", base_path).saveAsTable(f"{db_name}.{tb_name}")
    else:
        delta_table = DeltaTable.forPath(spark, f"{base_path}")
        delta_table.alias("old").merge(df.alias("new"), mapped_col).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

def select_required_columns(product_df, store_df):
    return product_df.select(
        "store_id", "store_name", "location", "manager_name",
        "product_name", "product_code", "description", "category_id",
        "price", "stock_quantity", "supplier_id",
        "product_created_at", "product_updated_at", "image_url",
        "weight", "expiry_date", "is_active", "tax_rate"
    ).join(
        store_df.select("store_id", "store_name", "location", "manager_name"),
        "store_id"
    )

def transform_data_for_gold_layer(customer_sales_df, gold_df):
    return customer_sales_df.join(
        gold_df,
        customer_sales_df.product_id == gold_df.product_code,
        "inner"
    ).select(
        "order_date", "category", "city", "customer_id", "order_id",
        "product_id", "profit", "region", "sales", "segment",
        "ship_date", "ship_mode", "latitude", "longitude",
        "store_name", "location", "manager_name", "product_name",
        "price", "stock_quantity", "image_url"
    )
    

