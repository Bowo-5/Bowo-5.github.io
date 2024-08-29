from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType, DateType

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("MongoDB to PySpark") \
    .config("spark.mongodb.input.uri", "mongodb+srv://tmp-dta-st-1:Uoe3GROXyB4wXxox@cluster0-qov8h.mongodb.net/analytics_test.accounts") \
    .config("spark.mongodb.output.uri", "mongodb+srv://tmp-dta-st-1:Uoe3GROXyB4wXxox@cluster0-qov8h.mongodb.net/analytics_test") \
    .getOrCreate()

# Load data from MongoDB
accounts_df = spark.read.format("mongo").option("uri", "mongodb+srv://tmp-dta-st-1:Uoe3GROXyB4wXxox@cluster0-qov8h.mongodb.net/analytics_test.accounts").load()
customers_df = spark.read.format("mongo").option("uri", "mongodb+srv://tmp-dta-st-1:Uoe3GROXyB4wXxox@cluster0-qov8h.mongodb.net/analytics_test.customers").load()
transactions_df = spark.read.format("mongo").option("uri", "mongodb+srv://tmp-dta-st-1:Uoe3GROXyB4wXxox@cluster0-qov8h.mongodb.net/analytics_test.transactions").load()

# Convert data types
transactions_df = transactions_df.withColumn("transaction_amount", transactions_df["transaction_amount"].cast(FloatType()))
transactions_df = transactions_df.withColumn("transaction_date", transactions_df["transaction_date"].cast(DateType()))

# Aggregate data
total_transactions = transactions_df.groupBy("customer_id") \
    .agg(F.sum("transaction_amount").alias("total_transaction_amount"),
         F.count("transaction_id").alias("total_transactions"))

# Join dataframes
customer_transactions_df = customers_df.join(total_transactions, customers_df["_id"] == total_transactions["customer_id"], "left")

# Add additional features
customer_transactions_df = customer_transactions_df.withColumn("avg_transaction_amount",
                                                               customer_transactions_df["total_transaction_amount"] / customer_transactions_df["total_transactions"])

# Save the transformed data
customer_transactions_df.toPandas().to_csv("transformed_data.csv", index=False)



