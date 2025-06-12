from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder.appName("ETLJob").getOrCreate()
    df = spark.read.csv("input/*.csv", header=True, inferSchema=True)
    # SOLUTION: Use filter() with col() instead of collect() to avoid OOM
    high_value_df = df.filter(col("amount") > 1000)
    high_value_df.write.mode("overwrite").parquet("output/")

if __name__ == "__main__":
    main()
