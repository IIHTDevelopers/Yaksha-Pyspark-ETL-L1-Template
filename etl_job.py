from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("ETLJob").getOrCreate()
    df = spark.read.csv("input/*.csv", header=True, inferSchema=True)
    # BUG: bringing all records to driver
    rows = df.collect()
    high_value = [r for r in rows if r['amount'] and float(r['amount']) > 1000]
    spark.createDataFrame(high_value).write.mode("overwrite").parquet("output/")

if __name__ == "__main__":
    main()
