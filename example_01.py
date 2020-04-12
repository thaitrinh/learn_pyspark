from pyspark.sql import SparkSession
import pandas as pd


df = pd.read_csv("permit-inspections.csv", delimiter=";")
print(df.shape)
appName = "PySpark Partition Example"
master = "local[1]"

spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .getOrCreate()

print(spark.version)

