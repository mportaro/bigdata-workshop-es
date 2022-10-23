import findspark

findspark.add_jars('/app/postgresql-42.1.4.jar')
findspark.init()

import pyspark.sql.types as t

from pyspark.sql import SparkSession
spark = (
    SparkSession.builder
    .appName("Stocks:ETL")
    .config("spark.driver.memory", "512m")
    .config("spark.driver.cores", "1")
    .config("spark.executor.memory", "512m")
    .config("spark.executor.cores", "1")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)
spark.version

stocks_dir = '/dataset/stocks-small'

import sys

from pyspark.sql import SparkSession

# UDF
from pyspark.sql.types import StringType
#
from pyspark.sql import functions as F
from pyspark.sql.window import Window

df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(stocks_dir)

df.count()
df.printSchema()

df.show()

df = df.withColumn('filename', F.input_file_name())

df.show(truncate=False)

df_lookup = spark.read.csv('/dataset/yahoo-symbols-201709.csv')

df_lookup.show()

def extract_symbol_from(filename):
    return filename.split('/')[-1].split('.')[0].upper()

# filename = 'file:///dataset/stocks-small/ibm.us.txt' # => IBM
extract_symbol_from('file:///dataset/stocks-small/ibm.us.txt')

extract_symbol = F.udf(lambda filename: extract_symbol_from(filename), StringType())

stocks_folder = stocks_dir
df = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv(stocks_folder) \
        .withColumn("name", extract_symbol(F.input_file_name()))

df.show(5)

df = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv(stocks_folder) \
        .withColumn("name", extract_symbol(F.input_file_name())) \
        .withColumnRenamed("Date", "dateTime") \
        .withColumnRenamed("Open", "open") \
        .withColumnRenamed("High", "high") \
        .withColumnRenamed("Low", "low") \
        .withColumnRenamed("Close", "close") \
        .drop("Volume", "OpenInt")

df_stocks = df

df_stocks.show(5)

lookup_file = '/dataset/yahoo-symbols-201709.csv'

symbols_lookup = spark.read. \
        option("header", True). \
        option("inferSchema", True). \
        csv(lookup_file). \
        select("Ticker", "Category Name"). \
        withColumnRenamed("Ticker", "symbol"). \
        withColumnRenamed("Category Name", "category")

df_stocks.show(3)
symbols_lookup.show(3)

joined_df = df_stocks \
    .withColumnRenamed('dateTime', "full_date") \
    .filter("full_date >= \"2017-09-01\"") \
    .withColumn("year", F.year("full_date")) \
    .withColumn("month", F.month("full_date")) \
    .withColumn("day", F.dayofmonth("full_date")) \
    .withColumnRenamed("name", "symbol") \
    .join(symbols_lookup, ["symbol"])

joined_df.show(3)

window20 = (Window.partitionBy(F.col('symbol')).orderBy(F.col("full_date")).rowsBetween(-20, 0))
window50 = (Window.partitionBy(F.col('symbol')).orderBy(F.col("full_date")).rowsBetween(-50, 0))
window100 = (Window.partitionBy(F.col('symbol')).orderBy(F.col("full_date")).rowsBetween(-100, 0))

stocks_moving_avg_df = joined_df \
    .withColumn("ma20", F.avg("close").over(window20)) \
    .withColumn("ma50", F.avg("close").over(window50)) \
    .withColumn("ma100", F.avg("close").over(window100))

# Moving Average
stocks_moving_avg_df.select('symbol', 'close', 'ma20').show(25)

output_dir = '/dataset/output.parquet'

stocks_moving_avg_df \
    .write \
    .mode('overwrite') \
    .partitionBy("year", "month", "day") \
    .parquet(output_dir)

df_parquet = spark.read.parquet(output_dir)

df_parquet.count()

df_parquet.createOrReplaceTempView("stocks")

badHighestClosingPrice = spark.sql("SELECT symbol, MAX(close) AS price FROM stocks WHERE full_date >= '2017-09-01' AND full_date < '2017-10-01' GROUP BY symbol")
badHighestClosingPrice.explain()

highestClosingPrice = spark.sql("SELECT symbol, MAX(close) AS price FROM stocks WHERE year=2017 AND month=9 GROUP BY symbol")
highestClosingPrice.explain()

# Write to Postgres
stocks_moving_avg_df \
    .drop("year", "month", "day") \
    .write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres/workshop") \
    .option("dbtable", "workshop.stocks") \
    .option("user", "workshop") \
    .option("password", "w0rkzh0p") \
    .option("driver", "org.postgresql.Driver") \
    .mode('overwrite') \
    .save()
# cambié mode('append') por mode('overwrite') en la linea 148 para que no salte error.

spark.stop()