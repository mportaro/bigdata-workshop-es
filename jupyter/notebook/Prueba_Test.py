import findspark
findspark.init()

from pyspark.sql import SparkSession
spark = (
    SparkSession.builder
    .appName("pyspark-titanic-exe")
    .config("spark.driver.memory", "512m")
    .config("spark.driver.cores", "1")
    .config("spark.executor.memory", "512m")
    .config("spark.executor.cores", "1")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.version

dft = spark.read.csv('/dataset/titanic.csv', header=True)

dft.show(10)