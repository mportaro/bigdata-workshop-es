import findspark
findspark.init()

from pyspark.sql import SparkSession
spark = (
    SparkSession.builder
    .appName("pyspark-df-overview")
    .config("spark.driver.memory", "512m")
    .config("spark.driver.cores", "1")
    .config("spark.executor.memory", "512m")
    .config("spark.executor.cores", "1")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

df = spark.read.csv("/dataset/pyspark-df-overview/census_income.csv.gz", header=True)
df.printSchema()

import pyspark.sql.types as t

census_schema = t.StructType([
      t.StructField('age', t.IntegerType(), True)
    , t.StructField('workclass', t.StringType(), True)
    , t.StructField('fnlwgt', t.IntegerType(), True)
    , t.StructField('education', t.StringType(), True)
    , t.StructField('education-num', t.IntegerType(), True)
    , t.StructField('marital-status', t.StringType(), True)
    , t.StructField('occupation', t.StringType(), True)
    , t.StructField('relationship', t.StringType(), True)
    , t.StructField('race', t.StringType(), True)
    , t.StructField('sex', t.StringType(), True)
    , t.StructField('capital-gain', t.DoubleType(), True)
    , t.StructField('capital-loss', t.DoubleType(), True)
    , t.StructField('hours-per-week', t.IntegerType(), True)
    , t.StructField('native-country', t.StringType(), True)
    , t.StructField('label', t.StringType(), True)
])

# Support for compressed (gziped) payload
df = spark.read.csv("/dataset/pyspark-df-overview/census_income.csv.gz", header=True, schema=census_schema)
df.printSchema()

df = df.drop('fnlwgt')
df.printSchema()

from pyspark.sql.functions import count, avg, desc

df.groupBy(['education']). \
agg(
    count('*').alias('qty'), 
    avg('age').alias('avg_age')
).orderBy(desc('qty')). \
show()

df.createOrReplaceTempView("census")
s = spark.sql("""
SELECT 
    education, 
    COUNT(*) AS qty, 
    AVG(age) AS avg_age
FROM census
GROUP BY education
""")
s.show()

# a transformation can be exposed as function
def my_query(field):
    return df.groupBy([field]). \
    agg(
        count('*').alias('qty'), 
        avg('age').alias('avg_age')
    ).orderBy(desc('qty'))
    

    
print(my_query('workclass').show())

df.select('age', 'education-num', 'capital-gain', 'capital-loss', 'hours-per-week').describe().show()

df.select('workclass', 'education', 'marital-status').describe().show()

df.freqItems(['marital-status']).show(truncate=False)

df.crosstab('age', 'label').sort("age_label").show()

df.groupby('native-country').agg({'native-country': 'count'}).sort('count(native-country)').show()

from pyspark.sql.functions import isnan, when, count, col

# All columns
# cols = df.columns
# Selected columns
cols = ['workclass', 'education-num', 'occupation', 'hours-per-week', 'native-country']

# https://stackoverflow.com/a/44631639/570393
df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in cols]).show()

# Total rows
print('total rows: %s' % df.count())

# After droping NA records
print('only complete rows: %s' % df.dropna().count())

def show_df(df, field='occupation'):
    df.groupBy(field).count().show()

show_df(df)

# Fill with a fixed value
new_df = df.fillna({'occupation': 'Other-service'})

# Count 
show_df(new_df)

from pyspark.sql.functions import mean
df.groupBy().agg(mean('hours-per-week').alias('hours-per-week')).show()

from pyspark.sql.functions import mean
import pandas as pd

data_to_fill = \
    df.groupBy().agg(mean('hours-per-week').alias('hours-per-week')).toPandas().to_dict('records')[0]

# Simple Python Dict Update
data_to_fill.update({'occupation': 'Other-service'})

data_to_fill

df.fillna(data_to_fill).select('hours-per-week', 'occupation').show(50)

# This is distributed
df_spark = df.groupBy('workclass').agg(count('*').alias('counts')).orderBy('counts')
# df_spark.show()

# This is running on driver
df_wk = df_spark.toPandas()

import matplotlib.pyplot as plt

df_wk.plot.bar(x='workclass', y='counts', figsize=(20,6));

# Check Pandas DF content
print(df_wk)


spark.stop()
