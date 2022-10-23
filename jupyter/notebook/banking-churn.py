import findspark

findspark.init()

import pyspark.sql.types as t
import pyspark.sql.functions as f
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession

# Vamos a grabar los datos en una BD Postgres
findspark.add_jars('/app/postgresql-42.1.4.jar')

# Inicializamos la sesión de Spark para procesar los datos de manera distribuida
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

# A business manager of a consumer credit card portfolio is facing the problem of customer attrition.
# They want to analyze the data to find out the reason behind this and leverage the same to predict customers who are likely to drop off.

from pathlib import Path
import pandas as pd

# Extracción online - el link de kaggle no me permite llegar al .csv. En caso que pudiera usaria el siguiente código (no comentado):
# def data_online(url, refresh_cache=False):
#    cache_fn = Path('/dataset/BankChurners_cache.csv')
#    if not cache_fn.exists() or refresh_cache:
#        print("Getting data")
#        ds = pd.read_csv(url)
#        ds.to_csv(cache_fn, index=False)
#    print("Using cache")
#    ds = spark.read.csv(str(cache_fn), header=True)
#    return ds
#
# url = '/kaggle/input/credit-card-customers/BankChurners.csv'
# ds = data_online (url)

# Extracción desde directorio local
ds = spark.read.csv('/dataset/BankChurners.csv', header=True)

ds.printSchema()
ds.count()

# Las ultimas dos columnas del dataset (Naive Bayes Classification) son eliminadas segun recomendación.

ds=ds.drop('Naive_Bayes_Classifier_Attrition_Flag_Card_Category_Contacts_Count_12_mon_Dependent_count_Education_Level_Months_Inactive_12_mon_1')
ds=ds.drop('Naive_Bayes_Classifier_Attrition_Flag_Card_Category_Contacts_Count_12_mon_Dependent_count_Education_Level_Months_Inactive_12_mon_2')

ds=ds.withColumnRenamed("CLIENTNUM", "client#")

accounts_schema = t.StructType([
      t.StructField('client#', t.IntegerType(), True)
    , t.StructField('Attrition_Flag', t.StringType(), True)
    , t.StructField('Customer_Age', t.IntegerType(), True)
    , t.StructField('Gender', t.StringType(), True)
    , t.StructField('Dependent_count', t.IntegerType(), True)
    , t.StructField('Education_Level', t.StringType(), True)
    , t.StructField('Marital_Status', t.StringType(), True)
    , t.StructField('Income_Category', t.StringType(), True)
    , t.StructField('Card_Category', t.StringType(), True)
    , t.StructField('Months_on_book', t.IntegerType(), True)
    , t.StructField('Total_Relationship_Count', t.IntegerType(), True)
    , t.StructField('Months_Inactive_12_mon', t.IntegerType(), True)
    , t.StructField('Contacts_Count_12_mon', t.IntegerType(), True)
    , t.StructField('Credit_Limit', t.IntegerType(), True)
    , t.StructField('Total_Revolving_Bal', t.IntegerType(), True)
    , t.StructField('Avg_Open_To_Buy', t.IntegerType(), True)
    , t.StructField('Total_Amt_Chng_Q4_Q1', t.FloatType(), True)
    , t.StructField('Total_Trans_Amt', t.IntegerType(), True)
    , t.StructField('Total_Trans_Ct', t.IntegerType(), True)
    , t.StructField('Total_Ct_Chng_Q4_Q1', t.FloatType(), True)
    , t.StructField('Avg_Utilization_Ratio', t.FloatType(), True)
])

# Cálculo proporción de la gente que se va del banco
churned = ds.filter(f.col('Attrition_Flag') == "Attrited Customer").count()
total = ds.count()
churned / total

# Proporción de Nulls en las categorias 'Education_Level', 'Marital_Status' e 'Income_Category'
ds_nulos = ds.select([f.sum(f.col(c).isNull().cast('integer') / ds.count()).alias(c) for c in ds.columns])
ds_nulos.columns

# Se eliminan columnas con mas del 50% de nulos
def _drop_nulos(ds, max_nulo_prop=0.5):
    ds_nulos = ds.select([f.sum(f.col(c).isNull().cast('integer') / ds.count()).alias(c) 
                          for c in ds.columns])
    null_cols = [c for c in ds_nulos.columns if ds_nulos.select(c).first()[0] > max_nulo_prop 
                 and c not in PROTECTED_COLS]
    df = ds.drop(*null_cols)
    return ds

PROTECTED_COLS = ['Attrition_Flag']
ds = _drop_nulos(ds)

ds.columns
ds.show(20)




