# El objetivo es desarrollar un modelo que ayude a entender las razones del churn de los clientes de los servicios de un banco.

import findspark

findspark.init()

import pyspark.sql.types as t
import pyspark.sql.functions as f
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,when
from pyspark.sql.functions import rand, when
from pathlib import Path
from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import OneHotEncoderEstimator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator


# Vamos a grabar los datos en una BD Postgres
findspark.add_jars('/app/postgresql-42.1.4.jar')

# Inicializamos la sesión de Spark para procesar los datos de manera distribuida
spark = (
    SparkSession.builder
    .appName("Banking Clients Churn")
    .config("spark.driver.memory", "512m")
    .config("spark.driver.cores", "1")
    .config("spark.executor.memory", "512m")
    .config("spark.executor.cores", "2")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

""" NOTA: Extracción online - el link de kaggle no me permite llegar directamente via url al archivo .csv desde el código. 
En caso que pudiera hubiera usado el siguiente código, donde aprovecharia para hacer un cache también.

def data_online(url, refresh_cache=False):
    cache_fn = Path('/dataset/BankChurners_cache.csv')
    if not cache_fn.exists() or refresh_cache:
        print("Extrayendo el dataset")
        ds = pd.read_csv(url)
        ds.to_csv(cache_fn, index=False)
    print("Using cache")
    ds = spark.read.csv(str(cache_fn), header=True)
    return ds

url = '/kaggle/input/credit-card-customers/BankChurners.csv' """

# Extracción desde directorio local. Se deja que PySpark infiera el esquema de los datos.
ds = spark.read.option("header", True).option("inferSchema", True).csv('/dataset/BankChurners.csv')

ds.printSchema()
ds.count()

# Las ultimas dos columnas del dataset (Naive Bayes Classification) son eliminadas según recomendación.
ds=ds.drop('Naive_Bayes_Classifier_Attrition_Flag_Card_Category_Contacts_Count_12_mon_Dependent_count_Education_Level_Months_Inactive_12_mon_1')
ds=ds.drop('Naive_Bayes_Classifier_Attrition_Flag_Card_Category_Contacts_Count_12_mon_Dependent_count_Education_Level_Months_Inactive_12_mon_2')

# Se reemplaza 'Unknown' for null
##ds=ds.select([when(col(c)=="Unknown",None).otherwise(col(c)).alias(c) for c in ds.columns])

# Se reemplaza 'Existing Customer' por 0 y 'Attrited Customer' por 1 y se lo castea como entero
ds = ds.withColumn('Attrition_Flag', regexp_replace('Attrition_Flag', 'Existing Customer', '0'))
ds = ds.withColumn('Attrition_Flag', regexp_replace('Attrition_Flag', 'Attrited Customer', '1'))
ds = ds.withColumn('Attrition_Flag', ds['Attrition_Flag'].cast(IntegerType()))

ds.show(10)

# Se determina la proporción de la gente que se va del banco
churned = ds.filter(f.col('Attrition_Flag') == 1).count()
total = ds.count()
churned / total

# Proporción de Nulls en las categorias 'Education_Level', 'Marital_Status' e 'Income_Category'
##ds_nulos = ds.select([f.sum(f.col(c).isNull().cast('integer') / ds.count()).alias(c) for c in ds.columns])
##ds_nulos.columns

# Se eliminan columnas con mas del 50% de nulos
def _drop_nulos(ds, max_nulo_prop=0.5):
    ds_nulos = ds.select([f.sum(f.col(c).isNull().cast('integer') / ds.count()).alias(c) 
                          for c in ds.columns])
    null_cols = [c for c in ds_nulos.columns if ds_nulos.select(c).first()[0] > max_nulo_prop 
                 and c not in PROTECTED_COLS]
    ds = ds.drop(*null_cols)
    return ds

# Training / Testing set (80% / 20%)
ds = ds.withColumn('train', when(rand(seed=3618) > 0.20, True).otherwise(False))

# Columnas a proteger:
PROTECTED_COLS = ['Attrition_Flag', 'train']
ds = _drop_nulos(ds)

# Se eliminan columnas con baja desviación standard (<0.015)
ds.toPandas().std()

def _drop_std(ds, min_std_dev=0.015):
    num_cols = [c for c,dtype in ds.dtypes if dtype.startswith(('int', 'double'))]
    ds_std = ds.select([f.stddev(f.col(c)).alias(c) for c in num_cols])
    low_variance_cols = [c for c in ds_std.columns if ds_std.select(c).first()[0] < min_std_dev 
                         and c not in PROTECTED_COLS]
    ds.drop(*low_variance_cols)
    return ds

ds = _drop_std(ds)

# Se completan los Nulls con la mediana en caso de números.
# En el caso de las categorias SE VAN A DEJAR los valores 'Unknown', aunque lo ideal seria competarlos de manera proporcional.
# Otra alternativa, lejos de lo ideal, seria completarlo con el valor más común (ver código a continuación mencionado.)

def _get_typed_cols(ds, col_type='cat'):
    assert col_type in ('cat', 'num')
    dtypes = ('int', 'double') if col_type == 'num' else ('string')
    typed_cols = [c for c,dtype in ds.dtypes if dtype.startswith(dtypes) 
                  and c not in PROTECTED_COLS]
    return typed_cols

num_cols = _get_typed_cols(ds, col_type='num')

def _fill_nulls(ds):
    for t in ['num', 'cat']:
        cols = _get_typed_cols(ds, col_type=t)
        for c in cols:
            if t == 'num':
                median_val = ds.approxQuantile(c, [0.5], 0)[0]
                ds = ds.fillna(median_val, subset=[c])
            else:
                val_counts = ds.filter(f.col(c).isNotNull()).select(c).groupBy(c).count().orderBy(f.desc('count'))
                common_val = val_counts.select(c).first()[0]
                ds = ds.fillna(common_val, subset=[c])
    return ds

ds = _fill_nulls(ds)

# Salvándolo a Postgres
ds \
    .write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres/workshop") \
    .option("dbtable", "workshop.churn") \
    .option("user", "workshop") \
    .option("password", "w0rkzh0p") \
    .option("driver", "org.postgresql.Driver") \
    .mode('overwrite') \
    .save()

# Se grafican las variables numéricas para ver su distribución
plt.figure(figsize = (18,28))
nrow = 3
ncol = 5
i = 1
for c in num_cols:
    plt.subplot(nrow, ncol, i)
    plt.grid()
    plt.title(c) 
    sns.distplot(ds.select(c).toPandas(), color='m')
    i += 1
plt.tight_layout()
plt.legend(loc=4, bbox_to_anchor=(2.0, 0.5), prop={'size': 18})
plt.show()

# De estos histogramas se determina que hay cinco variables con distribución sesgada hacia la izquierda
# Se le aplicara la transformación logarítmica

@f.udf('double')
def np_log(x):
    if x is None:
        return 0
    return float(np.log(x + 1)) 

ds = ds.withColumn('log_Credit_Limit', np_log(ds.Credit_Limit))
ds = ds.withColumn('log_Avg_Open_To_Buy', np_log(ds.Avg_Open_To_Buy))
ds = ds.withColumn('log_Total_Trans_Amt', np_log(ds.Total_Trans_Amt))
ds = ds.withColumn('log_Total_Ct_Chng_Q4_Q1', np_log(ds.Total_Ct_Chng_Q4_Q1))
ds = ds.withColumn('log_Avg_Utilization_Ratio', np_log(ds.Avg_Utilization_Ratio))

ds = ds.drop('Credit_Limit')
ds = ds.drop('Avg_Open_To_Buy')
ds = ds.drop('Total_Trans_Amt')
ds = ds.drop('Total_Ct_Chng_Q4_Q1')
ds = ds.drop('Avg_Utilization_Ratio')

ds.printSchema()

# Fitting
num_cols = _get_typed_cols(ds, col_type='num')
cat_cols = _get_typed_cols(ds, col_type='cat')

def _encode_categorical(ds):
    cat_cols = _get_typed_cols(ds, col_type='cat')
    encoded_cols = []
    for cat in cat_cols:
        cat_suff = f'{cat}_num'
        encoded_cols.append(cat_suff)
        if cat_suff not in ds.columns:
            indexer = StringIndexer(inputCol=cat, outputCol=cat_suff).fit(ds)
            ds = indexer.transform(ds)
    return ds, encoded_cols

ds, encoded_cols = _encode_categorical(ds)

feature_cols = num_cols + encoded_cols

ohe_cols = [f'{c}_vec' for c in encoded_cols]
encoder = OneHotEncoderEstimator(inputCols=encoded_cols, outputCols=ohe_cols)
ohem = encoder.fit(ds)
ds = ohem.transform(ds)

feature_cols = num_cols + ohe_cols

assembler = VectorAssembler(inputCols=feature_cols, outputCol='features')
ds = assembler.transform(ds)
ds.select('features').show(5)

train_data = ds.filter(f.col('train') == True).select('Attrition_Flag', 'features')
test_data = ds.filter(f.col('train') == False).select('Attrition_Flag', 'features')

# Usamos el modelo de Regresión Logística de PySpark
lr = LogisticRegression(labelCol='Attrition_Flag', featuresCol='features')
lrm = lr.fit(train_data)

# Evaluación del Modelo

print("MÉTRICAS DE EVALUACIÓN")
print("---------------------- \n")

pred_ds = lrm.transform(test_data)
print(f"Canitdad de iteraciones                         {lrm.summary.totalIterations}\n")
print(f"Accuracy                                        {lrm.summary.accuracy:.1%}")
print(f"Área debajo de la curva ROC                     {lrm.summary.areaUnderROC:.1%}\n")

evaluator = BinaryClassificationEvaluator(labelCol='Attrition_Flag')
print(f"Evaluación del modelo de regresión logística    {evaluator.evaluate(pred_ds):.1%}\n")


