Seminario Intensivo de Tópicos Avanzados en Datos Complejos

# Trabajo Práctico

## Contenidos
* [Introducción al proyecto](#Introducción-al-proyecto)
* [Dataset](#Dataset)
* [Levantar el ambiente](#levantar-el-ambiente)
* [ETFL](#etfl)
* [Ver base de datos en Superset](#ver-base-de-datos-en-superset)
* [Conclusiones](#conclusiones)


## Introducción al proyecto
Para integrar el material que se ha cubierto durante el seminario, en este trabajo práctico haremos un proceso de ETFL (Extract Transform Fit Load) de un dataset público. Aunque el dataset es relativamente liviano (10,000 registros con 20 variables) el proceso de ETFL va a ser realizado en un entorno distribuidopara así complementar y reforzar los conocimientos adquiridos en la materia previa de grandes volúmenes de datos. Esto no solo es fundamental para entender el proceso subyacente de estos ambientes de *Big Data*, sino además es una buena oportunidad para incursionar en la sintáxis de `PySpark`.

Los datos trabajados va a ser luego cargados en una base de datos `Postgres` para finalmente ser levantados en `Superset` para su posterior análisis mediante gráficos y dashboards. Para todo esto utilizaremos `Docker` para conteinizar cada aplicación, aprovechando la ventaja de portabilidad que lo hace asi independiente del sistema operativo de quienes lo ejecuten y todo el proceso pueda así correr sin problemas en cualquier plataforma o incluso en Cloud.

Vale la pena notar que la data a procesar es ingestada una única vez, ya que los datos son estáticos. Por lo que no habia una clara ventaja en utilizar Airflow. También se planteó si tenia sentido correr el script de PySpark de manera automática, es decir, crear un proceso que dispare Python que corra el script en su container correspondiente. Pero en una segunda impresión esto no parecía algo muy práctico por ser una tarea trivial al ser una única tarea. Quizás sería más práctico y eficiente que el script se disparase automáticamente tan pronto se levantara el container.

## Dataset
El ejercicio de ETFL se basará en un [dataset](https://www.kaggle.com/datasets/sakshigoyal7/credit-card-customers?select=BankChurners.csv) disponible en la plataforma [Kaggle](https://www.kaggle.com "Kaggle's Homepage").



## Levantar el ambiente




## ETFL
Correr el código Python




## Ver base de datos en Superset





## Conclusiones


