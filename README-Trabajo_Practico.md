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
Para integrar el material que se ha cubierto durante el seminario, en este trabajo práctico haremos un proceso de ETFL (Extract Transform Fit Load) de un dataset público. Aunque el dataset es relativamente liviano (10,000 registros con 20 variables aproximadamente) el proceso de ETFL va a ser realizado en un entorno distribuidopara así complementar y reforzar los conocimientos adquiridos en la materia previa de grandes volúmenes de datos. Esto no solo es fundamental para entender el proceso subyacente de estos ambientes de *Big Data*, sino además es una buena oportunidad para incursionar en la sintáxis de `PySpark`.

Los datos trabajados va a ser luego cargados en una base de datos `Postgres` para finalmente ser levantados en `Superset` para su posterior análisis mediante gráficos y dashboards. Para todo esto utilizaremos `Docker` para conteinizar cada aplicación, aprovechando la ventaja de portabilidad que lo hace asi independiente del sistema operativo de quienes lo ejecuten y todo el proceso pueda así correr sin problemas en cualquier plataforma o incluso en Cloud.

Vale la pena notar que la data a procesar es ingestada una única vez, ya que los datos son estáticos. Por lo que no habia una clara ventaja en utilizar Airflow. También se planteó si tenia sentido correr el script de PySpark de manera automática, es decir, crear un proceso que dispare Python que corra el script en su container correspondiente. Pero en una segunda impresión esto no parecía algo muy práctico por ser una tarea trivial al ser una única tarea. Quizás sería más práctico y eficiente que el script se disparase automáticamente tan pronto se levantara el container.


## Levantar el ambiente



## Dataset
El ejercicio de ETFL se basará en un [dataset](https://www.kaggle.com/datasets/sakshigoyal7/credit-card-customers?select=BankChurners.csv) disponible en la plataforma [Kaggle](https://www.kaggle.com "Kaggle's Homepage").

El dataset en formato csv ha utilizar contiene 10,127 registros y 22 columnas respecto al churn de clientes de un banco El propósito es poder desarrollar un modelo que permita anticiparse a la decisión de un cliente de prescindir de los servicios del banco para irse a la competencia.
Los registros (o filas) corresponden a cada cliente que actualmente es, o en su momento fue cliente del banco. Las columnas se dividen en dos grandes grupos:

* Datos demográficos: edad, género, cantidad de dependientes, nivel de educación, estado civil, nivel de ingreso.
* Datos transaccionales o especificos del banco: antigüedad del cliente, tipo de tarjeta de crédito, meses sin actividad, limite de crédito, balance de la cuenta, cantidad y monto de transacciones en el año, variación de la cantidad y el monto de las transacciones en el período Q4-Q1, grado de utilización de la tarjeta de crédito. 

La últimas dos columnas del archivo original se eliminan al no ser de utilidad para el análisis.

Importante notar que el churn de los clientes es de aproximadamente un 16%, por lo que se trabaja con un dataset desbalanceado.

## ETFL
* **Extracción de los datos:** como se mencionó anteriormente, el dataset en formato csv se extrajo de la página de Kaggle. La intención original era que el script de python accediera directamente a la página para su debida extracción cada vez que se ejecutara. Lamentablemente no se encontró la manera de hacer el vínculo directo, por lo que, aunque no es lo ideal, se decidió bajar el archivo a la máquina local para que quede en el repositorio en  *bigdata-workshop-es/dataset/BankChurners.csv*. De todos modos se menciona en el script de Python cuales serían los comandos a utilizar si se hubiera podido hacer el vínculo directo.
* **Transformación:**

* **Fit:**

*  **Load:**



## Ver base de datos en Superset





## Conclusiones


