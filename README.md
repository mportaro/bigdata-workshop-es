Seminario Intensivo de Tópicos Avanzados en Datos Complejos

# Trabajo Práctico

## Contenidos
* [Introducción al proyecto](#Introducción-al-proyecto)
* [Levantar el ambiente y acceso al script de Python](#levantar-el-ambiente-y-acceso-al-script-de-python)
* [Comprensión del Dataset](#comprensión-del-dataset)
* [ETFL](#etfl)
* [Acceso a la base de datos Postgres desde Superset](#acceso-a-la-base-de-datos-postgres-desde-superset)


## Introducción al proyecto
Para integrar el material que se ha cubierto durante el seminario, en este trabajo práctico haremos un proceso de ETFL (*Extract Transform Fit Load*) de un dataset público. Aunque el dataset es relativamente liviano (10,000 registros con 20 *features* aproximadamente) el proceso de ETFL va a ser realizado en un **entorno distribuido** para así complementar y reforzar los conocimientos adquiridos en la materia previa de grandes volúmenes de datos. Esto no solo es fundamental para empezar a entender el proceso subyacente de estos ambientes de *Big Data*, sino además es una buena oportunidad para incursionar en la sintáxis de `PySpark`.

Los datos trabajados van a ser luego cargados en una base de datos `Postgres` para finalmente ser levantados en `Superset` para su posterior análisis mediante gráficos y dashboards. Para todo esto utilizaremos `Docker` para conteinizar cada aplicación, aprovechando la ventaja de portabilidad que lo hace así independiente del sistema operativo de quienes lo ejecuten y todo el proceso pueda correr sin problemas en cualquier plataforma o incluso en *Cloud*.

Vale la pena notar que la data a procesar es ingestada una única vez, ya que los datos son estáticos. Por lo que no había una clara necesidad de utilizar Kafka. También se planteó si tenía sentido correr el script de manera automática desde Airflow, es decir, crear un proceso que dispare Python que corra el script en su container correspondiente. Pero en una segunda impresión esto no parecía algo muy práctico por ser una tarea trivial al ser una única tarea sin necesidad de orquestación.  


## Levantar el ambiente y acceso al script de Python

El ambiente que usaremos en este TP se encuentra en  
[mportaro workshop_TP GitHub repository](https://github.com/mportaro/bigdata-workshop-es.git). Este repositorio se puede clonar en el disco local para acompañar los varios pasos a continuación.

Notar que se quizo aprovechar parte de la estructura ya creada en https://github.com/MuttData/bigdata-workshop-es.git vista en clase y modificarla de acuerdo al nuevo objetivo, eliminando además carpetas y archivos que no se utilizarán aquí.

Como se mencionó en la introducción, vamos a levantar los siguientes containers:
* `master`, `worker1` y `worker2` ya que vamos a trabajar en un framework de **procesamiento distribuido**.
* `pyspark` donde correremos el **código de PySpark** `banking-churn.py` que se encuentra en https://github.com/mportaro/bigdata-workshop-es/tree/master/python para el proceso de ETFL.
* `postgres` para acceder a la base de datos que se persistirá.
* `superset` para la creación de un dashboard con el propósito de analizar la data en la BD.

Para eso adaptamos el archivo `bigdata-workshop-es/docker-compose.yml` para que esto quede reflejado aquí (los containers que no se usan quedan comentados[#] ).  

Corremos el comando de bash `docker-compose up` para levantar el ambiente en modo detachado e iniciar así los contenedores:

```bash
docker-compose --project-name wksp up -d
```  

Verifiquemos que efectivamente estén levantados mediante el siguiente comando:

```bash
docker ps --format "table '{{.Names}}'\t'{{.ID}}'\t'{{.Ports}}'"
```

![](./images/containers.png)  


Los contenedores están activos. A continuación veremos como correr el script de python para el proceso de **ETFL**.
  

## Comprensión del Dataset
El ejercicio de ETFL se basará en un [dataset](https://www.kaggle.com/datasets/sakshigoyal7/credit-card-customers?select=BankChurners.csv) disponible en la plataforma [Kaggle](https://www.kaggle.com "Kaggle's Homepage").  

El dataset en formato csv a utilizar contiene 10,127 registros y 22 columnas respecto al *churn* de clientes de un banco. El propósito es poder desarrollar un modelo que permita anticiparse a la decisión de un cliente de prescindir de los servicios del banco para irse a la competencia.
Los registros (o filas) corresponden a cada cliente que actualmente es, o en su momento fue, cliente del banco. Las columnas se dividen en dos grandes grupos:

* Attrition_Flag (i.e.: *churn*): es la variable dependiente. Clientes que se fueron vs los que siguen siendo clientes del banco.
* Datos demográficos: edad, género, cantidad de dependientes, nivel de educación, estado civil, nivel de ingreso.
* Datos transaccionales o específicos del banco: antigüedad del cliente, tipo de tarjeta de crédito, meses sin actividad, límite de crédito, balance de la cuenta, cantidad y monto de transacciones en el año, variación de la cantidad y el monto de las transacciones en el período Q4-Q1, grado de utilización de la tarjeta de crédito. 

Importante notar que el *churn* de los clientes es de aproximadamente un 16%, por lo que se trabaja con un dataset desbalanceado.  
  
Vamos a correr ahora el script *banking-churn.py* que se encuentra en el contenedor `pyspark` para el proceso de **ETFL**. El dataset *BankChurners.cvs* que alimenta al script se encuentra en la carpeta *dataset*/.  

Accedemos al container `pyspark` mediante el siguiente comando de bash:
```bash
 docker exec -it pyspark bash
 ```
 Nos ubicamos en la carpeta *python*/ en el directorio raiz y corremos el siguiente comando para que se ejecute el script que va a efectuar el ETFL:

```bash
python3 banking-churn.py
```
![](./images/script.png)  

A continuación veremos que es lo que hace el script de python.

## ETFL
1. **Extracción de los datos -** Como se mencionó anteriormente, el dataset en formato csv se extrajo de la página de Kaggle. La intención original era que el script de Python accediera directamente a la página para su debida extracción cada vez que se ejecutara. Lamentablemente no se encontró la manera de hacer el vínculo directo, por lo que, aunque no es lo ideal, se decidió bajar el archivo al disco local para que quede en el repositorio en *bigdata-workshop-es/dataset/BankChurners.csv*. De todos modos se menciona en el script de Python cuales serían los comandos a utilizar si se hubiera podido hacer el vínculo directo (ver lineas# 42-55).  

2. **Transformación -** Para la limpieza del dataset realizamos las siguientes operaciones:
    * Se deja que PySpark infiera el esquema de los datos mediante *InferSchema* y se corrobora que fueron correctamente casteados.
 ![](./images/inferschema.png)  

    * La últimas dos columnas del archivo original se eliminan al no ser de utilidad para el análisis.
    * Vemos la proporción de *Nulls* y si el porcentaje es muy alto (>50%) eliminamos completamente esa columna. No hubo ningún caso. Luego, para el caso de *features* **numéricos** completamos los *Nulls* con el valor de la mediana. Para el caso de las variables **categóricas** los valores faltantes aparecen como *'Unknown'*. Una opción sería completarlos con los valores que más se repiten, pero dada la cantidad no parece una buena idea. Otra solución sería completarlo de manera proporcional a la cantidad de valores categóricos, pero eso ya sería un poco más complejo. Por lo que se decidió dejarlos así y asegurarnos que no estamos agregando ruido para los casos donde los valores ya son conocidos.
    * Se analizan todos las columnas numéricas para ver sus respectivas desviaciones standards (<0.015) para eliminarlas en caso que así fuera por no agregar valor. Pero no hubo ningún caso.
    * Se grafican mediante la libreria de `matplotlib` todas las variables numéricas para ver si tienen una distribución relativamente normal. El algoritmo de regresión logística a utilizar responde mejor bajo estas condiciones. De este análisis se encontraron cinco *features* con distribución asimétrica, por lo que se decidió aplicar una transformación logarítmica para normalizarlas (ver figura a continuación).
    * Se agrega una columna de training que luego se usará en el *fitteo* del modelo. La relación es 80% training y 20% testing.  


    ![](./images/matplotlib.png)  

3. **Fit -** Para predecir el label *Attrition* (i.e.: *churn*) en este ejercicio se usará un modelo paramétrico de regresión logistica de PySpark, ya que se trata de un problema de clasificación binaria (el cliente va a quedarse o irse). En principio un modelo paramétrico que ajuste bien sin *overfitting* es ideal ya que es más fácil de interpretar. Luego se aplica la función de *OneHotEncoding* a las variables categóricas ya que el modelador tiene que recibir variables continuas para un funcionamiento correcto.

Las métricas resultantes para evaluar el desempeño del modelo dan una Precisión del 79.3% y una Cobertura del 64.5% para el Label correspondiente a Attrition. El Accuracy, en cambio, es del 91.4%. Y el Evaluador arroja un desepeño del 93.1%.

![](./images/resultado_modelo.png)


Un punto a tener en cuenta para un próximo análisis es que el modelo en este caso no trabaja con *validación* durante la etapa de training. También sería importante compensar el desbalance del dataset ya que la clase minoritaria (clientes con *Attrition*) representa solo un 16% sobre el total de registros. Por eso es que *Precisión* y *Cobertura* son tan buenos para los clientes **sin** el flag de *Attrition* (0.0) y no tan buenos para los que tienen al flag de *Attrition* (1.0).

4. **Load -** Una vez ya limpiados y transformados los datos (pero previo al uso del *OneHotEncoder* arriba descripto) se los persiste en una base de datos `Postgres`. Esto nos va a permitir, entre otras cosas, acceder a este nuevo dataset desde herramientas de visualización, tales como `Superset` para diversos tipos de análisis, como veremos en el apartado siguiente.

## Acceso a la base de datos Postgres desde Superset
Como se comentó más arriba, las datos se han persistido en una BD `Postgres`. Desde una terminal de `bash` podemos acceder al **container** correspondiente via

```bash
docker exec -it postgres bash
```
Una vez dentro del container corremos el siguiente comando para poder ver la tabla que se creó.
```Postgres
psql -U workshop workshop
workshop=# \d
```
Obteniendo así la confirmación que la tabla **churn** fue creada. 

![](./images/postgres_table.png)

Desde aquí mismo podríamos correr una query para ver el contenido de la tabla. Veamos algunas columnas:

```postgres
SELECT "CLIENTNUM","Attrition_Flag", "Education_Level","Customer_Age", "Gender" FROM churn limit 5;
```
![](./images/postgres_table_2.png)  

Salimos del container con Ctrl+P y Ctrl+Q.

Accedemos a Superset desde http://localhost:8088/  
![](./images/localhost8088.png)

Desde `Superset` creamos el vínculo a la base de datos Postgres y levantamos la tabla **churn**. Usamos las siguientes credenciales:  

Nombre BD: `workshop`  
User ID: `workshop`  
Password: `w0rkzh0p`   
Por lo que el SQLALCHEMY URI es `postgresql://workshop:w0rksh0p@postgres/workshop`  
Nombre de la Tabla: `churn`  

![](./images/churn_superset.png)  

Creamos un dashboard donde vamos agregando distintos gráficos a analizar.  

![](./images/dashboard.png)  

Veamos a continuación unos ejemplos de los gráficos incluidos aquí:

En la Fig.1 vemos que los clientes graduados del secundario o de la universidad forman la mitad de los clientes. Hay una gran proporción de clientes sin estudios o de los que no se tienen información.

![](./images/nivel-de-educacion-2022-10-31T22-10-41.763Z.jpg)  
*Fig.1 - Nivel de Educación*


En la próxima figura apreciamos que **no hay** una clara diferencia proporcional entre la cantidad entre hombres y la de mujeres respecto de sus respectivos niveles de estudio.



![](./images/nivel-de-educacion-por-sexo-2022-10-31T22-10-22.250Z.jpg)  
*Fig.2 - Nivel de Educación vs Sexo*


El histograma a continuación deja en claro que hay una gran proporción de solteros a la largo de todas las edades, no solo con los más jóvenes, siendo el segmento 45-55 años el más significativo. Curiosamente la proporción de casados más allá de los 55 años baja dramáticamente.

![](./images/nivel-de-ingreso-por-edad-2022-10-31T22-57-23.155Z.jpg)  
*Fig.3 - Histograma Edad vs Estado Civil*


Es interesante ver en la tabla de la Fig.4 que los clientes que dejan el banco en mayor proporción son los graduados de la universidad, casi un 5%, seguidos por los que tienen estudios secundarios completos. Tener en cuenta que hay un 2.5% de *Unknown*, por lo que aquí habría que indagar un poco más para conseguir esa información.

![](./images/attrition-por-nivel-de-estudio-2022-10-31T22-10-27.059Z.jpg)  
*Fig.4 - Tabla Attrition vs Nivel de Estudio*  

Y finalmente, exportamos el Dashboard completo con el dataset y todas los gráficos incluidos. El archivo .zip se encuentra en: 
`./superset/dashboard_export_20221031T233517.zip`  

Este es el archivo que deberá importarse luego desde Superset para poder acceder a las visualizaciones.

Una vez concluido el análisis se puede terminar la sesión de Docker:

```bash
docker-compose --project-name wksp down
```  

