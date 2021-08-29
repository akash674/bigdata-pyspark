dflr=spark.read.format("csv").option("header","true").option("inferSchema","true").load("/FileStore/tables/cruise_ship_info-3.csv")
dflr.show()

"""
+-----------+-----------+---+------------------+----------+------+------+-----------------+----+
|  Ship_name|Cruise_line|Age|           Tonnage|passengers|length|cabins|passenger_density|crew|
+-----------+-----------+---+------------------+----------+------+------+-----------------+----+
|    Journey|    Azamara|  6|30.276999999999997|      6.94|  5.94|  3.55|            42.64|3.55|
|      Quest|    Azamara|  6|30.276999999999997|      6.94|  5.94|  3.55|            42.64|3.55|
|Celebration|   Carnival| 26|            47.262|     14.86|  7.22|  7.43|             31.8| 6.7|
|   Conquest|   Carnival| 11|             110.0|     29.74|  9.53| 14.88|            36.99|19.1|
|    Destiny|   Carnival| 17|           101.353|     26.42|  8.92| 13.21|            38.36|10.0|
|    Ecstasy|   Carnival| 22|            70.367|     20.52|  8.55|  10.2|            34.29| 9.2|
|    Elation|   Carnival| 15|            70.367|     20.52|  8.55|  10.2|            34.29| 9.2|
|    Fantasy|   Carnival| 23|            70.367|     20.56|  8.55| 10.22|            34.23| 9.2|
|Fascination|   Carnival| 19|            70.367|     20.52|  8.55|  10.2|            34.29| 9.2|
|    Freedom|   Carnival|  6|110.23899999999999|      37.0|  9.51| 14.87|            29.79|11.5|
|      Glory|   Carnival| 10|             110.0|     29.74|  9.51| 14.87|            36.99|11.6|
|    Holiday|   Carnival| 28|            46.052|     14.52|  7.27|  7.26|            31.72| 6.6|
|Imagination|   Carnival| 18|            70.367|     20.52|  8.55|  10.2|            34.29| 9.2|
|Inspiration|   Carnival| 17|            70.367|     20.52|  8.55|  10.2|            34.29| 9.2|
|     Legend|   Carnival| 11|              86.0|     21.24|  9.63| 10.62|            40.49| 9.3|
|   Liberty*|   Carnival|  8|             110.0|     29.74|  9.51| 14.87|            36.99|11.6|
|    Miracle|   Carnival|  9|              88.5|     21.24|  9.63| 10.62|            41.67|10.3|
|   Paradise|   Carnival| 15|            70.367|     20.52|  8.55|  10.2|            34.29| 9.2|
|      Pride|   Carnival| 12|              88.5|     21.24|  9.63| 11.62|            41.67| 9.3|
|  Sensation|   Carnival| 20|            70.367|     20.52|  8.55|  10.2|            34.29| 9.2|
+-----------+-----------+---+------------------+----------+------+------+-----------------+----+
only showing top 20 rows
"""
dflr.printSchema()

"""
root
 |-- Ship_name: string (nullable = true)
 |-- Cruise_line: string (nullable = true)
 |-- Age: integer (nullable = true)
 |-- Tonnage: double (nullable = true)
 |-- passengers: double (nullable = true)
 |-- length: double (nullable = true)
 |-- cabins: double (nullable = true)
 |-- passenger_density: double (nullable = true)
 |-- crew: double (nullable = true)
"""

dflr.printSchema

#Out[90]: <bound method DataFrame.printSchema of DataFrame[Ship_name: string, Cruise_line: string, Age: int, Tonnage: double, passengers: double, length: double, cabins: double, passenger_density: double, crew: double]>
dflr.head(5)
"""
Out[91]: [Row(Ship_name='Journey', Cruise_line='Azamara', Age=6, Tonnage=30.276999999999997, passengers=6.94, length=5.94, cabins=3.55, passenger_density=42.64, crew=3.55),
 Row(Ship_name='Quest', Cruise_line='Azamara', Age=6, Tonnage=30.276999999999997, passengers=6.94, length=5.94, cabins=3.55, passenger_density=42.64, crew=3.55),
 Row(Ship_name='Celebration', Cruise_line='Carnival', Age=26, Tonnage=47.262, passengers=14.86, length=7.22, cabins=7.43, passenger_density=31.8, crew=6.7),
 Row(Ship_name='Conquest', Cruise_line='Carnival', Age=11, Tonnage=110.0, passengers=29.74, length=9.53, cabins=14.88, passenger_density=36.99, crew=19.1),
 Row(Ship_name='Destiny', Cruise_line='Carnival', Age=17, Tonnage=101.353, passengers=26.42, length=8.92, cabins=13.21, passenger_density=38.36, crew=10.0)]
"""
dflr.groupBy('Cruise_line').count().show()
"""
+-----------------+-----+
|      Cruise_line|count|
+-----------------+-----+
|            Costa|   11|
|              P&O|    6|
|           Cunard|    3|
|Regent_Seven_Seas|    5|
|              MSC|    8|
|         Carnival|   22|
|          Crystal|    2|
|           Orient|    1|
|         Princess|   17|
|        Silversea|    4|
|         Seabourn|    3|
| Holland_American|   14|
|         Windstar|    3|
|           Disney|    2|
|        Norwegian|   13|
|          Oceania|    3|
|          Azamara|    2|
|        Celebrity|   10|
|             Star|    6|
|  Royal_Caribbean|   23|
+-----------------+-----+
"""
from pyspark.ml.feature import StringIndexer
indexer=StringIndexer(inputCol='Cruise_line',outputCol='cruise_cat')
indexed=indexer.fit(dflr).transform(dflr)
indexed.head(5)
"""
Out[96]: [Row(Ship_name='Journey', Cruise_line='Azamara', Age=6, Tonnage=30.276999999999997, passengers=6.94, length=5.94, cabins=3.55, passenger_density=42.64, crew=3.55, cruise_cat=16.0),
 Row(Ship_name='Quest', Cruise_line='Azamara', Age=6, Tonnage=30.276999999999997, passengers=6.94, length=5.94, cabins=3.55, passenger_density=42.64, crew=3.55, cruise_cat=16.0),
 Row(Ship_name='Celebration', Cruise_line='Carnival', Age=26, Tonnage=47.262, passengers=14.86, length=7.22, cabins=7.43, passenger_density=31.8, crew=6.7, cruise_cat=1.0),
 Row(Ship_name='Conquest', Cruise_line='Carnival', Age=11, Tonnage=110.0, passengers=29.74, length=9.53, cabins=14.88, passenger_density=36.99, crew=19.1, cruise_cat=1.0),
 Row(Ship_name='Destiny', Cruise_line='Carnival', Age=17, Tonnage=101.353, passengers=26.42, length=8.92, cabins=13.21, passenger_density=38.36, crew=10.0, cruise_cat=1.0)]
"""
from pyspark.ml.linalg import Vector
from pyspark.ml.feature import VectorAssembler
indexed.columns
"""Out[99]: ['Ship_name',
 'Cruise_line',
 'Age',
 'Tonnage',
 'passengers',
 'length',
 'cabins',
 'passenger_density',
 'crew',
 'cruise_cat']"""

assembler=VectorAssembler(inputCols=['Age',
 'Tonnage',
 'passengers',
 'length',
 'cabins',
 'passenger_density',
 'cruise_cat'],outputCol='features')
output=assembler.transform(indexed)
final_data=output.select(['features','crew'])
train_data,test_data=final_data.randomSplit([0.7,0.3])
train_data.describe().show()

"""
+-------+-----------------+
|summary|             crew|
+-------+-----------------+
|  count|              119|
|   mean| 7.49638655462186|
| stddev|3.507724099438806|
|    min|             0.59|
|    max|             21.0|
+-------+-----------------+

test_data.describe().show()
+-------+------------------+
|summary|              crew|
+-------+------------------+
|  count|                39|
|   mean|  8.70282051282051|
| stddev|3.3734411204662154|
|    min|              2.87|
|    max|              19.1|
+-------+------------------+
"""
from pyspark.ml.regression import LinearRegression
ship_lr=LinearRegression(labelCol='crew')
trai_ship_model=ship_lr.fit(train_data)
ship_result=trai_ship_model.evaluate(test_data)
ship_result.rootMeanSquaredError

#Out[110]: 1.3216524174375341

train_data.describe().show()

"""
+-------+-----------------+
|summary|             crew|
+-------+-----------------+
|  count|              119|
|   mean| 7.49638655462186|
| stddev|3.507724099438806|
|    min|             0.59|
|    max|             21.0|
+-------+-----------------+
"""
ship_result.r2
#Out[112]: 0.8424678316639028

ship_result.meanSquaredError
#Out[113]: 1.7467651125184778

ship_result.meanAbsoluteError
#Out[114]: 0.6965944124380937

from pyspark.sql.functions import corr
dflr.select(corr('crew','passengers')).show()

"""
+----------------------+
|corr(crew, passengers)|
+----------------------+
|    0.9152341306065384|
+----------------------+
"""

dflr.select(corr('crew','cabins')).show()
"""
+------------------+
|corr(crew, cabins)|
+------------------+
|0.9508226063578497|
+------------------+
"""