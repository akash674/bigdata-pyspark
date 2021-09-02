#create the spark data frame and load the data

df=spark.read.format("csv").option("header","true").option("inferSchema","true").load("/FileStore/tables/seeds_dataset.csv")
df.show()


"""
+-----+---------+-----------+------------------+------------------+---------------------+------------------+
| area|perimeter|compactness|  length_of_kernel|   width_of_kernel|asymmetry_coefficient|  length_of_groove|
+-----+---------+-----------+------------------+------------------+---------------------+------------------+
|15.26|    14.84|      0.871|             5.763|             3.312|                2.221|              5.22|
|14.88|    14.57|     0.8811| 5.553999999999999|             3.333|                1.018|             4.956|
|14.29|    14.09|      0.905|             5.291|3.3369999999999997|                2.699|             4.825|
|13.84|    13.94|     0.8955|             5.324|3.3789999999999996|                2.259|             4.805|
|16.14|    14.99|     0.9034|5.6579999999999995|             3.562|                1.355|             5.175|
|14.38|    14.21|     0.8951|             5.386|             3.312|   2.4619999999999997|             4.956|
|14.69|    14.49|     0.8799|             5.563|             3.259|   3.5860000000000003| 5.218999999999999|
|14.11|     14.1|     0.8911|              5.42|             3.302|                  2.7|               5.0|
|16.63|    15.46|     0.8747|             6.053|             3.465|                 2.04| 5.877000000000001|
|16.44|    15.25|      0.888|5.8839999999999995|             3.505|                1.969|5.5329999999999995|
|15.26|    14.85|     0.8696|5.7139999999999995|             3.242|                4.543|             5.314|
|14.03|    14.16|     0.8796|             5.438|             3.201|   1.7169999999999999|             5.001|
|13.89|    14.02|      0.888|             5.439|             3.199|                3.986|             4.738|
|13.78|    14.06|     0.8759|             5.479|             3.156|                3.136|             4.872|
|13.74|    14.05|     0.8744|             5.482|             3.114|                2.932|             4.825|
|14.59|    14.28|     0.8993|             5.351|             3.333|                4.185| 4.781000000000001|
|13.99|    13.83|     0.9183|             5.119|             3.383|                5.234| 4.781000000000001|
|15.69|    14.75|     0.9058|             5.527|             3.514|                1.599|             5.046|
| 14.7|    14.21|     0.9153|             5.205|             3.466|                1.767|             4.649|
|12.72|    13.57|     0.8686|             5.226|             3.049|                4.102|             4.914|
+-----+---------+-----------+------------------+------------------+---------------------+------------------+
only showing top 20 rows
"""

#describing the data
df.describe().show()

"""
+-------+------------------+------------------+--------------------+-------------------+------------------+---------------------+-------------------+
|summary|              area|         perimeter|         compactness|   length_of_kernel|   width_of_kernel|asymmetry_coefficient|   length_of_groove|
+-------+------------------+------------------+--------------------+-------------------+------------------+---------------------+-------------------+
|  count|               210|               210|                 210|                210|               210|                  210|                210|
|   mean|14.847523809523816|14.559285714285718|  0.8709985714285714|  5.628533333333335| 3.258604761904762|   3.7001999999999997|  5.408071428571429|
| stddev|2.9096994306873647|1.3059587265640225|0.023629416583846364|0.44306347772644983|0.3777144449065867|   1.5035589702547392|0.49148049910240543|
|    min|             10.59|             12.41|              0.8081|              4.899|              2.63|                0.765|              4.519|
|    max|             21.18|             17.25|              0.9183|              6.675|             4.033|                8.456|               6.55|
+-------+------------------+------------------+--------------------+-------------------+------------------+---------------------+-------------------+
"""

#displaying the schema

df.printSchema()

"""
root
 |-- area: double (nullable = true)
 |-- perimeter: double (nullable = true)
 |-- compactness: double (nullable = true)
 |-- length_of_kernel: double (nullable = true)
 |-- width_of_kernel: double (nullable = true)
 |-- asymmetry_coefficient: double (nullable = true)
 |-- length_of_groove: double (nullable = true)
"""

# displaying only row of the data
df.head(1)

#Out[36]: [Row(area=15.26, perimeter=14.84, compactness=0.871, length_of_kernel=5.763, width_of_kernel=3.312, asymmetry_coefficient=2.221, length_of_groove=5.22)]

#displaying the columns name
df.columns

"""
Out[37]: ['area',
 'perimeter',
 'compactness',
 'length_of_kernel',
 'width_of_kernel',
 'asymmetry_coefficient',
 'length_of_groove']

 """

#creating the vector Assembler and tranforming the data
assembler1=VectorAssembler(inputCols=df.columns,outputCol='features')
final_data1=assembler1.transform(df)

#creating the feature column
scaler1=StandardScaler(inputCol='features',outputCol='scalaerfeatures')
scaler_model1=scaler1.fit(final_data1)
cluster_final_data1=scaler_model1.transform(final_data1)

#checking the schema of final data variable

cluster_final_data1.printSchema()

"""
root
 |-- area: double (nullable = true)
 |-- perimeter: double (nullable = true)
 |-- compactness: double (nullable = true)
 |-- length_of_kernel: double (nullable = true)
 |-- width_of_kernel: double (nullable = true)
 |-- asymmetry_coefficient: double (nullable = true)
 |-- length_of_groove: double (nullable = true)
 |-- features: vector (nullable = true)
 |-- scalaerfeatures: vector (nullable = true)
"""

#using the KMeans algorithms
kmeans1=KMeans(featuresCol='features',k=3)
model=kmeans1.fit(cluster_final_data1)
model.transform(final_data1).show()

"""
+-----+---------+-----------+------------------+------------------+---------------------+------------------+--------------------+----------+
| area|perimeter|compactness|  length_of_kernel|   width_of_kernel|asymmetry_coefficient|  length_of_groove|            features|prediction|
+-----+---------+-----------+------------------+------------------+---------------------+------------------+--------------------+----------+
|15.26|    14.84|      0.871|             5.763|             3.312|                2.221|              5.22|[15.26,14.84,0.87...|         0|
|14.88|    14.57|     0.8811| 5.553999999999999|             3.333|                1.018|             4.956|[14.88,14.57,0.88...|         0|
|14.29|    14.09|      0.905|             5.291|3.3369999999999997|                2.699|             4.825|[14.29,14.09,0.90...|         0|
|13.84|    13.94|     0.8955|             5.324|3.3789999999999996|                2.259|             4.805|[13.84,13.94,0.89...|         0|
|16.14|    14.99|     0.9034|5.6579999999999995|             3.562|                1.355|             5.175|[16.14,14.99,0.90...|         0|
|14.38|    14.21|     0.8951|             5.386|             3.312|   2.4619999999999997|             4.956|[14.38,14.21,0.89...|         0|
|14.69|    14.49|     0.8799|             5.563|             3.259|   3.5860000000000003| 5.218999999999999|[14.69,14.49,0.87...|         0|
|14.11|     14.1|     0.8911|              5.42|             3.302|                  2.7|               5.0|[14.11,14.1,0.891...|         0|
|16.63|    15.46|     0.8747|             6.053|             3.465|                 2.04| 5.877000000000001|[16.63,15.46,0.87...|         0|
|16.44|    15.25|      0.888|5.8839999999999995|             3.505|                1.969|5.5329999999999995|[16.44,15.25,0.88...|         0|
|15.26|    14.85|     0.8696|5.7139999999999995|             3.242|                4.543|             5.314|[15.26,14.85,0.86...|         0|
|14.03|    14.16|     0.8796|             5.438|             3.201|   1.7169999999999999|             5.001|[14.03,14.16,0.87...|         0|
|13.89|    14.02|      0.888|             5.439|             3.199|                3.986|             4.738|[13.89,14.02,0.88...|         0|
|13.78|    14.06|     0.8759|             5.479|             3.156|                3.136|             4.872|[13.78,14.06,0.87...|         0|
|13.74|    14.05|     0.8744|             5.482|             3.114|                2.932|             4.825|[13.74,14.05,0.87...|         0|
|14.59|    14.28|     0.8993|             5.351|             3.333|                4.185| 4.781000000000001|[14.59,14.28,0.89...|         0|
|13.99|    13.83|     0.9183|             5.119|             3.383|                5.234| 4.781000000000001|[13.99,13.83,0.91...|         1|
|15.69|    14.75|     0.9058|             5.527|             3.514|                1.599|             5.046|[15.69,14.75,0.90...|         0|
| 14.7|    14.21|     0.9153|             5.205|             3.466|                1.767|             4.649|[14.7,14.21,0.915...|         0|
|12.72|    13.57|     0.8686|             5.226|             3.049|                4.102|             4.914|[12.72,13.57,0.86...|         1|
+-----+---------+-----------+------------------+------------------+---------------------+------------------+--------------------+----------+
only showing top 20 rows

"""