# creating the spark  dataframe

data=spark.read.format("csv").option("header","true").option("inferSchema","true").load("/FileStore/tables/movielens_ratings.csv")
data.show()
"""
+-------+------+------+
|movieId|rating|userId|
+-------+------+------+
|      2|   3.0|     0|
|      3|   1.0|     0|
|      5|   2.0|     0|
|      9|   4.0|     0|
|     11|   1.0|     0|
|     12|   2.0|     0|
|     15|   1.0|     0|
|     17|   1.0|     0|
|     19|   1.0|     0|
|     21|   1.0|     0|
|     23|   1.0|     0|
|     26|   3.0|     0|
|     27|   1.0|     0|
|     28|   1.0|     0|
|     29|   1.0|     0|
|     30|   1.0|     0|
|     31|   1.0|     0|
|     34|   1.0|     0|
|     37|   1.0|     0|
|     41|   2.0|     0|
+-------+------+------+
only showing top 20 rows
"""

# importing the libraries
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

#displaying the data and calculating the mean, min, max, sd
data.describe().show()

"""
+-------+------------------+------------------+------------------+
|summary|           movieId|            rating|            userId|
+-------+------------------+------------------+------------------+
|  count|              1501|              1501|              1501|
|   mean| 49.40572951365756|1.7741505662891406|14.383744170552964|
| stddev|28.937034065088994| 1.187276166124803| 8.591040424293272|
|    min|                 0|               1.0|                 0|
|    max|                99|               5.0|                29|
+-------+------------------+------------------+------------------+
"""

#randomSplit train and test with 80 and 20
train,test=data.randomSplit([0.8,0.2])
als=ALS(maxIter=5,regParam=0.01,userCol='userId',itemCol='movieId',ratingCol='rating')

#training the model
model=als.fit(train)

#transforming the model
prediction=model.transform(test)

#showing the data
prediction.show()

"""
+-------+------+------+----------+
|movieId|rating|userId|prediction|
+-------+------+------+----------+
|     31|   1.0|    13|0.93357134|
|     31|   1.0|    26|-1.9508412|
|     31|   1.0|    29| 2.7379408|
|     85|   1.0|     4| 2.7876363|
|     65|   2.0|    15| 1.2982044|
|     65|   1.0|    16| 1.3119168|
|     65|   1.0|    19| 2.4092946|
|     65|   1.0|    24|0.35680467|
|     53|   1.0|     6|  4.150136|
|     53|   3.0|    14|  4.858346|
|     78|   1.0|     8|0.29063344|
|     78|   1.0|    17|  1.029181|
|     78|   1.0|    24|  1.176908|
|     76|   1.0|     1| 2.3910084|
|     76|   3.0|     7|  4.486595|
|     76|   3.0|    18|-1.4972886|
|     27|   1.0|     5|  3.590616|
|     26|   1.0|     6|  2.710273|
|     27|   1.0|    14|0.99982166|
|     27|   1.0|    18| 3.0749717|
+-------+------+------+----------+
only showing top 20 rows
"""

#using regression evaluator
evaluator=RegressionEvaluator(metricName='rmse',labelCol='rating',predictionCol='prediction')


rmse=evaluator.evaluate(prediction)
print("RMSE","=",rmse)
#RMSE = 1.6993286789294564

single_user=test.filter(test['userId']==11).select(['movieId','userId'])
single_user.show()

"""
+-------+------+
|movieId|userId|
+-------+------+
|     12|    11|
|     18|    11|
|     19|    11|
|     22|    11|
|     30|    11|
|     50|    11|
|     77|    11|
|     88|    11|
+-------+------+
"""

#
recommandation=model.transform(single_user)
recommandation.orderBy('prediction',ascending=False).show()

"""
+-------+------+-----------+
|movieId|userId| prediction|
+-------+------+-----------+
|     22|    11|  2.8568568|
|     12|    11|  2.5498793|
|     30|    11|  1.6695285|
|     88|    11| 0.27667642|
|     50|    11|   0.061867|
|     19|    11|-0.12907287|
|     77|    11| -1.3245386|
|     18|    11| -2.2024827|
+-------+------+-----------+
"""