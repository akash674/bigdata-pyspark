#creating the spark data frame and load the data
data=spark.read.format("csv").option("header","true").option("inferSchema","true").load("/FileStore/tables/dog_food.csv")
data.show()

"""
+---+---+----+---+-------+
|  A|  B|   C|  D|Spoiled|
+---+---+----+---+-------+
|  4|  2|12.0|  3|    1.0|
|  5|  6|12.0|  7|    1.0|
|  6|  2|13.0|  6|    1.0|
|  4|  2|12.0|  1|    1.0|
|  4|  2|12.0|  3|    1.0|
| 10|  3|13.0|  9|    1.0|
|  8|  5|14.0|  5|    1.0|
|  5|  8|12.0|  8|    1.0|
|  6|  5|12.0|  9|    1.0|
|  3|  3|12.0|  1|    1.0|
|  9|  8|11.0|  3|    1.0|
|  1| 10|12.0|  3|    1.0|
|  1|  5|13.0| 10|    1.0|
|  2| 10|12.0|  6|    1.0|
|  1| 10|11.0|  4|    1.0|
|  5|  3|12.0|  2|    1.0|
|  4|  9|11.0|  8|    1.0|
|  5|  1|11.0|  1|    1.0|
|  4|  9|12.0| 10|    1.0|
|  5|  8|10.0|  9|    1.0|
+---+---+----+---+-------+
only showing top 20 rows
"""

#importing the vector Assembler
from pyspark.ml.feature import VectorAssembler

#displaying the columns name
data.columns

#Out[4]: ['A', 'B', 'C', 'D', 'Spoiled']

#using the vector assembler and traforming the data
assembler=VectorAssembler(inputCols=['A', 'B', 'C', 'D'],outputCol='features')
output=assembler.transform(data)

#importing random forest classification algorithmns for prediction
from pyspark.ml.classification import RandomForestClassifier

#creating the a new column name is features
rfc=RandomForestClassifier(labelCol='Spoiled',featuresCol='features')
final_data=output.select(['features','Spoiled'])

# displaying final_data
final_data.show()

"""
+-------------------+-------+
|           features|Spoiled|
+-------------------+-------+
| [4.0,2.0,12.0,3.0]|    1.0|
| [5.0,6.0,12.0,7.0]|    1.0|
| [6.0,2.0,13.0,6.0]|    1.0|
| [4.0,2.0,12.0,1.0]|    1.0|
| [4.0,2.0,12.0,3.0]|    1.0|
|[10.0,3.0,13.0,9.0]|    1.0|
| [8.0,5.0,14.0,5.0]|    1.0|
| [5.0,8.0,12.0,8.0]|    1.0|
| [6.0,5.0,12.0,9.0]|    1.0|
| [3.0,3.0,12.0,1.0]|    1.0|
| [9.0,8.0,11.0,3.0]|    1.0|
|[1.0,10.0,12.0,3.0]|    1.0|
|[1.0,5.0,13.0,10.0]|    1.0|
|[2.0,10.0,12.0,6.0]|    1.0|
|[1.0,10.0,11.0,4.0]|    1.0|
| [5.0,3.0,12.0,2.0]|    1.0|
| [4.0,9.0,11.0,8.0]|    1.0|
| [5.0,1.0,11.0,1.0]|    1.0|
|[4.0,9.0,12.0,10.0]|    1.0|
| [5.0,8.0,10.0,9.0]|    1.0|
+-------------------+-------+
only showing top 20 rows
"""

#creating the fit function for training the data

rfc_model=rfc.fit(final_data)
final_data.head(1)

#Out[12]: [Row(features=DenseVector([4.0, 2.0, 12.0, 3.0]), Spoiled=1.0)]

rfc_model.featureImportances

#Out[13]: SparseVector(4, {0: 0.0189, 1: 0.0141, 2: 0.9384, 3: 0.0287})