### "Load the customer churn data CSV File, have Spark infer the data types."
data=spark.read.format("csv").option("header","true").option("inferSchema","true").load("/FileStore/tables/customer_churn.csv")
data.show()

"""
+-------------------+----+--------------+---------------+-----+---------+-------------------+--------------------+--------------------+-----+
|              Names| Age|Total_Purchase|Account_Manager|Years|Num_Sites|       Onboard_date|            Location|             Company|Churn|
+-------------------+----+--------------+---------------+-----+---------+-------------------+--------------------+--------------------+-----+
|   Cameron Williams|42.0|       11066.8|              0| 7.22|      8.0|2013-08-30 07:00:40|10265 Elizabeth M...|          Harvey LLC|    1|
|      Kevin Mueller|41.0|      11916.22|              0|  6.5|     11.0|2013-08-13 00:38:46|6157 Frank Garden...|          Wilson PLC|    1|
|        Eric Lozano|38.0|      12884.75|              0| 6.67|     12.0|2016-06-29 06:20:07|1331 Keith Court ...|Miller, Johnson a...|    1|
|      Phillip White|42.0|       8010.76|              0| 6.71|     10.0|2014-04-22 12:43:12|13120 Daniel Moun...|           Smith Inc|    1|
|     Cynthia Norton|37.0|       9191.58|              0| 5.56|      9.0|2016-01-19 15:31:15|765 Tricia Row Ka...|          Love-Jones|    1|
|   Jessica Williams|48.0|      10356.02|              0| 5.12|      8.0|2009-03-03 23:13:37|6187 Olson Mounta...|        Kelly-Warren|    1|
|        Eric Butler|44.0|      11331.58|              1| 5.23|     11.0|2016-12-05 03:35:43|4846 Savannah Roa...|   Reynolds-Sheppard|    1|
|      Zachary Walsh|32.0|       9885.12|              1| 6.92|      9.0|2006-03-09 14:50:20|25271 Roy Express...|          Singh-Cole|    1|
|        Ashlee Carr|43.0|       14062.6|              1| 5.46|     11.0|2011-09-29 05:47:23|3725 Caroline Str...|           Lopez PLC|    1|
|     Jennifer Lynch|40.0|       8066.94|              1| 7.11|     11.0|2006-03-28 15:42:45|363 Sandra Lodge ...|       Reed-Martinez|    1|
|       Paula Harris|30.0|      11575.37|              1| 5.22|      8.0|2016-11-13 13:13:01|Unit 8120 Box 916...|Briggs, Lamb and ...|    1|
|     Bruce Phillips|45.0|       8771.02|              1| 6.64|     11.0|2015-05-28 12:14:03|Unit 1895 Box 094...|    Figueroa-Maynard|    1|
|       Craig Garner|45.0|       8988.67|              1| 4.84|     11.0|2011-02-16 08:10:47|897 Kelley Overpa...|     Abbott-Thompson|    1|
|       Nicole Olson|40.0|       8283.32|              1|  5.1|     13.0|2012-11-22 05:35:03|11488 Weaver Cape...|Smith, Kim and Ma...|    1|
|     Harold Griffin|41.0|       6569.87|              1|  4.3|     11.0|2015-03-28 02:13:44|1774 Peter Row Ap...|Snyder, Lee and M...|    1|
|       James Wright|38.0|      10494.82|              1| 6.81|     12.0|2015-07-22 08:38:40|45408 David Path ...|      Sanders-Pierce|    1|
|      Doris Wilkins|45.0|       8213.41|              1| 7.35|     11.0|2006-09-03 06:13:55|28216 Wright Moun...|Andrews, Adams an...|    1|
|Katherine Carpenter|43.0|      11226.88|              0| 8.08|     12.0|2006-10-22 04:42:38|Unit 4948 Box 481...|Morgan, Phillips ...|    1|
|     Lindsay Martin|53.0|       5515.09|              0| 6.85|      8.0|2015-10-07 00:27:10|69203 Crosby Divi...|      Villanueva LLC|    1|
|        Kathy Curry|46.0|        8046.4|              1| 5.69|      8.0|2014-11-06 23:47:14|9569 Caldwell Cre...|Berry, Orr and Ca...|    1|
+-------------------+----+--------------+---------------+-----+---------+-------------------+--------------------+--------------------+-----+
only showing top 20 rows
"""

#import the Logistic Regression
from pyspark.ml.classification import LogisticRegression

#displaying the schema of the data
data.printSchema()

"""
root
 |-- Names: string (nullable = true)
 |-- Age: double (nullable = true)
 |-- Total_Purchase: double (nullable = true)
 |-- Account_Manager: integer (nullable = true)
 |-- Years: double (nullable = true)
 |-- Num_Sites: double (nullable = true)
 |-- Onboard_date: string (nullable = true)
 |-- Location: string (nullable = true)
 |-- Company: string (nullable = true)
 |-- Churn: integer (nullable = true)
"""

#describing the data and it is showing all the information about data
data.describe().show()

"""
+-------+-------------+-----------------+-----------------+------------------+-----------------+------------------+-------------------+--------------------+--------------------+-------------------+
|summary|        Names|              Age|   Total_Purchase|   Account_Manager|            Years|         Num_Sites|       Onboard_date|            Location|             Company|              Churn|
+-------+-------------+-----------------+-----------------+------------------+-----------------+------------------+-------------------+--------------------+--------------------+-------------------+
|  count|          900|              900|              900|               900|              900|               900|                900|                 900|                 900|                900|
|   mean|         null|41.81666666666667|10062.82403333334|0.4811111111111111| 5.27315555555555| 8.587777777777777|               null|                null|                null|0.16666666666666666|
| stddev|         null|6.127560416916251|2408.644531858096|0.4999208935073339|1.274449013194616|1.7648355920350969|               null|                null|                null| 0.3728852122772358|
|    min|   Aaron King|             22.0|            100.0|                 0|              1.0|               3.0|2006-01-02 04:16:13|00103 Jeffrey Cre...|     Abbott-Thompson|                  0|
|    max|Zachary Walsh|             65.0|         18026.01|                 1|             9.15|              14.0|2016-12-28 04:07:38|Unit 9800 Box 287...|Zuniga, Clark and...|                  1|
+-------+-------------+-----------------+-----------------+------------------+-----------------+------------------+-------------------+--------------------+--------------------+-------------------+
"""

data.columns
Out[5]: ['Names',
 'Age',
 'Total_Purchase',
 'Account_Manager',
 'Years',
 'Num_Sites',
 'Onboard_date',
 'Location',
 'Company',
 'Churn']

 # import the vectorAssembler
from pyspark.ml.feature import VectorAssembler
assembler=VectorAssembler(inputCols=['Age',
 'Total_Purchase',
 'Account_Manager',
 'Years',
 'Num_Sites'],outputCol='features')

output=assembler.transform(data)
final_data=output.select(['features','churn'])

#it is spliting the with 70 and 30
train_data,test_data=final_data.randomSplit([0.7,0.3])

#displaying the train data information

train_data.describe().show()
"""
+-------+-------------------+
|summary|              churn|
+-------+-------------------+
|  count|                624|
|   mean|0.16506410256410256|
| stddev| 0.3715362179025329|
|    min|                  0|
|    max|                  1|
+-------+-------------------+
"""

#displaying the test data information
test_data.describe().show()

"""
+-------+-------------------+
|summary|              churn|
+-------+-------------------+
|  count|                276|
|   mean|0.17028985507246377|
| stddev|0.37657005535174365|
|    min|                  0|
|    max|                  1|
+-------+-------------------+
"""

lr_churn=LogisticRegression(labelCol='churn')
fitted_churn_model=lr_churn.fit(train_data)
training_sum=fitted_churn_model.summary
training_sum.predictions.describe().show()

"""
+-------+-------------------+-------------------+
|summary|              churn|         prediction|
+-------+-------------------+-------------------+
|  count|                624|                624|
|   mean|0.16506410256410256|              0.125|
| stddev| 0.3715362179025329|0.33098423194731313|
|    min|                0.0|                0.0|
|    max|                1.0|                1.0|
+-------+-------------------+-------------------+
"""

#import the BinaryClassificationEvaluator and evaluate the churn data and prediction
from pyspark.ml.evaluation import BinaryClassificationEvaluator
pred_and_labels=fitted_churn_model.evaluate(test_data)
pred_and_labels.predictions.show()

"""
+--------------------+-----+--------------------+--------------------+----------+
|            features|churn|       rawPrediction|         probability|prediction|
+--------------------+-----+--------------------+--------------------+----------+
|[27.0,8628.8,1.0,...|    0|[5.35879821906363...|[0.99531548896440...|       0.0|
|[28.0,8670.98,0.0...|    0|[7.59665807600217...|[0.99949812530284...|       0.0|
|[29.0,8688.17,1.0...|    1|[2.67611656503959...|[0.93560254041190...|       0.0|
|[29.0,10203.18,1....|    0|[3.75239867510143...|[0.97707641736131...|       0.0|
|[29.0,13240.01,1....|    0|[6.63849297293983...|[0.99869271278272...|       0.0|
|[30.0,6744.87,0.0...|    0|[3.41802123619628...|[0.96826302102774...|       0.0|
|[30.0,10960.52,1....|    0|[2.42282628732077...|[0.91855144263442...|       0.0|
|[31.0,10058.87,1....|    0|[4.43570580578262...|[0.98829199969882...|       0.0|
|[31.0,10182.6,1.0...|    0|[4.63265661612392...|[0.99036486026294...|       0.0|
|[31.0,11297.57,1....|    1|[1.03919032678773...|[0.73869374888162...|       0.0|
|[32.0,5756.12,0.0...|    0|[4.10235984205574...|[0.98373530160800...|       0.0|
|[32.0,6367.22,1.0...|    0|[2.77329519541093...|[0.94121557110130...|       0.0|
|[32.0,9885.12,1.0...|    1|[1.83695891255705...|[0.86258864623461...|       0.0|
|[32.0,11540.86,0....|    0|[6.57030311999517...|[0.99860058853312...|       0.0|
|[32.0,12254.75,1....|    0|[2.57160661966371...|[0.92901172383867...|       0.0|
|[33.0,8556.73,0.0...|    0|[3.62951828646351...|[0.97415663687363...|       0.0|
|[33.0,10306.21,1....|    0|[2.03591093551138...|[0.88451623749122...|       0.0|
|[33.0,12115.91,1....|    0|[2.41754207219532...|[0.91815523033152...|       0.0|
|[33.0,12249.96,0....|    0|[5.33115603825358...|[0.99518482477919...|       0.0|
|[34.0,7324.32,0.0...|    0|[1.18906442853938...|[0.76657369626155...|       0.0|
+--------------------+-----+--------------------+--------------------+----------+
only showing top 20 rows
"""

churn_eval=BinaryClassificationEvaluator(rawPredictionCol='prediction',labelCol='churn')

#accuracy of the churn evaluation
auc=churn_eval.evaluate(pred_and_labels.predictions)
auc

#Out[32]: 0.7653999814178204
final_lr_model=lr_churn.fit(final_data)

# creating the new dataframe named is new_customer

new_customer=spark.read.format("csv").option("header","true").option("inferSchema","true").load("/FileStore/tables/customer_churn.csv")
new_customer.show()

"""
+-------------------+----+--------------+---------------+-----+---------+-------------------+--------------------+--------------------+-----+
|              Names| Age|Total_Purchase|Account_Manager|Years|Num_Sites|       Onboard_date|            Location|             Company|Churn|
+-------------------+----+--------------+---------------+-----+---------+-------------------+--------------------+--------------------+-----+
|   Cameron Williams|42.0|       11066.8|              0| 7.22|      8.0|2013-08-30 07:00:40|10265 Elizabeth M...|          Harvey LLC|    1|
|      Kevin Mueller|41.0|      11916.22|              0|  6.5|     11.0|2013-08-13 00:38:46|6157 Frank Garden...|          Wilson PLC|    1|
|        Eric Lozano|38.0|      12884.75|              0| 6.67|     12.0|2016-06-29 06:20:07|1331 Keith Court ...|Miller, Johnson a...|    1|
|      Phillip White|42.0|       8010.76|              0| 6.71|     10.0|2014-04-22 12:43:12|13120 Daniel Moun...|           Smith Inc|    1|
|     Cynthia Norton|37.0|       9191.58|              0| 5.56|      9.0|2016-01-19 15:31:15|765 Tricia Row Ka...|          Love-Jones|    1|
|   Jessica Williams|48.0|      10356.02|              0| 5.12|      8.0|2009-03-03 23:13:37|6187 Olson Mounta...|        Kelly-Warren|    1|
|        Eric Butler|44.0|      11331.58|              1| 5.23|     11.0|2016-12-05 03:35:43|4846 Savannah Roa...|   Reynolds-Sheppard|    1|
|      Zachary Walsh|32.0|       9885.12|              1| 6.92|      9.0|2006-03-09 14:50:20|25271 Roy Express...|          Singh-Cole|    1|
|        Ashlee Carr|43.0|       14062.6|              1| 5.46|     11.0|2011-09-29 05:47:23|3725 Caroline Str...|           Lopez PLC|    1|
|     Jennifer Lynch|40.0|       8066.94|              1| 7.11|     11.0|2006-03-28 15:42:45|363 Sandra Lodge ...|       Reed-Martinez|    1|
|       Paula Harris|30.0|      11575.37|              1| 5.22|      8.0|2016-11-13 13:13:01|Unit 8120 Box 916...|Briggs, Lamb and ...|    1|
|     Bruce Phillips|45.0|       8771.02|              1| 6.64|     11.0|2015-05-28 12:14:03|Unit 1895 Box 094...|    Figueroa-Maynard|    1|
|       Craig Garner|45.0|       8988.67|              1| 4.84|     11.0|2011-02-16 08:10:47|897 Kelley Overpa...|     Abbott-Thompson|    1|
|       Nicole Olson|40.0|       8283.32|              1|  5.1|     13.0|2012-11-22 05:35:03|11488 Weaver Cape...|Smith, Kim and Ma...|    1|
|     Harold Griffin|41.0|       6569.87|              1|  4.3|     11.0|2015-03-28 02:13:44|1774 Peter Row Ap...|Snyder, Lee and M...|    1|
|       James Wright|38.0|      10494.82|              1| 6.81|     12.0|2015-07-22 08:38:40|45408 David Path ...|      Sanders-Pierce|    1|
|      Doris Wilkins|45.0|       8213.41|              1| 7.35|     11.0|2006-09-03 06:13:55|28216 Wright Moun...|Andrews, Adams an...|    1|
|Katherine Carpenter|43.0|      11226.88|              0| 8.08|     12.0|2006-10-22 04:42:38|Unit 4948 Box 481...|Morgan, Phillips ...|    1|
|     Lindsay Martin|53.0|       5515.09|              0| 6.85|      8.0|2015-10-07 00:27:10|69203 Crosby Divi...|      Villanueva LLC|    1|
|        Kathy Curry|46.0|        8046.4|              1| 5.69|      8.0|2014-11-06 23:47:14|9569 Caldwell Cre...|Berry, Orr and Ca...|    1|
+-------------------+----+--------------+---------------+-----+---------+-------------------+--------------------+--------------------+-----+
only showing top 20 rows
"""

# displaying the schema of the new_customer data
new_customer.printSchema()
"""
root
 |-- Names: string (nullable = true)
 |-- Age: double (nullable = true)
 |-- Total_Purchase: double (nullable = true)
 |-- Account_Manager: integer (nullable = true)
 |-- Years: double (nullable = true)
 |-- Num_Sites: double (nullable = true)
 |-- Onboard_date: string (nullable = true)
 |-- Location: string (nullable = true)
 |-- Company: string (nullable = true)
 |-- Churn: integer (nullable = true)
"""

#with the help of assemble we are transforming the data
test_new_customer=assembler.transform(new_customer)
test_new_customer.printSchema()

"""
root
 |-- Names: string (nullable = true)
 |-- Age: double (nullable = true)
 |-- Total_Purchase: double (nullable = true)
 |-- Account_Manager: integer (nullable = true)
 |-- Years: double (nullable = true)
 |-- Num_Sites: double (nullable = true)
 |-- Onboard_date: string (nullable = true)
 |-- Location: string (nullable = true)
 |-- Company: string (nullable = true)
 |-- Churn: integer (nullable = true)
 |-- features: vector (nullable = true)
"""

#displaying the final result
final_result=final_lr_model.transform(test_new_customer)
final_result.select('Company','prediction').show()

"""
+--------------------+----------+
|             Company|prediction|
+--------------------+----------+
|          Harvey LLC|       0.0|
|          Wilson PLC|       1.0|
|Miller, Johnson a...|       1.0|
|           Smith Inc|       0.0|
|          Love-Jones|       0.0|
|        Kelly-Warren|       0.0|
|   Reynolds-Sheppard|       1.0|
|          Singh-Cole|       0.0|
|           Lopez PLC|       1.0|
|       Reed-Martinez|       1.0|
|Briggs, Lamb and ...|       0.0|
|    Figueroa-Maynard|       1.0|
|     Abbott-Thompson|       1.0|
|Smith, Kim and Ma...|       1.0|
|Snyder, Lee and M...|       0.0|
|      Sanders-Pierce|       1.0|
|Andrews, Adams an...|       1.0|
|Morgan, Phillips ...|       1.0|
|      Villanueva LLC|       0.0|
|Berry, Orr and Ca...|       0.0|
+--------------------+----------+
only showing top 20 rows
"""

test_new_customer.describe().show()

"""
+-------+-------------+-----------------+-----------------+------------------+-----------------+------------------+-------------------+--------------------+--------------------+-------------------+
|summary|        Names|              Age|   Total_Purchase|   Account_Manager|            Years|         Num_Sites|       Onboard_date|            Location|             Company|              Churn|
+-------+-------------+-----------------+-----------------+------------------+-----------------+------------------+-------------------+--------------------+--------------------+-------------------+
|  count|          900|              900|              900|               900|              900|               900|                900|                 900|                 900|                900|
|   mean|         null|41.81666666666667|10062.82403333334|0.4811111111111111| 5.27315555555555| 8.587777777777777|               null|                null|                null|0.16666666666666666|
| stddev|         null|6.127560416916251|2408.644531858096|0.4999208935073339|1.274449013194616|1.7648355920350969|               null|                null|                null| 0.3728852122772358|
|    min|   Aaron King|             22.0|            100.0|                 0|              1.0|               3.0|2006-01-02 04:16:13|00103 Jeffrey Cre...|     Abbott-Thompson|                  0|
|    max|Zachary Walsh|             65.0|         18026.01|                 1|             9.15|              14.0|2016-12-28 04:07:38|Unit 9800 Box 287...|Zuniga, Clark and...|                  1|
+-------+-------------+-----------------+-----------------+------------------+-----------------+------------------+-------------------+--------------------+--------------------+-------------------+
"""