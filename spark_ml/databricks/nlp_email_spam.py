# creating the spark data frame and load  the email SMSSpamCollection data
data=spark.read.format("csv").option("inferSchema","true").load("/FileStore/tables/SMSSpamCollection-1",sep='\t')
data.show()

"""
+----+--------------------+
| _c0|                 _c1|
+----+--------------------+
| ham|Go until jurong p...|
| ham|Ok lar... Joking ...|
|spam|Free entry in 2 a...|
| ham|U dun say so earl...|
| ham|Nah I don't think...|
|spam|FreeMsg Hey there...|
| ham|Even my brother i...|
| ham|As per your reque...|
|spam|WINNER!! As a val...|
|spam|Had your mobile 1...|
| ham|I'm gonna be home...|
|spam|SIX chances to wi...|
|spam|URGENT! You have ...|
| ham|I've been searchi...|
| ham|I HAVE A DATE ON ...|
|spam|XXXMobileMovieClu...|
| ham|Oh k...i'm watchi...|
| ham|Eh u remember how...|
| ham|Fine if thats th...|
|spam|England v Macedon...|
+----+--------------------+
only showing top 20 rows
"""

#renaming the columns

data=data.withColumnRenamed('_c0','class').withColumnRenamed('_c1','text')
data.show()

"""
+-----+--------------------+
|class|                text|
+-----+--------------------+
|  ham|Go until jurong p...|
|  ham|Ok lar... Joking ...|
| spam|Free entry in 2 a...|
|  ham|U dun say so earl...|
|  ham|Nah I don't think...|
| spam|FreeMsg Hey there...|
|  ham|Even my brother i...|
|  ham|As per your reque...|
| spam|WINNER!! As a val...|
| spam|Had your mobile 1...|
|  ham|I'm gonna be home...|
| spam|SIX chances to wi...|
| spam|URGENT! You have ...|
|  ham|I've been searchi...|
|  ham|I HAVE A DATE ON ...|
| spam|XXXMobileMovieClu...|
|  ham|Oh k...i'm watchi...|
|  ham|Eh u remember how...|
|  ham|Fine if thats th...|
| spam|England v Macedon...|
+-----+--------------------+
only showing top 20 rows
"""
#import the length function for check the of text
from pyspark.sql.functions import length
data=data.withColumn('length',length(data['text']))
data.show()

"""
+-----+--------------------+------+
|class|                text|length|
+-----+--------------------+------+
|  ham|Go until jurong p...|   111|
|  ham|Ok lar... Joking ...|    29|
| spam|Free entry in 2 a...|   155|
|  ham|U dun say so earl...|    49|
|  ham|Nah I don't think...|    61|
| spam|FreeMsg Hey there...|   147|
|  ham|Even my brother i...|    77|
|  ham|As per your reque...|   160|
| spam|WINNER!! As a val...|   157|
| spam|Had your mobile 1...|   154|
|  ham|I'm gonna be home...|   109|
| spam|SIX chances to wi...|   136|
| spam|URGENT! You have ...|   155|
|  ham|I've been searchi...|   196|
|  ham|I HAVE A DATE ON ...|    35|
| spam|XXXMobileMovieClu...|   149|
|  ham|Oh k...i'm watchi...|    26|
|  ham|Eh u remember how...|    81|
|  ham|Fine if thats th...|    56|
| spam|England v Macedon...|   155|
+-----+--------------------+------+
only showing top 20 rows
"""

#grouping the ham and spam separately

data.groupBy('class').mean().show()

"""
+-----+-----------------+
|class|      avg(length)|
+-----+-----------------+
|  ham| 71.4545266210897|
| spam|138.6706827309237|
+-----+-----------------+
"""

#importing the all function which we need for this
from pyspark.ml.feature import (Tokenizer,StopWordsRemover,CountVectorizer,IDF,StringIndexer)

#using the tokenizer
tokenizer=Tokenizer(inputCol='text',outputCol='token_text')
Stop_remove=StopWordsRemover(inputCol='token_text',outputCol='stop_token')
count_vec=CountVectorizer(inputCol='stop_token',outputCol='c_vec')
idf=IDF(inputCol='c_vec',outputCol='tf_idf')
ham_spam_to_numeric=StringIndexer(inputCol='class',outputCol='label')

#importing vector VectorAssembler
from pyspark.ml.feature import VectorAssembler


#creating the new column name is Features
clean_up=VectorAssembler(inputCols=['tf_idf','length'],outputCol='features')

#importing the naive bayes and using naive bayes algorithmns

from pyspark.ml.classification import NaiveBayes
nb=NaiveBayes()

#importing the pipelines and using pipelines
from pyspark.ml import Pipeline
data_prep_pipe=Pipeline(stages=[ham_spam_to_numeric,tokenizer,Stop_remove,count_vec,idf,clean_up])

#cleaning the data
cleaner=data_prep_pipe.fit(data)

#After cleaning the data ,tranforming the data
clean_data=cleaner.transform(data)

#it is showing clean data
clean_data.show()

"""
+-----+--------------------+
|label|            features|
+-----+--------------------+
|  0.0|(13424,[7,11,31,6...|
|  0.0|(13424,[0,24,297,...|
|  1.0|(13424,[2,13,19,3...|
|  0.0|(13424,[0,70,80,1...|
|  0.0|(13424,[36,134,31...|
|  1.0|(13424,[10,60,139...|
|  0.0|(13424,[10,53,103...|
|  0.0|(13424,[125,184,4...|
|  1.0|(13424,[1,47,118,...|
|  1.0|(13424,[0,1,13,27...|
|  0.0|(13424,[18,43,120...|
|  1.0|(13424,[8,17,37,8...|
|  1.0|(13424,[13,30,47,...|
|  0.0|(13424,[39,96,217...|
|  0.0|(13424,[552,1697,...|
|  1.0|(13424,[30,109,11...|
|  0.0|(13424,[82,214,47...|
|  0.0|(13424,[0,2,49,13...|
|  0.0|(13424,[0,74,105,...|
|  1.0|(13424,[4,30,33,5...|
+-----+--------------------+
only showing top 20 rows
"""

#
clean_data=clean_data.select('label','features')
clean_data.show()

""""
+-----+--------------------+
|label|            features|
+-----+--------------------+
|  0.0|(13424,[7,11,31,6...|
|  0.0|(13424,[0,24,297,...|
|  1.0|(13424,[2,13,19,3...|
|  0.0|(13424,[0,70,80,1...|
|  0.0|(13424,[36,134,31...|
|  1.0|(13424,[10,60,139...|
|  0.0|(13424,[10,53,103...|
|  0.0|(13424,[125,184,4...|
|  1.0|(13424,[1,47,118,...|
|  1.0|(13424,[0,1,13,27...|
|  0.0|(13424,[18,43,120...|
|  1.0|(13424,[8,17,37,8...|
|  1.0|(13424,[13,30,47,...|
|  0.0|(13424,[39,96,217...|
|  0.0|(13424,[552,1697,...|
|  1.0|(13424,[30,109,11...|
|  0.0|(13424,[82,214,47...|
|  0.0|(13424,[0,2,49,13...|
|  0.0|(13424,[0,74,105,...|
|  1.0|(13424,[4,30,33,5...|
+-----+--------------------+
only showing top 20 rows
"""
# using the random split with 70 and 30
train,test=clean_data.randomSplit([0.7,0.3])
spam_detector=nb.fit(train)

test_result=spam_detector.transform(test)
test_result.show()

"""
+-----+--------------------+--------------------+--------------------+----------+
|label|            features|       rawPrediction|         probability|prediction|
+-----+--------------------+--------------------+--------------------+----------+
|  0.0|(13424,[0,1,2,7,8...|[-793.82767742551...|[1.0,2.3444966644...|       0.0|
|  0.0|(13424,[0,1,3,9,1...|[-570.73615309635...|[0.99999999999997...|       0.0|
|  0.0|(13424,[0,1,5,15,...|[-998.71783715621...|[1.0,6.3558945097...|       0.0|
|  0.0|(13424,[0,1,7,8,1...|[-1164.9775603190...|[1.0,1.6966547341...|       0.0|
|  0.0|(13424,[0,1,7,15,...|[-669.15479296564...|[1.0,8.2482609745...|       0.0|
|  0.0|(13424,[0,1,9,14,...|[-535.65641097047...|[1.0,1.6105086235...|       0.0|
|  0.0|(13424,[0,1,11,32...|[-872.72887151548...|[1.0,4.6757993951...|       0.0|
|  0.0|(13424,[0,1,14,31...|[-215.82822481286...|[1.0,8.4943760631...|       0.0|
|  0.0|(13424,[0,1,15,20...|[-688.91794617007...|[1.0,2.8545264615...|       0.0|
|  0.0|(13424,[0,1,21,27...|[-1007.8475677599...|[1.0,8.3993269378...|       0.0|
|  0.0|(13424,[0,1,27,88...|[-1536.0338645371...|[0.00896796163277...|       1.0|
|  0.0|(13424,[0,1,30,12...|[-614.52182304932...|[1.0,5.5094231864...|       0.0|
|  0.0|(13424,[0,1,43,69...|[-631.42052126855...|[0.00476283129999...|       1.0|
|  0.0|(13424,[0,1,46,17...|[-1136.2887621930...|[3.70261813669151...|       1.0|
|  0.0|(13424,[0,1,3657,...|[-127.88988026100...|[0.99998170638038...|       0.0|
|  0.0|(13424,[0,2,3,4,6...|[-1280.1408057853...|[1.0,5.3117430230...|       0.0|
|  0.0|(13424,[0,2,3,6,9...|[-3283.5012976442...|[1.0,6.9768036008...|       0.0|
|  0.0|(13424,[0,2,4,5,1...|[-2492.5184994840...|[1.0,2.5054779587...|       0.0|
|  0.0|(13424,[0,2,4,7,2...|[-517.57682493113...|[1.0,7.9673972188...|       0.0|
|  0.0|(13424,[0,2,4,8,2...|[-1418.7403514602...|[1.0,1.7398029476...|       0.0|
+-----+--------------------+--------------------+--------------------+----------+
only showing top 20 rows
"""
#importing the MulticlassClassificationEvaluator for check the accuracy of my result

from pyspark.ml.evaluation import MulticlassClassificationEvaluator
acc_eval=MulticlassClassificationEvaluator()
acc=acc_eval.evaluate(test_result)
print("ACCURACY OF NaiveByes model","=",acc)

#ACCURACY OF NaiveByes model = 0.9287205668642323
