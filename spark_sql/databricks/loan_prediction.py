#creating the data frame with using spark sql and load the loan data
df=spark.read.format("delta").load("/user/hive/warehouse/my_demo_db.db/loan_train_data_copy")

#creating temporary table for performing the sql queries
df.createOrReplaceTempView("loan_table")

#use of select query
df1=spark.sql("select * from loan_table")
df1.show()

"""
+--------+------+-------+----------+------------+-------------+---------------+-----------------+----------+----------------+--------------+-------------+-----------+
| Loan_ID|Gender|Married|Dependents|   Education|Self_Employed|ApplicantIncome|CoapplicantIncome|LoanAmount|Loan_Amount_Term|Credit_History|Property_Area|Loan_Status|
+--------+------+-------+----------+------------+-------------+---------------+-----------------+----------+----------------+--------------+-------------+-----------+
|LP001002|  Male|     No|         0|    Graduate|           No|           5849|                0|      null|             360|             1|        Urban|          Y|
|LP001003|  Male|    Yes|         1|    Graduate|           No|           4583|             1508|       128|             360|             1|        Rural|          N|
|LP001005|  Male|    Yes|         0|    Graduate|          Yes|           3000|                0|        66|             360|             1|        Urban|          Y|
|LP001006|  Male|    Yes|         0|Not Graduate|           No|           2583|             2358|       120|             360|             1|        Urban|          Y|
|LP001008|  Male|     No|         0|    Graduate|           No|           6000|                0|       141|             360|             1|        Urban|          Y|
|LP001011|  Male|    Yes|         2|    Graduate|          Yes|           5417|             4196|       267|             360|             1|        Urban|          Y|
|LP001013|  Male|    Yes|         0|Not Graduate|           No|           2333|             1516|        95|             360|             1|        Urban|          Y|
|LP001014|  Male|    Yes|        3+|    Graduate|           No|           3036|             2504|       158|             360|             0|    Semiurban|          N|
|LP001018|  Male|    Yes|         2|    Graduate|           No|           4006|             1526|       168|             360|             1|        Urban|          Y|
|LP001020|  Male|    Yes|         1|    Graduate|           No|          12841|            10968|       349|             360|             1|    Semiurban|          N|
|LP001024|  Male|    Yes|         2|    Graduate|           No|           3200|              700|        70|             360|             1|        Urban|          Y|
|LP001027|  Male|    Yes|         2|    Graduate|         null|           2500|             1840|       109|             360|             1|        Urban|          Y|
|LP001028|  Male|    Yes|         2|    Graduate|           No|           3073|             8106|       200|             360|             1|        Urban|          Y|
|LP001029|  Male|     No|         0|    Graduate|           No|           1853|             2840|       114|             360|             1|        Rural|          N|
|LP001030|  Male|    Yes|         2|    Graduate|           No|           1299|             1086|        17|             120|             1|        Urban|          Y|
|LP001032|  Male|     No|         0|    Graduate|           No|           4950|                0|       125|             360|             1|        Urban|          Y|
|LP001034|  Male|     No|         1|Not Graduate|           No|           3596|                0|       100|             240|          null|        Urban|          Y|
|LP001036|Female|     No|         0|    Graduate|           No|           3510|                0|        76|             360|             0|        Urban|          N|
|LP001038|  Male|    Yes|         0|Not Graduate|           No|           4887|                0|       133|             360|             1|        Rural|          N|
|LP001041|  Male|    Yes|         0|    Graduate|         null|           2600|             3500|       115|            null|             1|        Urban|          Y|
+--------+------+-------+----------+------------+-------------+---------------+-----------------+----------+----------------+--------------+-------------+-----------+
only showing top 20 rows
"""

spark.sql("select count('Loan_ID') as id_count from loan_table").show()
"""
+--------+
|id_count|
+--------+
|     614|
+--------+
"""
spark.sql("select Gender,count(*) from loan_table group by Gender").show()

"""
+------+--------+
|Gender|count(1)|
+------+--------+
|  null|      13|
|Female|     112|
|  Male|     489|
+------+--------+
"""

spark.sql("select Education, count(*) from loan_table group by Education order by Education").show()

"""
+------------+--------+
|   Education|count(1)|
+------------+--------+
|    Graduate|     480|
|Not Graduate|     134|
+------------+--------+
"""

spark.sql("select * from Loan_table where Gender is null").show()

"""
+--------+------+-------+----------+------------+-------------+---------------+-----------------+----------+----------------+--------------+-------------+-----------+
| Loan_ID|Gender|Married|Dependents|   Education|Self_Employed|ApplicantIncome|CoapplicantIncome|LoanAmount|Loan_Amount_Term|Credit_History|Property_Area|Loan_Status|
+--------+------+-------+----------+------------+-------------+---------------+-----------------+----------+----------------+--------------+-------------+-----------+
|LP001050|  null|    Yes|         2|Not Graduate|           No|           3365|             1917|       112|             360|             0|        Rural|          N|
|LP001448|  null|    Yes|        3+|    Graduate|           No|          23803|                0|       370|             360|             1|        Rural|          Y|
|LP001585|  null|    Yes|        3+|    Graduate|           No|          51763|                0|       700|             300|             1|        Urban|          Y|
|LP001644|  null|    Yes|         0|    Graduate|          Yes|            674|             5296|       168|             360|             1|        Rural|          Y|
|LP002024|  null|    Yes|         0|    Graduate|           No|           2473|             1843|       159|             360|             1|        Rural|          N|
|LP002103|  null|    Yes|         1|    Graduate|          Yes|           9833|             1833|       182|             180|             1|        Urban|          Y|
|LP002478|  null|    Yes|         0|    Graduate|          Yes|           2083|             4083|       160|             360|          null|    Semiurban|          Y|
|LP002501|  null|    Yes|         0|    Graduate|           No|          16692|                0|       110|             360|             1|    Semiurban|          Y|
|LP002530|  null|    Yes|         2|    Graduate|           No|           2873|             1872|       132|             360|             0|    Semiurban|          N|
|LP002625|  null|     No|         0|    Graduate|           No|           3583|                0|        96|             360|             1|        Urban|          N|
|LP002872|  null|    Yes|         0|    Graduate|           No|           3087|             2210|       136|             360|             0|    Semiurban|          N|
|LP002925|  null|     No|         0|    Graduate|           No|           4750|                0|        94|             360|             1|    Semiurban|          Y|
|LP002933|  null|     No|        3+|    Graduate|          Yes|           9357|                0|       292|             360|             1|    Semiurban|          Y|
+--------+------+-------+----------+------------+-------------+---------------+-----------------+----------+----------------+--------------+-------------+-----------+
"""
spark.sql("select * from loan_table where Gender='Male' ").show()

""""
+--------+------+-------+----------+------------+-------------+---------------+-----------------+----------+----------------+--------------+-------------+-----------+
| Loan_ID|Gender|Married|Dependents|   Education|Self_Employed|ApplicantIncome|CoapplicantIncome|LoanAmount|Loan_Amount_Term|Credit_History|Property_Area|Loan_Status|
+--------+------+-------+----------+------------+-------------+---------------+-----------------+----------+----------------+--------------+-------------+-----------+
|LP001002|  Male|     No|         0|    Graduate|           No|           5849|                0|      null|             360|             1|        Urban|          Y|
|LP001003|  Male|    Yes|         1|    Graduate|           No|           4583|             1508|       128|             360|             1|        Rural|          N|
|LP001005|  Male|    Yes|         0|    Graduate|          Yes|           3000|                0|        66|             360|             1|        Urban|          Y|
|LP001006|  Male|    Yes|         0|Not Graduate|           No|           2583|             2358|       120|             360|             1|        Urban|          Y|
|LP001008|  Male|     No|         0|    Graduate|           No|           6000|                0|       141|             360|             1|        Urban|          Y|
|LP001011|  Male|    Yes|         2|    Graduate|          Yes|           5417|             4196|       267|             360|             1|        Urban|          Y|
|LP001013|  Male|    Yes|         0|Not Graduate|           No|           2333|             1516|        95|             360|             1|        Urban|          Y|
|LP001014|  Male|    Yes|        3+|    Graduate|           No|           3036|             2504|       158|             360|             0|    Semiurban|          N|
|LP001018|  Male|    Yes|         2|    Graduate|           No|           4006|             1526|       168|             360|             1|        Urban|          Y|
|LP001020|  Male|    Yes|         1|    Graduate|           No|          12841|            10968|       349|             360|             1|    Semiurban|          N|
|LP001024|  Male|    Yes|         2|    Graduate|           No|           3200|              700|        70|             360|             1|        Urban|          Y|
|LP001027|  Male|    Yes|         2|    Graduate|         null|           2500|             1840|       109|             360|             1|        Urban|          Y|
|LP001028|  Male|    Yes|         2|    Graduate|           No|           3073|             8106|       200|             360|             1|        Urban|          Y|
|LP001029|  Male|     No|         0|    Graduate|           No|           1853|             2840|       114|             360|             1|        Rural|          N|
|LP001030|  Male|    Yes|         2|    Graduate|           No|           1299|             1086|        17|             120|             1|        Urban|          Y|
|LP001032|  Male|     No|         0|    Graduate|           No|           4950|                0|       125|             360|             1|        Urban|          Y|
|LP001034|  Male|     No|         1|Not Graduate|           No|           3596|                0|       100|             240|          null|        Urban|          Y|
|LP001038|  Male|    Yes|         0|Not Graduate|           No|           4887|                0|       133|             360|             1|        Rural|          N|
|LP001041|  Male|    Yes|         0|    Graduate|         null|           2600|             3500|       115|            null|             1|        Urban|          Y|
|LP001043|  Male|    Yes|         0|Not Graduate|           No|           7660|                0|       104|             360|             0|        Urban|          N|
+--------+------+-------+----------+------------+-------------+---------------+-----------------+----------+----------------+--------------+-------------+-----------+
only showing top 20 rows
"""

spark.sql("select Loan_ID,ApplicantIncome from loan_table order by ApplicantIncome desc limit 5").show()

"""
+--------+---------------+
| Loan_ID|ApplicantIncome|
+--------+---------------+
|LP002945|           9963|
|LP002103|           9833|
|LP001814|           9703|
|LP001066|           9560|
|LP001543|           9538|
+--------+---------------+
"""
spark.sql("select min(ApplicantIncome) from (select ApplicantIncome from loan_table order by ApplicantIncome desc limit 2)").show()

"""
+--------------------+
|min(ApplicantIncome)|
+--------------------+
|                9833|
+--------------------+
""""

spark.sql("select ApplicantIncome from (select ApplicantIncome from loan_table order by ApplicantIncome desc limit 3) order by ApplicantIncome limit 2 ").show()

"""
+---------------+
|ApplicantIncome|
+---------------+
|           9703|
|           9833|
+---------------+
"""