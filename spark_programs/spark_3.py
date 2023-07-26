import pyspark
from pyspark.sql import Row
import py4j

spark = pyspark.sql.SparkSession.builder.appName('test').getOrCreate()
sc=spark.sparkContext


x = [Row(col1="xx", col2="yy", col3="zz", col4=[123,234])]
rdd = sc.parallelize([Row(col1="xx", col2="yy", col3="zz", col4=[123,234])])
df = spark.createDataFrame(rdd)
df.show()

