import pyspark
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import StringType
import random
from pyspark.sql.functions import  col, lit, udf
from pyspark.sql.types import IntegerType
from datetime import date
import datetime
import pytz

spark = pyspark.sql.SparkSession.builder.appName('test').getOrCreate()
sc=spark.sparkContext

# from pyspark.sql import Row
# x = [Row(col1="xx", col2="yy", col3="zz", col4=[123,234])]
# rdd = sc.parallelize([Row(col1="xx", col2="yy", col3="zz", col4=[123,234])])
# df = spark.createDataFrame(rdd)
# df.show()

# df = df.withColumn("newcolumn", df["col4"][1])
# df.show()

from datetime import datetime

start = datetime.now()


data = spark.read.format('csv').options(header='true', inferSchema='true').load('../data/maestro_leadquality_scores_1.csv')
data = data.select('attributevalue').withColumn("newcolumn", F.lit(date.today()))

data.show()


end  = datetime.now()

timeTaken = end - start
print(timeTaken)