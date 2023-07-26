import pyspark
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import StringType
import random
from pyspark.sql.functions import  col, lit, udf
from pyspark.sql.types import IntegerType

spark = pyspark.sql.SparkSession.builder.appName('test').getOrCreate()

data = spark.read.format('csv').options(header='true', inferSchema='true').load('../data/maestro_leadquality_scores_1.csv')
# data.show(5, False)

# data = data.withColumn("attributecolumnname", F.lower(F.col("attributecolumnname")))
# data.show(5, False)

def getRandomScores(randomcolumn):
    return random.randint(1, 15)

getRandomNumberUDF = udf(getRandomScores, IntegerType())

data = data.withColumn('previousvoicemailcount', getRandomNumberUDF(data.attributecolumnname))
data.show(5, False)