import pyspark
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import StringType

spark = pyspark.sql.SparkSession.builder.appName('test').getOrCreate()

original_df = spark.read.format('csv').options(header='true', inferSchema='true').load('../data/Maestro_Failure.csv')

loansField = original_df.select('linecount')
# loansField.show(1, False)

import pyspark.sql.functions as f

df_split = loansField.select(f.split(loansField.linecount, " ")).rdd.flatMap(lambda x: x).toDF()
df_split.show(5, False)

newDF = df_split.select(col("_27").alias("loannumber"))
# newDF.show(1)

def parse_lno(field):
    field = field.split('\\')[0]
    return field

parseUDF = udf(parse_lno, StringType())

newDF = newDF.withColumn('loannumber', parseUDF(newDF["loannumber"]))
newDF.show(5)

print(newDF.count())

loansList = newDF.select("loannumber").rdd.flatMap(lambda x: x).collect()
print(loansList)





