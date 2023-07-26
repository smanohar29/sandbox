import pyspark
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import StringType

spark = pyspark.sql.SparkSession.builder.appName('test').getOrCreate()

original_df = spark.read.format('csv').options(header='true', inferSchema='true').load('../data/blueprint_inventory_report_51424_20200506_40164.csv')

df = original_df.filter(F.col('active')=='yes')
df = df.filter(F.col('Dial Priority Score') != 0 )
df = df.filter(F.col('Credit Trigger Selection') == 0 )

# DP_A_1_4_5_2
# DP_A_1_4_5_with_DT1
# DP_A_1_4_5_with_DT2
# RandomControl


########## LOLA Scores ##########
df_group = df.select('Loan Number', 'Active', 'Dial Sequence', 'Dial Priority Model', 'Dial Priority Score', 'Relative Position Score')
df_group = df_group.filter(F.col('Dial Priority Model') == 'DP_A_1_4_5_2').orderBy('Dial Priority Score', ascending=False)
df_group = df_group.orderBy('Dial Sequence')

df_group_lola = df_group.withColumn('Relative Position Score', df_group['Relative Position Score'])
df_group_lola.createOrReplaceTempView('tempTable')
df_group_lola = spark.sql("SELECT *, ROW_NUMBER() OVER (ORDER BY `Relative Position Score` DESC) AS LOLA_Rank FROM tempTable")
df_group_lola.show(20)
df_group_lola.createOrReplaceTempView('lola_table')


########## Maestro Scores ##########
print(df_group.count())
tcount = int(df_group.count())


df_group.createOrReplaceTempView('tempTable')
df_group = spark.sql("SELECT *, ROW_NUMBER() OVER (ORDER BY `Dial Priority Score` DESC) AS Rank FROM tempTable")

def get_realtive_score(rank):
    score = 1- int(rank)/tcount
    return str(score)

get_realtive_score_UDF = udf(get_realtive_score, StringType())

df_group_maestro = df_group.withColumn('Maestro Relative Score', get_realtive_score_UDF(df_group['Rank']))
df_group_maestro.createOrReplaceTempView('tempTable')
df_group_maestro = spark.sql("SELECT *, ROW_NUMBER() OVER (ORDER BY `Maestro Relative Score` DESC) AS Maestro_Rank FROM tempTable")
df_group_maestro = df_group_maestro.select('Loan Number', 'Active', 'Dial Sequence', 'Dial Priority Model', 'Dial Priority Score', 'Relative Position Score', 'Maestro Relative Score', 'Maestro_Rank')
df_group_maestro.show(20, False)
df_group_maestro.createOrReplaceTempView('maestro_table')

diff_df = spark.sql(''' SELECT lo.`Loan Number`, lo.`Dial Priority Model`, lo.`Dial Priority Score`, `LOLA_Rank`, `Maestro_Rank`, `LOLA_Rank` - `Maestro_Rank` AS Diff
                                FROM lola_table lo 
                                INNER JOIN maestro_table ma
                                ON lo.`Loan Number` = ma.`Loan Number`
                            ''')

diff_df.show()

diff_df.filter(F.col('Diff') != 0).show()

