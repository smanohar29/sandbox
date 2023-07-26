from pyspark.sql.functions import udf, when, col, lit
from pyspark.sql import functions as F


def get_conversion_data(spark, timestamp1, df_submission):
    query = """
            SELECT * 

            FROM (
                SELECT attributevalue,
                    conversionrate,
                    ROW_NUMBER() OVER (PARTITION BY datekey ORDER BY enddate DESC) as row
                FROM model_offline_feature_store.conversion_metrics
                WHERE datekey < {})t 

            WHERE row=1

        """

    print(query.format(timestamp1))
    conversionDF = spark.sql(query.format(timestamp1))
    conversionDF = conversionDF.cache()

    df_features_raw = df_submission.join(conversionDF, col('h_tleadtype') == col('attributevalue'), how='left').select(
        df_submission["*"], conversionDF["conversionrate"].alias("LTC_close_rate"))

    df_features_raw = df_features_raw.join(conversionDF, col('h_sstate') == col('attributevalue'), how='left').select(
        df_features_raw["*"], conversionDF["conversionrate"].alias("state_close_rate"))

    df_features_raw = df_features_raw.withColumn("adproperty_parsed", F.substring('h_tadproperty', 0, 3))

    df_features_raw = df_features_raw.join(conversionDF, col('adproperty_parsed') == col('attributevalue'),
                                           how='left').select(df_features_raw["*"],
                                                              conversionDF["conversionrate"].alias(
                                                                  "adproperty_close_rate"))

    # df_features_raw.show(1)

    return df_features_raw