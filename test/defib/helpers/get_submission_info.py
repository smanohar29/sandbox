
def get_submission_data(spark, timestamp1, campaignsDF):

    query = "SELECT * FROM leadallocation_conformed.lead_submissions WHERE receiveddate < {}"

    print(query.format(timestamp1))
    loansDF = spark.sql(query.format(timestamp1))

    df_features_raw = loansDF.join(campaignsDF, "loannumber")

    df_features_raw = df_features_raw.cache()
    # df_features_raw.show(1)

    return df_features_raw