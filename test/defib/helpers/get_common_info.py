spark = None
logger = None

from .feature_etl_helper import *
from pyspark.sql.functions import udf, when, col, lit
from datetime import datetime

def get_aggregate_features(df_calllevelfeaturesdata):

    addtoprevcallUDF = udf(add_one_to_prevcallcall, StringType())

    df_loanlevelcalls = df_calllevelfeaturesdata.groupBy("loannumber").agg(
        {"callingclientdatetime": "count", "earlymorningflag": "sum", "morningflag": "sum", "lunchtimeflag": "sum",
         "afternoonflag": "sum", "eveningflag": "sum", "nightflag": "sum",
         "localearlymorningflag": "sum", "localmorningflag": "sum", "locallunchtimeflag": "sum",
         "localafternoonflag": "sum", "localeveningflag": "sum", "localnightflag": "sum"})

    df_loanlevelcalls = df_loanlevelcalls.select(col("loannumber").alias("loannumber"),
                                                 col("count(callingclientdatetime)").alias("previouscallscount"),
                                                 col("sum(earlymorningflag)").alias("earlymorningcallcount"),
                                                 col("sum(morningflag)").alias("morningcallcount"),
                                                 col("sum(lunchtimeflag)").alias("lunchtimecallcount"),
                                                 col("sum(afternoonflag)").alias("afternooncallcount"),
                                                 col("sum(eveningflag)").alias("eveningallcount"),
                                                 col("sum(nightflag)").alias("nightcallcount"),
                                                 col("sum(localearlymorningflag)").alias(
                                                     "localearlymorningcallcount"),
                                                 col("sum(localmorningflag)").alias("localmorningcallcount"),
                                                 col("sum(locallunchtimeflag)").alias("locallunchtimecallcount"),
                                                 col("sum(localafternoonflag)").alias("localafternooncallcount"),
                                                 col("sum(localeveningflag)").alias("localeveningallcount"),
                                                 col("sum(localnightflag)").alias("localnightcallcount"))

    df_features_raw = df_calllevelfeaturesdata.join(df_loanlevelcalls, "loannumber", "left_outer")

    df_features_raw = df_features_raw.select("*").withColumn("rank", F.dense_rank().over(
        Window.partitionBy("loannumber").orderBy(F.col("callingclientdatetime").desc())))

    df_features_raw = df_features_raw.filter(F.col("rank") == 1)

    df_1 = df_features_raw.select("loannumber", "calldateid").groupBy("loannumber", "calldateid").agg(func.count(lit(1)).alias("callcount"))

    df_2 = df_features_raw.select("loannumber", "calldateid", "callingclientdatetime") \
        .groupBy("loannumber", "callingclientdatetime", "calldateid") \
        .agg(F.count(lit(1)).over(Window.partitionBy("loannumber").orderBy(F.col("callingclientdatetime"))).alias(
        "callcount"))

    df_features_raw_p = df_features_raw.alias("df_features_raw").join(df_1.alias("df_1"), (
                df_features_raw["loannumber"] == df_1["loannumber"]) & (df_features_raw["previousdaycalldateid"] ==
                                                                        df_1["calldateid"]), "left") \
        .select("df_features_raw.*") \
        .drop("df_1.callcount")

    df_features_raw = df_features_raw_p.alias("df_features_raw_p").join(df_2.alias("df_2"), (
                df_features_raw_p["loannumber"] == df_2["loannumber"]) & (df_features_raw_p[
                                                                              "calldateid"] == "test_date"), "left") \
        .select("df_features_raw_p.*", "df_2.callcount") \
        .withColumn('previousdaycallcount', addtoprevcallUDF(df_features_raw_p['previouscallscount'])) \
        .withColumn('currentdaycallcount', when(F.col("df_2.callcount").isNull(), 0).otherwise(F.col("df_2.callcount"))) \
        .orderBy("loannumber", "callingclientdatetime") \
        .drop("df_2.callcount")

    # df_features_raw.show(1)

    return df_features_raw


##############################################################################################################################


def update_feature_tables(df):

    # print("### Number of records: " + str(df.count()))
    df = df.withColumn('datekey', F.lit(datetime.now().date()))
    df = df.withColumn('insertdatetime', F.lit(datetime.now()))

    fieldListCurrent = ["loannumber","campaignname","campaignstartdate","campaignenddate","h_sloan","addressstatus","h_twebcredit","h_thome_search_status","h_thearaboutuscd","h_tgoals","h_lisva","h_tisservicing","h_scurrrate","typeoflead","h_twoupb","h_trealestateagent","h_tsalesprice","h_tupemclassic","h_tleasingmonth","h_tfirsttimehomebuyer","h_tlbfirsttimehomebuyer","h_tzip","h_toffer","upemmodeltype","h_tleasingyear","h_ttotmthlydebt","h_teveareaphone","h_tyearhomebought","h_tcity","h_tleasing","h_tupemgrade","h_tcurrentdate","h_tdownpayment","h_tcontactinfuture","correlationid","h_sselfcreditrating","h_tcurrbal","h_tbaseloanamt","h_tcashout","h_tppresentval","h_sstate","h_temail","h_tleadtype","h_tvenleadid","h_tadproperty","h_tdupsystemname","hasclientrefinancedbefore","h_tconfirmationemail","currentloanvendor","h_tprptytypcd","h_torigmorttype","leadcreatedate","ltc_close_rate","state_close_rate","adproperty_parsed","adproperty_close_rate","escalationcount","callingclientdatetime","preesc_previoustransfercount","preesc_previouscallcount","timezone","leadcreatehour","clientemaildomain","clientemailtopleveldomain","leadtimebucket","localleadcreatehour","localleadtimebucket","leadagedays","createdate","leadcreatedatetime","createdtid","loanpurpose","leadtypebucket","earlymorningflag","morningflag","lunchtimeflag","afternoonflag","eveningflag","nightflag","localearlymorningflag","localmorningflag","locallunchtimeflag","localafternoonflag","localeveningflag","localnightflag","dateid","calldateid","previousdaycalldateid","marketing_partner","previouscallscount","earlymorningcallcount","morningcallcount","lunchtimecallcount","afternooncallcount","eveningallcount","nightcallcount","localearlymorningcallcount","localmorningcallcount","locallunchtimecallcount","localafternooncallcount","localeveningallcount","localnightcallcount","callcount","previousdaycallcount","currentdaycallcount","calldayofweek","weekendflg","holidayflg","hourofdial","dialtimebucket","localhourofdial","localdialtimebucket","localcalltstarttime","insertdatetime","datekey","campaign_group"]
    current_df = df.select(*fieldListCurrent)
    # print(current_df.printSchema())
    current_df.repartition(1).write.insertInto("model_offline_feature_store_uat.lead_allocation_features_loans_1_0_0_current_fix", overwrite=True)

    fieldListHistorical = ["loannumber","campaignname","campaignstartdate","campaignenddate","h_sloan","addressstatus","h_twebcredit","h_thome_search_status","h_thearaboutuscd","h_tgoals","h_lisva","h_tisservicing","h_scurrrate","typeoflead","h_twoupb","h_trealestateagent","h_tsalesprice","h_tupemclassic","h_tleasingmonth","h_tfirsttimehomebuyer","h_tlbfirsttimehomebuyer","h_tzip","h_toffer","upemmodeltype","h_tleasingyear","h_ttotmthlydebt","h_teveareaphone","h_tyearhomebought","h_tcity","h_tleasing","h_tupemgrade","h_tcurrentdate","h_tdownpayment","h_tcontactinfuture","correlationid","h_sselfcreditrating","h_tcurrbal","h_tbaseloanamt","h_tcashout","h_tppresentval","h_sstate","h_temail","h_tleadtype","h_tvenleadid","h_tadproperty","h_tdupsystemname","hasclientrefinancedbefore","h_tconfirmationemail","currentloanvendor","h_tprptytypcd","h_torigmorttype","leadcreatedate","ltc_close_rate","state_close_rate","adproperty_parsed","adproperty_close_rate","escalationcount","callingclientdatetime","preesc_previoustransfercount","preesc_previouscallcount","timezone","leadcreatehour","clientemaildomain","clientemailtopleveldomain","leadtimebucket","localleadcreatehour","localleadtimebucket","leadagedays","createdate","leadcreatedatetime","createdtid","loanpurpose","leadtypebucket","earlymorningflag","morningflag","lunchtimeflag","afternoonflag","eveningflag","nightflag","localearlymorningflag","localmorningflag","locallunchtimeflag","localafternoonflag","localeveningflag","localnightflag","dateid","calldateid","previousdaycalldateid","marketing_partner","previouscallscount","earlymorningcallcount","morningcallcount","lunchtimecallcount","afternooncallcount","eveningallcount","nightcallcount","localearlymorningcallcount","localmorningcallcount","locallunchtimecallcount","localafternooncallcount","localeveningallcount","localnightcallcount","callcount","previousdaycallcount","currentdaycallcount","calldayofweek","weekendflg","holidayflg","dialtimebucket","localhourofdial","localdialtimebucket","localcalltstarttime","insertdatetime","campaign_group","datekey","hourofdial"]
    historical_df = df.select(*fieldListHistorical)
    # print(historical_df.printSchema())
    historical_df.repartition('campaign_group','datekey','hourofdial').write.insertInto("model_offline_feature_store_uat.lead_allocation_features_loans_1_0_0_historical_fix", overwrite=True)

    return current_df



