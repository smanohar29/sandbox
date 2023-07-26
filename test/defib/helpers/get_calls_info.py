from pyspark.sql.functions import rank, max, sum, count as tcount
from pyspark.sql.functions import udf, when, col, lit
from .feature_etl_helper import *

def get_calls_features(spark, timestamp1, features_raw_df):

    query = """ SELECT loannumber as loannumber_calls, callingclientdatetime, transferflag AS  blendeddialer_transferflag 
                        FROM leadallocation_conformed.calls 
                        WHERE receiveddate <= {0} AND callingclientdatetime < {0} """
    calls_df = spark.sql(query.format(timestamp1))

    campaign_df = features_raw_df.select("loannumber", "campaignstartdate").distinct()

    campaign_df = campaign_df.join(calls_df, (col('loannumber') == col('loannumber_calls')) & (
                                    col('callingclientdatetime') < col('campaignstartdate')), how='left') \
                                    .select("LoanNumber", "campaignstartdate", "callingclientdatetime", "blendeddialer_transferflag") \
                                    .groupBy("loannumber", "campaignstartdate").agg(
                                    sum("blendeddialer_transferflag").alias("preesc_previoustransfercount"),
                                    tcount("callingclientdatetime").alias("preesc_previouscallcount"))

    campaign_df = campaign_df.na.fill(0, "preesc_previoustransfercount")
    campaign_df = campaign_df.select('loannumber', 'preesc_previoustransfercount', 'preesc_previouscallcount')

    features_raw_df = features_raw_df.join(campaign_df, 'loannumber')

    calls_df = calls_df.withColumnRenamed('loannumber_calls', 'loannumber')

    df_features_raw = features_raw_df.join(calls_df, 'loannumber')

    # df_features_raw.show(1)

    return df_features_raw


##############################################################################################################################


def get_call_level_features(df_calls):

    # 1.0
    timezoneUDF = udf(get_timezone, StringType())
    leadcreatehourUDF = udf(get_leadcreatehour, IntegerType())
    clientemaildomainUDF = udf(get_clientemaildomain, StringType())
    clientemailtopleveldomainUDF = udf(get_clientemailtopleveldomain, StringType())
    leadtimebucketUDF = udf(get_leadtimebucket, StringType())
    localhourUDF = udf(get_localhour, IntegerType())
    leadagedaysUDF = udf(get_leadagedays, StringType())
    converttoDateIDUDF = udf(converttoDateID, IntegerType())

    # 1.2
    loanpurposeUDF = udf(get_loanpurpose, StringType())
    leadtypebucketUDF = udf(get_leadtypebucket, StringType())

    # call features
    # 1.0
    hourofdialUDF = udf(get_hour, StringType())
    dialtimebucketUDF = udf(get_dialtimebucket, StringType())
    dateidUDF = udf(get_dateid, StringType())

    # 1.2
    calldateidUDF = udf(get_calldateid, StringType())
    previousdaycalldateidUDF = udf(get_previousdaycalldateid, StringType())

    df_features_raw = df_calls.withColumn('timezone', timezoneUDF(df_calls['h_sstate']))

    df_features_raw = df_features_raw.withColumn('leadcreatehour', leadcreatehourUDF(df_features_raw['leadcreatedate']))
    df_features_raw = df_features_raw.withColumn('clientemaildomain', clientemaildomainUDF(df_features_raw['h_temail']))
    df_features_raw = df_features_raw.withColumn('clientemailtopleveldomain', clientemailtopleveldomainUDF(df_features_raw['h_temail']))
    df_features_raw = df_features_raw.withColumn('leadtimebucket', leadtimebucketUDF(df_features_raw['leadcreatehour']))
    df_features_raw = df_features_raw.withColumn('localleadcreatehour', localhourUDF(df_features_raw['timezone'], df_features_raw['leadcreatehour']))
    df_features_raw = df_features_raw.withColumn('localleadtimebucket', leadtimebucketUDF(df_features_raw['localleadcreatehour']))
    df_features_raw = df_features_raw.withColumn('leadagedays', leadagedaysUDF(df_features_raw['leadcreatedate']))
    df_features_raw = df_features_raw.withColumn('createdate', df_features_raw.leadcreatedate)
    df_features_raw = df_features_raw.withColumn('leadcreatedatetime', df_features_raw.leadcreatedate)
    df_features_raw = df_features_raw.withColumn('createdtid', converttoDateIDUDF(df_features_raw.leadcreatedate))
    df_features_raw = df_features_raw.withColumn('loanpurpose', loanpurposeUDF(df_features_raw['h_sloan']))
    df_features_raw = df_features_raw.withColumn('leadtypebucket', leadtypebucketUDF(df_features_raw['h_tleadtype'], df_features_raw['h_tvenleadid'], df_features_raw['h_tadproperty']))
    df_features_raw = df_features_raw.withColumn('hourofdial', hourofdialUDF(df_features_raw['callingclientdatetime']))
    df_features_raw = df_features_raw.withColumn('localhourofdial', localhourUDF(df_features_raw['timezone'], df_features_raw['hourofdial']))
    df_features_raw = df_features_raw.withColumn('dialtimebucket', dialtimebucketUDF(df_features_raw['hourofdial']))
    df_features_raw = df_features_raw.withColumn('localdialtimebucket', dialtimebucketUDF(df_features_raw['localhourofdial']))

    df_features_raw = df_features_raw.withColumn('earlymorningflag',
                                                 F.when(df_features_raw.dialtimebucket == 'weemorning', 1).when(
                                                     df_features_raw.dialtimebucket == 'earlymorning', 1).otherwise(
                                                     0))
    df_features_raw = df_features_raw.withColumn('morningflag',
                                                 F.when(df_features_raw.dialtimebucket == 'morning', 1).otherwise(
                                                     0))
    df_features_raw = df_features_raw.withColumn('lunchtimeflag',
                                                 F.when(df_features_raw.dialtimebucket == 'lunchtime', 1).otherwise(
                                                     0))
    df_features_raw = df_features_raw.withColumn('afternoonflag',
                                                 F.when(df_features_raw.dialtimebucket == 'afternoon', 1).otherwise(
                                                     0))
    df_features_raw = df_features_raw.withColumn('eveningflag',
                                                 F.when(df_features_raw.dialtimebucket == 'evening', 1).otherwise(
                                                     0))
    df_features_raw = df_features_raw.withColumn('nightflag',
                                                 F.when(df_features_raw.dialtimebucket == 'night', 1).otherwise(0))

    df_features_raw = df_features_raw.withColumn('localearlymorningflag',
                                                 F.when(df_features_raw.localdialtimebucket == 'weemorning',
                                                        1).when(
                                                     df_features_raw.localdialtimebucket == 'earlymorning',
                                                     1).otherwise(0))
    df_features_raw = df_features_raw.withColumn('localmorningflag',
                                                 F.when(df_features_raw.localdialtimebucket == 'morning',
                                                        1).otherwise(0))
    df_features_raw = df_features_raw.withColumn('locallunchtimeflag',
                                                 F.when(df_features_raw.localdialtimebucket == 'lunchtime',
                                                        1).otherwise(0))
    df_features_raw = df_features_raw.withColumn('localafternoonflag',
                                                 F.when(df_features_raw.localdialtimebucket == 'afternoon',
                                                        1).otherwise(0))
    df_features_raw = df_features_raw.withColumn('localeveningflag',
                                                 F.when(df_features_raw.localdialtimebucket == 'evening',
                                                        1).otherwise(0))
    df_features_raw = df_features_raw.withColumn('localnightflag',
                                                 F.when(df_features_raw.localdialtimebucket == 'night',
                                                        1).otherwise(0))
    df_features_raw = df_features_raw.withColumn('dateid', dateidUDF(df_features_raw['callingclientdatetime']))

    df_features_raw = df_features_raw.withColumn('calldateid',
                                                 calldateidUDF(df_features_raw['callingclientdatetime']))
    df_features_raw = df_features_raw.withColumn('previousdaycalldateid',
                                                 previousdaycalldateidUDF(df_features_raw['callingclientdatetime']))

    df_features_raw = marketingpartner(df_features_raw)

    # df_features_raw.show(1)
    # df_features_raw.printSchema()

    return df_features_raw


##############################################################################################################################


def get_currenttime_call_features(spark, timestamp1, df_features_raw):

    def convertweekendflg(weekendflg):
        if weekendflg == True:
            return 1
        else:
            return 0

    def convertholidayflg(holidayflg):
        if holidayflg == True:
            return 1
        else:
            return 0

    datapath = "/prod/Maestro/DialingPrioritizationStrategy/QLODS.dbo.DateDim.csv"

    datedim = spark.read.csv(datapath, header=True, inferSchema=True, sep="|")

    datedim = datedim.select('dateid',
                             'date',
                             'dayofweekname',
                             'weekendflg',
                             'holidayflg')

    datedim_filtered = datedim.filter(datedim.dateid > lit(timestamp1.split()[0]))

    datedim_filtered = datedim_filtered.select(col("dateid").alias("dateid"),
                                               col("date").alias("date"),
                                               col("dayofweekname").alias("calldayofweek"),
                                               col("weekendflg").alias("weekendflg"),
                                               col("holidayflg").alias("holidayflg"))

    convertweekendflgUDF = udf(convertweekendflg, IntegerType())
    convertholidayflgUDF = udf(convertholidayflg, IntegerType())
    localcallstarttimeUDF = udf(get_localhour, StringType())

    datedim_filtered = datedim_filtered.withColumn('weekendflg', convertweekendflgUDF(datedim_filtered['weekendflg']))
    datedim_filtered = datedim_filtered.withColumn('holidayflg',  convertholidayflgUDF(datedim_filtered['holidayflg']))

    df_features_raw = df_features_raw.join(datedim_filtered, "dateid", "left_outer")
    df_features_raw = df_features_raw.withColumn('localcalltstarttime', localcallstarttimeUDF(df_features_raw['timezone'], df_features_raw['hourofdial']))

    # df_features_raw.show(1)
    # df_features_raw.printSchema()

    return df_features_raw

