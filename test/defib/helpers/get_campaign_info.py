from pyspark.sql.window import Window
from pyspark.sql.functions import rank, max, sum
from pyspark.sql import functions as F
from pyspark.sql.functions import col

def get_loan_population(spark, timestamp1):

    datapath = "/uat/DataScience/Maestro/Defib/LoanPopulation.csv"
    campaignsDF = spark.read.csv(datapath, header=True, inferSchema=True, sep="|")

    campaignsDF.createOrReplaceTempView('campaignData')
    query = "SELECT * FROM campaignData WHERE {0} BETWEEN PoolStartDate AND PoolEndDate"

    print(query.format(timestamp1))
    campaignsDF = spark.sql(query.format(timestamp1))

    # campaignsDF = campaignsDF.filter(col("PoolStartDate") <= timestamp1).filter(col("PoolEndDate") >= timestamp1)
    campaignsDF = campaignsDF.select(col("loannumber"), col("loanpoolname").alias("campaignname"), col("poolstartdate").alias("campaignstartdate"), col("poolenddate").alias("campaignenddate"))

    return campaignsDF


def get_campaign_features(spark, timestamp1, df):

    config = {
        "defib": ["Big Gun COR All In", "Big Gun Precovery","Big Gun COR Second Voice","Big Gun COR Escalation","SLAE COR Precovery","Big Gun COR Foundation","Big Gun SC Rewrites", "Big Gun Escalation Rocket Mortgage Refinance"],
        "foundation": ["CARI Ocean", "Escalation Ocean", "LMB Exclusive", "Purchase Ocean", "Refinance Ocean", "Refinance Ocean 2", "Refinance Ocean 3"]
    }


    def getPreviousCampaigns(df):
        query = """  SELECT loannumber, campaignstartdate 
                            FROM lola_raw_access.campaignchangedevent_historical_vw 
                            WHERE datekey <= {0} AND campaignstartdate < {0} 
                            GROUP BY loannumber, campaignstartdate
                     """

        print("##### getPreviousCampaigns query #####")
        print(query.format(timestamp1))
        historicalcampaign_df = spark.sql(query.format(timestamp1))

        loans_df = df.select("loannumber").distinct()

        loans_campaigns_df = loans_df.join(historicalcampaign_df, 'loannumber')

        esc_window_spec = Window.partitionBy('loanNumber').orderBy('campaignstartdate')
        loans_campaigns_df = loans_campaigns_df.withColumn('escalationcount', rank().over(esc_window_spec))

        loans_campaigns_df = loans_campaigns_df.select("loannumber", "escalationcount").groupBy("loannumber").agg(max("escalationcount").alias("escalationcount"))

        df = df.join(loans_campaigns_df, 'loannumber')

        return df

    def addCampaignGroup(df):
        df_features_processed = df.withColumn('campaign_group', F.when(df.campaignname.isin(config['defib']), 'defib').when( df.campaignname.isin(config['foundation']), 'foundation').otherwise('other'))
        return df_features_processed

    df_features_processed = getPreviousCampaigns(df)
    df_features_processed = addCampaignGroup(df_features_processed)

    # df_features_processed.show(1)

    return df_features_processed
