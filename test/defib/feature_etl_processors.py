global spark, logger
from rsp.types.applications import Step
from datetime import datetime, timedelta

from .helpers.get_campaign_info import get_loan_population, get_campaign_features
from .helpers.get_submission_info import get_submission_data
from .helpers.get_conversion_info import get_conversion_data
from .helpers.get_calls_info import get_calls_features, get_call_level_features, get_currenttime_call_features
from .helpers.get_common_info import get_aggregate_features, update_feature_tables

spark = None
logger = None

class get_simulated_features(Step):

    def __init__(self, step_name, **kwargs):
        super().__init__(step_name, **kwargs)

    def prepare(self, **kwargs):
        self.start_timestamp = kwargs['datevalue']
        pass

    def process(self) -> 'data':
       return self.getSimulatedFeatures()


    def getSimulatedFeatures(self):
        start_timestamp = self.start_timestamp

        # Step 1
        loanPopDF = get_loan_population(spark, start_timestamp)

        # Step 2
        subDF = get_submission_data(spark, start_timestamp, loanPopDF)

        # Step 3
        featuresDF = get_conversion_data(spark, start_timestamp, subDF)

        # Step 4
        featuresDF = get_campaign_features(spark, start_timestamp, featuresDF) #add recevieddate <= simulateddate

        # Step 5
        featuresDF = get_calls_features(spark, start_timestamp, featuresDF) #add recevieddate <= simulateddate

        # Step 6
        featuresDF = get_call_level_features(featuresDF)

        # Step 7
        featuresDF = get_aggregate_features(featuresDF)

        # Step 8
        featuresDF = get_currenttime_call_features(spark, start_timestamp, featuresDF)

        #Step 9
        featuresCurrDF = update_feature_tables(featuresDF)

        return featuresCurrDF




