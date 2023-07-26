global spark, logger

from rsp.types.applications import Step
import pickle
from pyspark.sql.types import *
from pyspark.sql.functions import udf, lit
import json
from datetime import datetime
from rsp_spark_tools.data_sources.data_helper import generate_timestamp_col

spark = None
logger = None


class score(Step):
    """This class takes the preocessed data frame and scores it using
        the model object

    Args:
        Step: score_step

    kwargs:
        modelpath: string
            path where the model is stored

    Returns:
        pandas dataframe with associated score
    """

    def __init__(self, step_name, **kwargs):
        super().__init__(step_name, **kwargs)

    def prepare(self, **kwargs):
        self.model_path = kwargs['modelpath']
        self.applicationname = kwargs['applicationname']
        self.environment = kwargs['environment']
        self.config = kwargs['config']
        pass

    def load_model(self, model_path):
        model = pickle.load(open(model_path, 'rb'))
        return model

    def process(self, df:'data') -> 'data':
        model = self.load_model(self.model_path)
        predictions = model.predict(df)

        for i in range(len(predictions)):
           rowIndex = df.index[i]
           df.loc[rowIndex, 'prediction'] = predictions[i]
           # print ("Predicted outcome :: {}".format(predictions[i]))

        df = df[self.config['requiredFields']]

        with open(self.config['dataFrameSchemaPath'], "r") as read_file:
            json_data = json.load(read_file)

        new_schema = StructType.fromJson(json_data)
        # print(new_schema)

        res_df = spark.createDataFrame(df, schema=new_schema)
        res_df = res_df.drop('dateid')
        res_df = generate_timestamp_col(res_df, 'scoring_ts_utc', 'UTC')
        res_df = generate_timestamp_col(res_df, 'scoring_ts_est', 'EST')
        res_df = res_df.withColumn("datekey",  lit(datetime.now().date()))
        res_df.show(5, False)

        leads_scored = int(res_df.count())

        if leads_scored>0:
            categorical_metrics = {"MetricType": "Pipeline", "Application": self.applicationname, "Environment": self.environment, "Event": "Leads Scored"}
            numerical_metrics = {"NumberofLoans": leads_scored}
            logger.metric("DataScience.Defib", categorical_metrics, numerical_metrics)
        else:
            logger.error("##### Error during scoring #####")
            categorical_metrics = {"MetricType": "Error", "Application": self.applicationname, "Environment": self.environment, "Event": "Error"}
            numerical_metrics = {"NumberofLoans": leads_scored}
            logger.metric("DataScience.Defib", categorical_metrics, numerical_metrics)

        return res_df
