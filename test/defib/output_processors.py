global spark, logger 

from rsp.types.applications import Step

spark = None
logger = None

class update_scoring_table(Step):
    """This class takes a dataframe and writes it into a table

    Args:
        Step: score_step

    kwargs:
        config: string
            path to config file which holds the name of the HIVE table that houses scores

    Returns:
    """
    def __init__(self, step_name, **kwargs):
        super().__init__(step_name, **kwargs)

    def prepare(self, **kwargs):
        self.config = kwargs['config']
        pass

    def update_table(self, df, config):

        df = df.select(*config['predictions']['outputColumns'])

        df.repartition('datekey').write.insertInto(config['tableName'], overwrite=False)

        current_df.repartition(1).write.insertInto("model_offline_feature_store_uat.lead_allocation_features_loans_1_0_0_current_fix", overwrite=True)

    def process(self, df: 'data') -> 'data':
        return self.update_table(df, self.config)

