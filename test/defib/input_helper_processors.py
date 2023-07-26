global spark, logger

from rsp.types.applications import Step

spark = None
logger = None


class get_input_data(Step):
    """This class takes query and runs it

    Args:
        Step: get_features_step

    kwargs:
        query: string
            hive query to get the features for the model
        filelocation: string
            file location for local testing and unit testing
        filetype: string
            file type to read it in spark accordingly
    Returns:
        dataframe
    """

    def __init__(self, step_name, **kwargs):
        super().__init__(step_name, **kwargs)

    def prepare(self, **kwargs):
        if 'query' in kwargs:
            self.query = kwargs['query']
            logger.info(self.query)
        if 'filelocation' in kwargs:
            self.filelocation = kwargs['filelocation']
            logger.info(self.filelocation)
        if 'filetype' in kwargs:
            self.filetype = kwargs['filetype']
        if 'config' in kwargs:
            self.config = kwargs['config']
        pass

    def get_data(self):
        if hasattr(self, 'config'):
            logger.info("Running Query:" + self.config['query'])
            df = spark.sql(self.config['query'])
            logger.info(df.show(1))
            logger.info("Number of records: " + str(df.count()))

        elif hasattr(self, 'query'):
            logger.info("Running Query:" + self.query)
            df = spark.sql(self.query)
            logger.debug(df.show(1))

        elif hasattr(self, 'filelocation'):
            if self.filetype=='csv':
                df = spark.read.csv(self.filelocation, header=True, inferSchema=True)
                logger.debug(df.show(1))

            elif self.filetype=='parquet':
                df = spark.read.parquet(self.filelocation)
                logger.debug(df.show(1))
                logger.debug(df.count())
        else:
            logger.info("No query or file location provided to run_query step")
        return df

    def process(self) -> 'data':
        return self.get_data()

    def shutdown(self):
        pass


class get_local_data_subset(Step):
    """This class takes reads a local parquet

    Args:
        Step: get_local_features_step

    kwargs:
        filelocation: string
            file location for local testing and unit testing

    Returns:
        subset of dataframe
    """


    def __init__(self, step_name, **kwargs):
        super().__init__(step_name, **kwargs)

    def prepare(self, **kwargs):
        if 'filelocation' in kwargs:
            self.filelocation = kwargs['filelocation']
        pass

    def process(self) -> 'data':
        df = spark.read.parquet(self.filelocation)
        df.createOrReplaceTempView('defib_parquet')
        df = spark.sql("SELECT * FROM defib_parquet LIMIT 5")
        df.show()
        return df



class process_input_data(Step):
    """This class reads the  input dataframe and converts it into a pandas dataframe

    Args:
        Step: process_input_data_step

    kwargs:
        None

    Returns:
        pandas dataframe
    """

    def __init__(self, step_name, **kwargs):
        super().__init__(step_name, **kwargs)

    def prepare(self, **kwargs):
        pass

    def process(self, df:'data') -> 'data':
        return df.toPandas()