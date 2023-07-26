global spark, logger 

from rsp.types.applications import Step
import numpy as np
import pandas as pd


spark = None
logger = None

class pre_process(Step):
    """This class takes a dataframe and updates it with defined pre-processing
    
    Args: 
        Step: preprocess_step
    
    kwargs:
        config: string 
            path to config file which holds the mapping for pre-processing steps

    Returns:
        processed dataframe 
    """

    def __init__(self, step_name, **kwargs):
        super().__init__(step_name, **kwargs)

    def prepare(self, **kwargs):
        self.config = kwargs['config']
        pass

    def process(self, df: 'data') -> 'data':
       return self.allPreprocess(df, self.config)


    def allPreprocess(self, dataframe, config ):
        """This function pulls together all of the above functions and does
        additional data manipulation. This preprocessing script will be called
        on multiple data sets to prep for variable monitor comparisons.
        Args:
            self: class instance with spark information for logging
            dataframe: Pandas dataframe
            config: to do the necessary pre-processing
        Returns:
            Completely processed dataframe.
        """

        def lower_keys(x):
            if isinstance(x, str):
                return (x.lower())
            if isinstance(x, list):
                return [lower_keys(v) for v in x]
            elif isinstance(x, dict):
                return dict((k.lower(), lower_keys(v)) for k, v in x.items())
            else:
                return x


        def fix_categorical(data, config):
            """This makes sure all categorical variables are strings.
            Args:
                data: pandas dataframe
                config: dictionary with key categorical, with a list as it's value that stores categorical columns
            Returns:
                data with corrected data types
            """

            for col in config['categorical']:
                try:
                    # make string but keep nulls
                    data[col] = np.where(pd.isnull(data[col]), data[col], data[col].astype(str))
                    data[col] = data[col].str.lower()  # make lowercase
                except Exception:
                    print("Problem processing column in fix_categorical - " + col)
                    raise

            return data

        def fix_numerical(data, config):
            """This makes sure all numerical variables are floats.
            Args:
                data: pandas dataframe
                config: dictionary with key continuous, with a list as it's value that stores continuous columns
            Returns:
                data with corrected data types
            """

            for col in config['continuous']:
                try:
                    data[col] = pd.to_numeric(data[col], errors='coerce')
                    data[col] = data[col].astype(np.float32)
                except Exception:
                    print("Problem processing column in fix_numericals - " + col)
                    raise

            return data

        def updateMappings(data, config):
            """This function maps similar values to a common value.
            Args:
                data: pandas dataframe
                config: a dictionary with mapping as a key, with proper mapping structure as value
            Returns:
                data with simplified categories
            """

            for col in config['mapping']:
                data[col['column']] = data[col['column']].replace(col['map'])
            return data

        def minorityFeatures(data, config):
            """This function takes categorical variables with many categories and simplifies them to only include categories
            listed in a keep set (stored in config).
            Args:
                data: pandas dataframe
                config: config with fix_minority_categories as a key, with proper config/this function setup
            Returns:
                simplified pandas dataframe without minority categories
            """

            for recipe in config['fix_minority_categories']:
                data[recipe['column']] = [x if x in recipe['keep_set'] or not x else recipe['other_value'] for x in
                                          data[recipe['column']]]
            return data

        def credit_buckets(num_record, cat_record):
            """This function helps combine credit score features into bucketted credit scores.
            Args:
                num_record: numerical credit score
                cat_record: categorical credit score
            Returns:
                If exists, a bucketted version of the num_record else the cat_record.
            """
            if num_record > 850:
                rating = cat_record
            elif num_record >= 720:
                rating = 'excellent'
            elif num_record >= 660:
                rating = 'good'
            elif num_record >= 620:
                rating = "average"
            elif num_record >= 580:
                rating = 'below average'
            elif num_record <= 579:
                rating = 'poor'
            else:
                rating = cat_record
            return rating

        def unify_credit(data):
            """Calls credit_bucket() function to make credit_bucket column.
            Args:
                data: pandas dataframe with h_sselfcreditrating and h_twebcredit columns
            Returns:
                a pandas dataframe with a new credit_bucket column
            """

            data['credit_bucket'] = [credit_buckets(num, cat) for num, cat in
                                     zip(data.h_sselfcreditrating, data.h_twebcredit)]
            return data

        def fix_negative_leadage(data):
            """This defaults all negative leadagedays to zero"""

            data['leadagedays'] = [x if x >= 0 else 0 for x in data.leadagedays]
            return data

        def preprocess(dat, config):
            """This preprocess function calls the preprocessing functions in the correct order. This is to be done
            before we use the sklearn pipeline object.

            Args:
                dat: pandas dataframe
                config: dictionary with appropriate fields for preprocessing
            Returns:
                partially preprocessed pandas dataframe ready for sklearn pipeline
            """
            dat.columns = dat.columns.str.lower()
            dat1 = fix_categorical(dat, config)
            dat2 = fix_numerical(dat1, config)
            dat3 = updateMappings(dat2, config)
            dat4 = minorityFeatures(dat3, config)
            dat5 = unify_credit(dat4)
            dat6 = fix_negative_leadage(dat5)

            return dat6

        return preprocess(dataframe, config)


