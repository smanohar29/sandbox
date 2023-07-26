from rsp.types.applications import ApplicationDag
from ..input_helper_processors import get_input_data, process_input_data
from ..common_processors import pre_process
from ..output_processors import  update_scoring_table
from ..scoring_processors import score
from ..feature_etl_processors import get_simulated_features


def getDag(start_timestamp):

    dag = ApplicationDag("batch score")

    # Fix historical pipeline for defib
    simulated_feature_etl_step = get_simulated_features('get simulated features', datevalue=start_timestamp)

    # # Read data from feature table
    # get_features_step = get_input_data('get features', config="file://config/input/${meta.Environment}/input_features.json")
    # get_features_step.add_upstream(simulated_feature_etl_step)

    # Add necessary tra and convert to pandas
    process_input_data_step = process_input_data('process input data')
    # process_input_data_step.add_upstream(get_features_step)
    process_input_data_step.add_upstream(simulated_feature_etl_step)

    # Perform the required pre-processing
    preprocess_step = pre_process('pre-process', config="file://config/preprocessing/${meta.Environment}/Defib_1_0_0_RF.json")
    preprocess_step.add_upstream(process_input_data_step)

    # Score features
    score_step = score('score features', modelpath='file://models/1/0/0/GBM_lead.sav', applicationname='${meta.Application_Name}', environment='${meta.Environment}', config="file://config/input/${meta.Environment}/input_features.json")
    score_step.add_upstream(preprocess_step)

    # Writes data to output table
    update_scoring_table_step = update_scoring_table('save score results', config="file://config/output/${meta.Environment}/output_features.json")
    update_scoring_table_step.add_upstream(score_step)

    # dag.add_step(simulated_feature_etl_step)
    dag.add_step(update_scoring_table_step)

    return dag

