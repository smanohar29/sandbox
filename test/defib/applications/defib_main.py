from rsp.types.applications import ApplicationDag
from rsp.launcher.impl.assembler import ExperimentAssembler
from rsp.launcher.__init__ import ConfigHelper
from ..input_helper_processors import get_input_data, process_input_data
from ..common_processors import pre_process
from ..output_processors import  update_scoring_table
from ..scoring_processors import score


def getDag():
    dag = ApplicationDag("batch_control")

    assembly = ExperimentAssembler()

    assembly.configure(ConfigHelper.expandConfig('config/applications/batch_score.json')). \
        addOption(props="properties/common.properties")


    # Read data from feature table
    get_features_step = get_input_data('get features', config="file://config/input/${meta.Environment}/input_features.json")

    # Add necessary tra and convert to pandas
    process_input_data_step = process_input_data('process input data')
    process_input_data_step.add_upstream(get_features_step)

    # Perform the required pre-processing
    preprocess_step = pre_process('pre-process', config="file://config/preprocessing/${meta.Environment}/Defib_1_0_0_RF.json")
    preprocess_step.add_upstream(process_input_data_step)

    # Score features
    score_step = score('score features', modelpath='file://models/1/0/0/GBM_lead.sav', applicationname='${meta.Application_Name}', environment='${meta.Environment}', config="file://config/input/${meta.Environment}/input_features.json")
    score_step.add_upstream(preprocess_step)

    # Writes data to output table
    update_scoring_table_step = update_scoring_table('save score results', config="file://config/output/${meta.Environment}/output_features.json")
    update_scoring_table_step.add_upstream(score_step)

    dag.add_step(update_scoring_table_step)

    return dag