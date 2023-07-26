from rsp.types.applications import ApplicationDag
from rsp.launcher.impl.assembler import ExperimentAssembler
from rsp.launcher.__init__ import ConfigHelper
from . import etl_dag
from datetime import datetime, timedelta


def getDag():

    start = datetime.now()
    print("starting job at:" + str(start))

    dag = ApplicationDag("batch_control")

    assembly = ExperimentAssembler()

    assembly.configure(ConfigHelper.expandConfig('config/applications/batch_score.json')). \
        addOption(props="properties/test_common.properties")

    start_timestamp = "2020-06-01 07:30:00.000"
    end_timestamp = "2020-06-02 07:30:00.000"

    start_timestamp = datetime.strptime(start_timestamp, '%Y-%m-%d %H:%M:%S.%f')
    end_timestamp = datetime.strptime(end_timestamp, '%Y-%m-%d %H:%M:%S.%f')

    dag = etl_dag.getDag("'" + str(start_timestamp) + "'")


    end = datetime.now()
    runtime = end - start
    print("job complete at: " + str(end) + " in: " + str(runtime))

    return dag

