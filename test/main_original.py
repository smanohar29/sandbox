from rsp_spark_tools.deployment import ApplicationRunner
from datetime import datetime
import sys

if __name__ == '__main__':
    try:
        start = datetime.now() 
        print("starting job at:" + str(start))

        ApplicationRunner.main(sys.argv[1:])

        end = datetime.now()
        runtime = end - start
        print("job complete at:" + str(end) + " in:" + str(runtime))

    except Exception as e:
        print("exception in __main__")
        print(e)
        raise