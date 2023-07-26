#spark submit
/usr/hdp/current/spark2-client/bin/spark-submit --master yarn --deploy-mode cluster \
--queue BusinessIntelligence --name Defib_HistoricalData_Fix \
--principal 'svc-oozie-workflows' --keytab '/etc/security/keytabs/svc-oozie-workflows.keytab' \
--repositories http://repo.hortonworks.com/content/groups/public/ \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./envs/defib_env/bin/python \
--archives hdfs:///user/svc-oozie-workflows/DataScience/Maestro/Defib/2_0_0/defib_scoring/envs.zip#envs \
--num-executors 12 \
--executor-cores 6  \
--executor-memory 12G \
--driver-memory 18G \
--conf spark.executor.memoryOverhead=10G \
--conf spark.driver.memoryOverhead=10G \
--conf spark.yarn.maxAppAttempts=1 \
--files /etc/spark2/conf/hive-site.xml,configprops.tar.gz \
--py-files apps.zip,libs.zip main.py