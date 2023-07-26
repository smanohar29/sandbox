#spark submit
/usr/hdp/current/spark2-client/bin/spark-submit --master yarn --deploy-mode cluster \
--queue BatchScoring --name Defib_Scoring_1_0_0_Prod \
--principal 'svc-maestro' --keytab '/etc/security/keytabs/svc-maestro.keytab' \
--repositories http://repo.hortonworks.com/content/groups/public/ \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./envs/defib_env/bin/python \
--archives hdfs:///prod/DataScience/Maestro/Defib/2_0_0/defib_scoring/envs.zip#envs \
--num-executors 10 \
--executor-cores 4  \
--executor-memory 4G \
--driver-memory 4G \
--conf spark.executor.memoryOverhead=5G \
--conf spark.driver.memoryOverhead=10G \
--conf spark.yarn.maxAppAttempts=1 \
--files /etc/spark2/conf/hive-site.xml,configprops.tar.gz \
--py-files apps.zip,libs.zip main.py --app=batch_score --dagmodule=defib.applications.defib_main --hive=true