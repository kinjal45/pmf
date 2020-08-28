#!/bin/bash
export CLASSPATH=$CLASSPATH:/opt/spark/extra-jars/hive_thrift_dependencies.jar
declare -i file_present

while true
    do
    echo "--------------------------------------------------------------------------"
    echo "checking for files $S3_WIP_PATH"
    echo "--------------------------------------------------------------------------"
    file_present=`s3cmd -c /opt/spark/pmf/s3-nsn-franklinpark ls $S3_WIP_PATH --recursive| wc -l`

    if [ "$file_present" -ge 1 ];
    then
         echo "--------------------------------------------------------------------------"
         echo "Construction file found | Starting Evaluation ..."
         echo "--------------------------------------------------------------------------"
         s3cmd -c /opt/spark/pmf/s3-nsn-franklinpark setacl --recursive $S3_WIP_PATH
         
	 /opt/spark/bin/spark-submit --jars /opt/spark/jars/hadoop-aws-3.2.0.jar,/opt/spark/jars/aws-java-sdk-1.11.813.jar,/opt/spark/jars/aws-java-sdk-s3-1.11.813.jar,/opt/spark/jars/aws-java-sdk-core-1.11.813.jar,/opt/spark/jars/aws-java-sdk-dynamodb-1.11.813.jar /opt/spark/pmf/run_predictions_all_models.py $S3_END $S3_ACC $S3_SEC $S3_BUCKET $S3_CONFIG_PATH 
    fi;
    sleep 10m

done
