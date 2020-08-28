FROM ava-docker-local.esisoj70.emea.nsn-net.net/ava/customers/pmf:base1-spark-3

RUN  mkdir -p /opt/spark/pmf
RUN cd /opt/spark/jars/ &&     wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.11.813/aws-java-sdk-1.11.813.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-core/1.11.813/aws-java-sdk-core-1.11.813.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-dynamodb/1.11.813/aws-java-sdk-dynamodb-1.11.813.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-s3/1.11.813/aws-java-sdk-s3-1.11.813.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar && cd -
COPY requirements.txt ./
RUN pip install --upgrade pip && pip install -r requirements.txt 

COPY run_job.sh build_model.sh rest_apis_call.sh /
RUN chmod 755 /run_job.sh /build_model.sh /rest_apis_call.sh

COPY . /opt/spark/pmf/

ENTRYPOINT
