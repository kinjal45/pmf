import data_enginerring_pipeline
import util
import sys
from pyspark.sql import SparkSession


if __name__ == "__main__":
    print(sys.argv[1:])
    pmf_config = util.get_cfg_data(*sys.argv[1:])

    spark = SparkSession.builder.appName("pmf_predictions").getOrCreate()
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", pmf_config['s3_endpoint'])
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", pmf_config['s3_access_key'])
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", pmf_config['s3_secret_key'])
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.signing-algorithm", "S3SignerType")
    data_enginerring_pipeline.main(pmf_config, spark)
    
