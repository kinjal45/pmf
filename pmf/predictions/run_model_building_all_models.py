import util
import sys
from pyspark.sql import SparkSession
import model_building_pipeline

if __name__ == "__main__":
    pmf_config = util.get_cfg_data(*sys.argv[1:])
    spark = SparkSession.builder.appName("pmf_model_building").getOrCreate()
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", pmf_config['s3_endpoint'])
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", pmf_config['s3_access_key'])
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", pmf_config['s3_secret_key'])
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.signing-algorithm", "S3SignerType")
    model_building_pipeline.main(pmf_config, spark)
