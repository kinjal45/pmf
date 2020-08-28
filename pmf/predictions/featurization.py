from pyspark.sql.functions import *
from pyspark.sql.types import DoubleType
import numpy as np
from pyspark.sql.window import Window
from pyspark.sql.types import *

schema = StructType([
    StructField('refresh_date', DateType()),
    StructField('id', StringType()),
    StructField('region', StringType()),
    StructField('market', StringType()),
    StructField('package_name', StringType()),
    StructField('site', StringType()),
    StructField('civil_start', DateType()),
    StructField('civil_end', DateType()),
    StructField('tower_start', DateType()),
    StructField('tower_end', DateType()),
    StructField('customer', StringType()),
    StructField('latitude', DoubleType()),
    StructField('longitude', DoubleType()),
    StructField('project_name', StringType()),
    StructField('site_type', StringType()),
    StructField('plan_type', StringType()),
    StructField('site_class', StringType()),    
    StructField('configuration', StringType())    
])



def featurize_data_for_ml(spark, config, building_model=False):
    if building_model:
        raw_df = spark.read.csv(config["s3_input_trained_data"],header=True,schema=schema).dropDuplicates(['id']).drop(*['configuration','plan_type','site_type','site_class']).withColumn('region', upper(col('region'))).withColumn('market', upper(col('market'))).withColumn('label', datediff('tower_end', 'civil_start')).where('label>=0').cache()
         
    else:
        path = "s3a://"+str(config['s3_raw_data_bucket'])+str(config["s3_input_file_path"])
        raw_df = spark.read.csv(path,header=True,schema=schema).dropDuplicates(['id']).drop(*['configuration','plan_type','site_type','site_class']).withColumn('region', upper(col('region'))).withColumn('market', upper(col('market'))).cache()
        
    # Handling missing values and Handling outliers
    if building_model:
        
        raw_df = raw_df.dropna(subset=config["numerical_cols"]+config["categorical_cols"], how='any')
        w = Window().partitionBy(['package_name'])
        raw_df = raw_df.withColumn('q',expr('percentile_approx(label,array(0.25,0.75))').over(w)).withColumn('lower',expr('q[0]-1.5*(q[1]-q[0])')).withColumn('upper',expr('q[1]+1.5*(q[1]-q[0])')).where('label>=lower and label<=upper')
        
    return raw_df
