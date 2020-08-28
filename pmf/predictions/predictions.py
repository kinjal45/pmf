from pyspark.sql import functions as f
from pyspark.sql.types import DoubleType, StringType
from pyspark.ml.tuning import CrossValidatorModel
import numpy as np
from datetime import datetime
import pandas as pd
import util


def make_and_save_predictions(input_df, config):
    print("*"*10)
    dt = input_df.select("refresh_date").first()[0]
    print("*"*10)

    model = CrossValidatorModel.load(config["model_path"])
    final_output_df = model.transform(input_df).withColumnRenamed("id", "JobID").withColumnRenamed("prediction", "JobDurationML")
    
    print(final_output_df.printSchema)
    prediction_path = config["prediction_path"] + "/year=" + str(dt.year) + "/month="+str(dt.month) + "/day="+str(dt.day)
    final_output_df.select(config["categorical_cols"] + config["numerical_cols"]+ ["JobID","JobDurationML"]).coalesce(1).write.mode('overwrite').option("header",'true').csv(prediction_path)
    


