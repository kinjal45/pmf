from featurization import featurize_data_for_ml
from model_building import build_and_save_model
import util


def main(config,  spark):
    featured_df = featurize_data_for_ml(spark, config, True)
    build_and_save_model(featured_df, config)
    util.s3_move(config["s3_raw_data_bucket"], '/'.join(config["completed"]["raw_data_path"].split('/')[1:]),
                 config["completed"]["proceeded_path"], config['s3_access_key'], config['s3_secret_key'],
                 config['s3_endpoint'])
    print("Model building is complete")
