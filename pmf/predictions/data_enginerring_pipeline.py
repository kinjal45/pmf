from featurization import featurize_data_for_ml
import util
from predictions import make_and_save_predictions


def main(config, spark):

    featured_df = featurize_data_for_ml(spark, config)
    make_and_save_predictions(featured_df, config)
    
    util.s3_move(config["s3_raw_data_bucket"], '/'.join(config["raw_data_path"].split('/')[1:]),
                 config["proceeded_path"], config['s3_access_key'], config['s3_secret_key'],
                 config['s3_endpoint'])
    print('*********'*10)
    print('Predictions have been generated ')
    print('*********'*10)
