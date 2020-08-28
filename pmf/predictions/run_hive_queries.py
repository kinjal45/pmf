import sys
import util

if __name__ == "__main__":
    pmf_config = util.get_cfg_data(*sys.argv[1:-1])
    input_model = sys.argv[-1]
    #for input_model in pmf_config["models_to_run"]:
    location_str = 's3a://{0}:{1}@{2}'.format(pmf_config["s3_access_key"], pmf_config["s3_secret_key"],
                                                  pmf_config[input_model]["predictions_path"])
    queries = ["drop table if exists {} purge".format(pmf_config[input_model]["table_name"]),
                   "create table if not exists {0} "
                   "(work_package_id string , region string , market string , package_name string ,"
                   " subcontractor string , site_code string , {1}_start_forecast date , {1}_complete_forecast date ,"
                   " customer_id integer , site_latitude float , site_longitude float , project_name string ,"
                   " site_class string , site_type string , site_agl float , site_state_province string , "
                   "configuration string , site_ref_model string , plan_type string , {1}_forecast double ,"
                   " refresh_date date) partitioned by (year int, month int, day int) "
                   "stored as parquet location {2}".format(
                       pmf_config[input_model]["table_name"], input_model, "'" + location_str + "'")
            , "msck repair table {}".format(pmf_config[input_model]["table_name"])]
    util.execute_hive_queries(queries, pmf_config)
    print('*********'*10)
    print("Hive query successful for {}".format(input_model))
    print('*********'*10)


