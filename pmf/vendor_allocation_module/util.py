import pandas as pd
import datetime as dt
import collections
import os
import shutil

import boto
import boto.s3.connection
from boto.s3.key import Key
import StringIO
import boto3
import json
import openpyxl
import io


def s3_move(
        bucket_name,
        source_path,
        target_path,
        aws_access_key_id,
        aws_secret_access_key,
        endpoint_url,
        add_date_to_filename=False):
    s3 = boto3.resource(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url='http://{}'.format(endpoint_url))
    for obj in s3.Bucket(bucket_name).objects.filter(Prefix=source_path):
        if obj.key[-1] == '/':
            continue
        copy_source = {'Bucket': bucket_name, 'Key': obj.key}
        file = obj.key.replace(source_path, target_path)
        if add_date_to_filename:
            file = file.replace('.csv', '_' + str(dt.date.today()) + '.csv')
        new_obj = s3.Bucket(bucket_name).Object(file)
        new_obj.copy(copy_source)
        obj.delete()


def local_to_s3(
        host,
        ACCESS_KEY,
        SECRET_KEY,
        bucket_name,
        root_path,
        s3_destination_path):
    s3_resource = boto3.resource(u's3',
                                 endpoint_url="http://" + host,
                                 aws_access_key_id=ACCESS_KEY,
                                 aws_secret_access_key=SECRET_KEY)
    my_bucket = s3_resource.Bucket(bucket_name)
    for path, subdirs, files in os.walk(root_path):
        path = path.replace("\\", "/")
        directory_name = path.replace(root_path, "")
        s3_destination_path = s3_destination_path.replace(
            bucket_name + '/', "")
        for file in files:
            my_bucket.upload_file(
                os.path.join(
                    path, file), os.path.join(
                    s3_destination_path, directory_name) + '/' + file)
    try:
        shutil.rmtree(root_path)
    except BaseException:
        print("error while deleting files!")


def execute_hive_queries(queries, pmf_config):
    import jaydebeapi
    database = pmf_config["thrift_db"]
    driver = 'org.apache.hive.jdbc.HiveDriver'
    server = pmf_config["thrift_url"]
    port = pmf_config["thrift_port"]

    # JDBC connection string
    url = ("jdbc:hive2://" + server + ":" + str(port) + "/" + database + ";")

    # Connect to HiveServer2
    conn = jaydebeapi.connect(
        driver,
        url,
        driver_args={
            "user": pmf_config["thrift_user"],
            "password": pmf_config["thrift_password"]})
    cursor = conn.cursor()
    for q in queries:
        cursor.execute(q)
        results = cursor.fetchall()
        print(results)


def get_cfg_data(
        s3_endpoint,
        s3_access_key,
        s3_secret_key,
        s3_bucket,
        s3_config_file_path):
    import traceback
    s3 = boto3.resource(
        's3',
        endpoint_url='http://' +
        s3_endpoint,
        aws_access_key_id=s3_access_key,
        aws_secret_access_key=s3_secret_key)
    obj = s3.Object(s3_bucket, s3_config_file_path)
    try:
        file_content = obj.get()['Body'].read()
        cfg_dict = json.loads(file_content)

        def convert(data):
            if isinstance(data, basestring):
                return str(data)
            elif isinstance(data, collections.Mapping):
                return dict(map(convert, data.iteritems()))
            elif isinstance(data, collections.Iterable):
                return type(data)(map(convert, data))
            else:
                return data
        return convert(cfg_dict)  # Converting unicode to string
    except Exception as e:
        traceback.print_exc()


def read_s3_csv_file(bucket, s3_dir, s3_filename):

    for o in bucket.list(prefix=s3_dir):
        if s3_filename in o.name.split('/'):
            content = bucket.get_key(o.name).get_contents_as_string()
            df = pd.read_csv(StringIO.StringIO(content), sep=',')
    try:
        return df
    except BaseException:
        print("File {} Not Found".format(s3_filename))


def create_s3_connection(
        endpoint_url,
        aws_access_key_id,
        aws_secret_access_key,
        s3_bucket_name):
    conn = boto.connect_s3(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        host=endpoint_url,
        # is_secure=False,               # uncomment if you are not using ssl
        calling_format=boto.s3.connection.OrdinaryCallingFormat(),
    )

    return conn.get_bucket(s3_bucket_name, validate=False)


def write_csv_in_s3(df, bucket, destFileName):

    op = pd.DataFrame.to_csv(df, index=False)
    k = Key(bucket, destFileName)
    k.set_contents_from_string(op)


def write_excel_in_s3(
        df,
        endpoint,
        access_key,
        secret_key,
        s3_bucket_name,
        config):
    now = dt.datetime.now()
    output = io.BytesIO()
    writer = pd.ExcelWriter(output, engine='openpyxl')
    df.to_excel(writer, index=False, sheet_name="Overlay")
    writer.save()
    path = str(config['s3_output_folder_path']) + "/year=" + str(now.year) + "/month=" + str(
        now.month) + "/gc_allocation_tmo_" + str(dt.datetime.now().strftime('%d%m%Y')) + ".xlsx"
    data = output.getvalue()
    s3 = boto3.resource(
        's3',
        endpoint_url='http://' +
        endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key)
    s3.Bucket(s3_bucket_name).put_object(Key=path, Body=data)
