
"""
To allocate crew and vendor for  entered, based on vendor skills, and availability.
"""


from va_data_processing import VADataProc
import time
import numpy as np
import util as Util
import pandas as pd
import datetime as dt
import sys
sys.path.insert(1, '/home/pmf/')


class RunVendorAllocation:
    def __init__(self):
        self.data_proc = VADataProc()
        print("Vendor Allocation 2.0 instantiated ")

    def start(self, s3_endpoint=None,
              s3_access_key=None,
              s3_secret_key=None,
              s3_bucket_name=None,
              s3_config_file_path=None):

        print("---" * 20)
        print("Starting Allocation of Vendors and Crew to jobs!")
        print("---" * 20)
        
        s = time.time()
        today = np.datetime64('today')
        
        print("Reading configuration file")
        pmf_config = Util.get_cfg_data(s3_endpoint,s3_access_key,s3_secret_key,s3_bucket_name,s3_config_file_path)
        config = pmf_config['vendor_allocation_module']
        config["today"] = today
        
        print("Reading files from s3 for {}".format(today))
        conn_bucket = Util.create_s3_connection(s3_endpoint,s3_access_key,s3_secret_key,config['s3_raw_data_bucket'])

        job_site_df = Util.read_s3_csv_file(conn_bucket,config['s3_input_folder_path'],config['job_site_file_name'])
        vendor_crew_df = Util.read_s3_csv_file(conn_bucket,config['s3_input_folder_path'],config['vendor_crew_file_name'])
        events = Util.read_s3_csv_file(conn_bucket,config['s3_input_folder_path'],config['events_file_name'])
        weather = Util.read_s3_csv_file(conn_bucket,config['s3_input_folder_path'],config['weather_file_name'])

        if (job_site_df is None) or (vendor_crew_df is None) or (events is None) or (weather is None):
            print("some files are missing!!!")
            sys.exit()

        holidays = self.data_proc.set_holidays(config['country_code'],config['calendar_start_year'],config['calendar_end_year'])
        
        print("Data Cleaning...")
        job_site_df = job_site_df.drop_duplicates(subset='JobID',keep='first')   
        job_site_df = self.data_proc.convert_date_format(job_site_df, config['job_site_field'])
        vendor_crew_df = self.data_proc.convert_date_format(vendor_crew_df, config['vendor_crew_field'])
        events = self.data_proc.convert_date_format(events, config['event_field'])
        
        print("Input data size {}".format(job_site_df.shape))
        job_site_df = job_site_df[job_site_df['CrewIdBooked']!= 'NAM Migration']
        print("Removing completed jobs from data")
        job_site_df = job_site_df[job_site_df['JobEndDateActual'].isnull()]
        job_site_df['Market'] = job_site_df['Market'].str.upper()
        vendor_crew_df = vendor_crew_df.dropna(subset=['VendorID','crew_name','ValidFrom','ValidUntil'],how='any')
        vendor_crew_df['Market'] = vendor_crew_df['Market'].str.upper()
        print("Cleaned Job data size {}".format(job_site_df.shape))
        
        # Save weather data into site_lock_dict
        self.data_proc.generate_site_lock_dict(weather)
        print(events['Market'].unique())
        events['Market'] = events['Market'].to_string(na_rep='').upper()
        events['Region'] = events['Region'].to_string(na_rep='').upper()
        
        print("Get qualified teams based on market, skills")
        jobs = self.data_proc.get_teams_based_on_skills_and_availability(job_site_df, vendor_crew_df)
        # JobStart
        cols = ['JobStartDateActual','JobStartDateForecast','SiteReadinessForecast','JobStart']
        jobs['JobStart'] = jobs['JobCreateDate'] + np.timedelta64(config["site_readiness_span"],'D')
        jobs['JobStart'] = jobs[cols].fillna(axis=1,method='bfill')
        
        print("filling default values for missing priority, job duration and job start offset")  
        # Add number of days between JobStart and JobEndForecast
        jobs['JobDuration'] = (jobs.JobEndDateForecast - jobs.JobStartDateForecast).dt.days + 1 + config['job_buffer']
      
        # Fill missing values in JobDuration
        jobs.loc[jobs['JobDuration'].isna(), 'JobDuration'] = jobs['JobDurationML'] 
        jobs.loc[jobs['JobDuration'].isna(), 'JobDuration'] = config['job_duration']

        # Fill NaN values with default_priority
        jobs.loc[jobs['Priority'].isna(),'Priority'] = config["job_default_priority"]
        # Fill missing values in JobStartMaxOffset 
        jobs.loc[jobs['JobStartMaxOffset'].isna(),'JobStartMaxOffset'] = config["scheduled_backlog_offset"]
        jobs = jobs.drop_duplicates(subset="JobID")
        print(jobs.shape)
        
        # Data validation using jobs data before saving into site_lock_dict
        self.data_proc.update_site_lock_dict(jobs,events)
        print("generating site distance matrix")
        self.data_proc.generate_site_distance(jobs)
        
        jobs['RecommendedVendor'] = jobs['RecommendedWorker'] = jobs['PlannedJobStart'] = np.nan
        tracking_codes = config['tracking_codes']
        print("Data Validation..")
        # Case when JobStartForecast > JobEndForecast
        where1 = jobs['JobStartDateForecast'] > jobs['JobEndDateForecast']

        # Case when JobEndForecast - JobStartForecast > job_length_max 
        where2 = jobs['JobDuration'] > config["job_duration_max"]

        # Case when BookedWorker is not null and JobStartActual is null and JobForecast is null 
        where3 = jobs['CrewIdBooked'].notna() \
            & jobs['JobStartDateActual'].isna() \
            & jobs['JobStartDateForecast'].isna()

        # Case when BookedWorker is null and JobStartActual <= today 
        where4 = jobs['CrewIdBooked'].isna() & (jobs['JobStartDateActual'] <= today)

        jobs.loc[where1|where2|where3|where4,'ResourceStatus'] = tracking_codes["5"][0]

        # Case when BookedWorker is null and JobStart + JobStartMaxOffset < today 
        where1 = jobs['CrewIdBooked'].isna() \
            & ((today - jobs['JobStart']).dt.days > jobs['JobStartMaxOffset'])

        # Case when BookedWorker is null and JobStart > today + schedule_end 
        where2 = jobs['CrewIdBooked'].isna() \
            & ((jobs['JobStart'] - today).dt.days > config["scheduled_backlog_offset"])

        jobs.loc[where1|where2,'ResourceStatus'] = tracking_codes["9"][0]

        # Case when BookedWorker is not null and no match with Workers data set
        where1 = jobs['ResourceStatus'].isna()
        where2 = jobs['CrewIdBooked'].notna() 
        where3 = jobs['CrewIdBooked'].isin(vendor_crew_df['crew_name'])

        jobs.loc[where1 & where2 & -where3,'ResourceStatus'] = tracking_codes["8"][0]

        # Case when BookedWorker is null and CountQualified = 0
        where1 = jobs['ResourceStatus'].isna()
        where2 = jobs['CrewIdBooked'].isna()
        where3 = jobs['CountQualified'] == 0

        jobs.loc[where1 & where2 & where3,'ResourceStatus'] =  tracking_codes["3"][0]

        print("Assigning Backlog Crew")

        # Assiging Backlog
        assigned_backlog = self.data_proc.filter_and_sort_on_column(
            jobs, (jobs.CrewIdBooked.notna() & jobs['ResourceStatus'].isna()), ['Priority', 'SiteReadinessForecast', 'JobStart', 'JobCreateDate'])

        assigned_backlog = self.data_proc.update_assigned_df(assigned_backlog)

        print("Assigning WIP Crew")

        where = (
            (jobs.CrewIdBooked.isnull()) & (
                jobs.JobStartDateForecast.notna()) & (
                jobs['ResourceStatus'].isna()))
        unassigned_backlog = self.data_proc.filter_and_sort_on_column(
            jobs, where, ['Priority', 'SiteReadinessForecast',  'JobStartDateForecast', 'JobCreateDate', 'CountQualified'])

        print("unassigned backlog ", unassigned_backlog.shape)
        unassigned_backlog = self.data_proc.recommend_workers_and_start_date(unassigned_backlog, config,holidays,['customer_id','SiteCode','Qualified','JobStartDateForecast','JobDuration','JobStartMaxOffset'])
        
        print("Assigning Pending Crew")

        unassigned_new = self.data_proc.filter_and_sort_on_column(
            jobs, (jobs.CrewIdBooked.isnull() & 
                jobs.JobStartDateForecast.isnull() & jobs['ResourceStatus'].isna()), [
                'Priority', 'SiteReadinessForecast','JobCreateDate', 'CountQualified'])
        print('unassigned new crews', unassigned_new.shape)
        
        unassigned_new = self.data_proc.recommend_workers_and_start_date(unassigned_new, config, holidays,['customer_id','SiteCode','Qualified','JobStart','JobDuration','JobStartMaxOffset'])
        
        cols = ['JobID','RecommendedVendor', 'RecommendedWorker', 'PlannedJobStart', 'refresh_date', 'ResourceStatus', 'JobDuration']
        output = pd.concat([ assigned_backlog[cols],  unassigned_backlog[cols],  unassigned_new[cols],  jobs.loc[jobs['ResourceStatus'].notna(),cols]],copy=False)

        jobs = output[['JobID',
                     'RecommendedVendor',
                     'RecommendedWorker',
                     'PlannedJobStart',
                     'refresh_date',
                     'ResourceStatus',
                     'JobDuration']]
        jobs.columns = [
            "WpId",
            "VendorId",
            "CrewId",
            "PlannedStartDate",
            "RefreshDate",
            "ResourceStatus",
            "JobDuration"]
        
        jobs["VendorId"].fillna("#", inplace=True)
        jobs["CrewId"].fillna("#", inplace=True)
        jobs["PlannedStartDate"].fillna("#", inplace=True)
        jobs["PlannedStartDate"].replace("nan", "#", inplace=True)
        
        Util.write_excel_in_s3(
            jobs,
            s3_endpoint,
            s3_access_key,
            s3_secret_key,
            s3_bucket_name,
            config)
        print("Writing Excel")
        y = str(jobs.RefreshDate.values[0]).split("-")[0]
        m = str(jobs.RefreshDate.values[0]).split("-")[1]
        Util.s3_move(config['s3_raw_data_bucket'],
                     config['s3_input_folder_path'],
                     config["s3_proceeded_folder"] + "/year={}/month={}".format(y,
                                                                                m),
                     s3_access_key,
                     s3_secret_key,
                     s3_endpoint,
                     add_date_to_filename=True)
        total_time = (time.time() - s) / 60
        print("time taken by the process {}".format(total_time))

if __name__ == "__main__":

    s3_endpoint, s3_access_key, s3_secret_key, s3_bucket_name, s3_config_file_path = sys.argv[1:]

    RunVendorAllocation().start(s3_endpoint,
                                s3_access_key,
                                s3_secret_key,
                                s3_bucket_name,
                                s3_config_file_path)
