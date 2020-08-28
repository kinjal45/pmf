import pandas as pd
import numpy as np
import datetime as dt
import math
import openpyxl
from collections import defaultdict
from workalendar.registry import registry
from sklearn.neighbors import DistanceMetric


class VADataProc:
    def __init__(self):
        self.site_lock_dict = defaultdict(set)
        self.resource_lock_dict = defaultdict(set)
        print("VA Data Process initialised")

    def generate_site_distance(self,jobs):
        print("Create inter site distance matrix")
        
        self.site_distance = jobs[['customer_id','SiteCode','Latitude','Longitude']].dropna().drop_duplicates(keep='first')
        if self.nearest_site_lock():
            haversine = DistanceMetric.get_metric('haversine')
            self.site_distance['SiteID'] = list(zip(*[self.site_distance[c] for c in ['Customer','Site']]))
            self.site_distance['Latitude'] = np.radians(self.site_distance['Latitude'])
            self.site_distance['Longitude'] = np.radians(self.site_distance['Longitude'])
            self.site_distance_cols = self.site_distance['SiteID'].unique()
            self.site_distance = pd.DataFrame(
                haversine.pairwise(self.site_distance[['Latitude','Longitude']].to_numpy())*6373,
                columns=site_distance_cols,
                index=site_distance_cols)          

    def generate_site_lock_dict(self,weather):
        for customer,site,dates in zip(*[weather[c].values for c in ['CustomerID','SiteID','Date']]):
            self.site_lock_dict[customer,site].add(np.datetime64(dates,'D'))
    
    def update_site_lock_dict(self,jobs,events):
        col_x = ['CustomerID','SiteCode','Market','Region','ValidFrom','ValidUntil']
        col_y = ['customer_id','SiteCode','Market','Region']
        for customer_x,site_x,market_x,region_x,start,end in zip(*[events[c] for c in col_x]):
            dates = self.date_range(start,end)
            print(dates)
            for customer_y,site_y,market_y,region_y in zip(*[jobs[c] for c in col_y]):
                if customer_x == customer_y:
                    if site_x == site_y:
                        self.site_lock_dict[customer_y,site_y].update(dates)
                    elif pd.isna(site_x):
                        if market_x == market_y: 
                            self.site_lock_dict[customer_y,site_y].update(dates)
                        elif region_x == region_y: 
                            self.site_lock_dict[customer_y,site_y].update(dates)
                        elif pd.isna(market_x) and pd.isna(region_x): 
                            self.site_lock_dict[customer_y,site_y].update(dates)            
                elif pd.isna(customer_x):
                    if site_x == site_y:
                        if market_x == market_y:
                            self.site_lock_dict[customer_y,site_y].update(dates)    
                        elif region_x == region_y:
                            self.site_lock_dict[customer_y,site_y].update(dates)    
                        elif pd.isna(market_x) and pd.isna(region_x): 
                            self.site_lock_dict[customer_y,site_y].update(dates)            
                    elif pd.isna(site_x):
                        if market_x == market_y:
                            self.site_lock_dict[customer_y,site_y].update(dates)    
                        elif region_x == region_y:
                            self.site_lock_dict[customer_y,site_y].update(dates)
                        elif pd.isna(market_x) and pd.isna(region_x):
                            self.site_lock_dict[customer_y,site_y].update(dates)
        
    def busday_offset(self,start,offset):
        return np.busday_offset(start.astype('M8[D]'),offset,'forward',weekmask,holidays)

    def date_range(self,start,end):
        
        return set(pd.date_range(start, end ))

    def nearest_site_lock(self):
        return self.site_distance.shape[0]>1

    def nearest_site_lock_update(self,customer,site,dates):
        nearest_sites = self.site_distance[self.site_distance[customer,site] < self.site_distance_min].index
        return [self.site_lock_dict[s].update(dates) for s in nearest_sites if s[0]==customer]

    def resource_lock(self,vendor,worker,dates):
        return not(self.resource_lock_dict.get(vendor,worker) is None or self.resource_lock_dict[vendor,worker].isdisjoint(dates))

    def resource_lock_update(self,vendor,worker,dates):
        return self.resource_lock_dict[vendor,worker].update(dates)

    def site_lock(self,customer,site,dates):
        return not(self.site_lock_dict.get(customer,site) is None or self.site_lock_dict[customer,site].isdisjoint(dates))

    def set_holidays(self, country_code, start, end):
        '''
        Returns country-specific holidays between start and end.

        '''
        holidays_country = []
        years = range(start, end + 1)
        if len(country_code) > 0:
            calendar = registry.get_calendar_class(country_code)()
            holidays_country = list(
                map(np.datetime64, set().union(*map(calendar.holidays_set, years))))
        return holidays_country

    def tracker(self,customer,site,vendor,worker,start,length):
        resource_status = 0
        end = self.busday_offset(start,length + job_buffer)    
        d1 = self.date_range(start,end)
        d2 = d1.union(self.date_range(end,end + job_downtime))
        if self.site_lock(customer,site,d1):
            resource_status = 6
        elif self.resource_lock(vendor,worker,d2):
            resource_status = 2
        else:
            self.resource_lock_update(vendor,worker,d2)
            if self.nearest_site_lock():
                self.nearest_site_lock_update(customer,site,d1)        
        return resource_status

    
    def scheduler(self,customer,site,workers,start,length,offset_max):
        resource_status = 4
        recommended_vendor = recommended_worker = np.nan    
        offset = 0
        offset_max = offset_max - (today - start)/np.timedelta64(1,'D')*(today > start)
        start = max(start, today + schedule_start)
        while (offset <= offset_max) and (resource_status!=0):
            vendor = worker = planned_start = np.nan
            start_offset = self.busday_offset(start,offset)
            if start_offset <= today + schedule_end:
                end_offset = self.busday_offset(start_offset,length + job_buffer)
                d1 = self.date_range(start_offset,end_offset)
                d2 = d1.union(self.date_range(end_offset,end_offset + job_downtime))
                if self.site_lock(customer,site,d1):
                    resource_status = 6
                else:
                    for vendor, worker, valid_from, valid_until in workers:
                        valid_dates = (start_offset >= valid_from) and (end_offset <= valid_until)
                        if valid_dates and not self.resource_lock(vendor,worker,d2):
                            recommended_vendor = vendor
                            recommended_worker = worker
                            planned_start = start_offset
                            resource_status = 0
                            self.resource_lock_update(vendor,worker,d2)
                            if self.nearest_site_lock():
                                self.nearest_site_lock_update(customer,site,d1)                
                            break
            offset+=1
        return resource_status,recommended_vendor,recommended_worker,planned_start


    def get_teams_based_on_skills_and_availability(
            self, job_site_df, vendor_crew_df):
        # Select workerIDs by market

        by_market = job_site_df.merge(vendor_crew_df, how='inner', on='Market')

        # Select workerIDs by vendor
        where = (
            by_market['VendorBooked'] == by_market['VendorID']) | (
            by_market['VendorBooked'].isnull())
        by_vendor = by_market[where]

        # Select workerIDs by matching attributes subsets with job features
        def f(s):
            return set([x.strip() for x in s.split('|')])

        by_attributes = by_vendor[by_vendor.apply(lambda s: set([w.strip() for w in s['Skills'].split(
            '|')]) <= set([w.strip() for w in s['Skills1'].split('|')]), axis=1)]

        cols = ['Score','VendorID','crew_name']
        by_score = by_attributes.sort_values(by=cols,ascending=False)
        by_score = by_score.assign(
            Qualified=list(
                zip(
                    by_score['VendorID'],
                    by_score['crew_name'],                    
                    by_score['ValidFrom'].values.astype('M8[D]'),
                    by_score['ValidUntil'].values.astype('M8[D]')))).groupby(['JobID']).agg({'Qualified': list}).merge(job_site_df, how='right',on=['JobID'])

        by_score.loc[by_score['Qualified'].isna(), 'Qualified'] = ['']
        by_score['CountQualified'] = by_score['Qualified'].str.len()
        by_score['CountQualified'] = by_score.Qualified.apply(lambda x: 0 if x is None else len(x))
        vendor_crew_df.rename(columns={'region': 'Region'}, inplace=True)
        by_score = by_score.merge(vendor_crew_df[[
                                  "Market", "Region"]], on="Market", how='left').drop_duplicates(subset="JobID")
        return by_score

    def create_matrix(self, min_date, max_date, col_index, fill_value=0):

        dateIndex = pd.date_range(start=min_date, end=max_date)

        C0 = pd.DataFrame(
            index=dateIndex,
            columns=col_index).fillna(fill_value)
            
        """
        for i in crewIndex:
            C0.loc[vendor_crew_df[vendor_crew_df['crew_name'] ==i]['ValidFrom'].values[0]: vendor_crew_df[vendor_crew_df['crew_name'] ==i]['ValidUntil'].values[0],[i]] =1
        """

        return C0

    def create_inter_site_distance_matrix(self, site_distance):
        haversine = DistanceMetric.get_metric('haversine')
        site_distance['Latitude'] = np.radians(site_distance['Latitude'])
        site_distance['Longitude'] = np.radians(site_distance['Longitude'])
        site_distance = pd.DataFrame(
            haversine.pairwise(site_distance[['Latitude', 'Longitude']].to_numpy()) * 6373,
            columns=site_distance.SiteCode.unique(),
            index=site_distance.SiteCode.unique()
        )
        return site_distance

    def convert_date_format(self, df, config):
        for c in config['date']:
            df[c] = pd.to_datetime(df[c], format='%Y-%m-%d', errors='coerce').values.astype('M8[D]')
        return df

    def filter_and_sort_on_column(self, df, where, sorted_columns):
        return df[where].sort_values(sorted_columns)

    def update_capacity_matrix(self, C0, df, config):
        C1 = C0.copy()

        # Update Capacity matrix with crew-assigned JobIDs by roll-out managers
        for idx, row in df.iterrows():
            i = C1[C1.index == row.JobStartDateForecast].index.tolist()[0]
            imax = i + int(row.JobDuration) - 1 + config['workers_downtime']
            C1.loc[i:imax, row.CrewIdBooked] -= 1
        return C1

    def update_assigned_df(self,df):
        res = []
        cols = ['customer_id','SiteCode','VendorBooked','CrewIdBooked','JobStart','JobDuration']    
        for customer,site,vendor,worker,start,length in zip(*[df[c].values for c in cols]):
            res.append(self.tracker(customer,site,vendor,worker,start,length))
        df['ResourceStatus'] = res
        return df

    def recommend_workers_and_start_date(self, df,config,days,cols):
        global today,holidays,schedule_end,schedule_start, weekmask,job_buffer,job_downtime
        today= config["today"]
        holidays=days
        schedule_end = config["scheduled_backlog_offset"]
        job_buffer = config["job_buffer"]
        job_downtime = 2
        schedule_start = config["job_start_buffer"]
        weekmask = config["workers_weekmask"]
        res = []     
        for customer,site,workers,start,length,offset_max in zip(*[df[c].values for c in cols]):
            res.append(self.scheduler(customer,site,workers,start,length,offset_max))
        if res: 
            df[['ResourceStatus','RecommendedVendor','RecommendedWorker','PlannedJobStart']] = res
        return df

    def add_recommend_vendor(self, vendor_crew_df, jobs, config):
        recommended_vendor = vendor_crew_df[['crew_name', 'VendorID']] .drop_duplicates(
        ) .rename(columns={'crew_name': 'RecommendedWorker', 'VendorID': 'RecommendedVendor'})
        jobs = jobs.merge(
            recommended_vendor,
            on='RecommendedWorker',
            how='left')
        jobs['RecommendedWorker'] = jobs['RecommendedWorker'].astype(str)

        # Fill NA values in RecommendedVendor where RecommendedWorker =
        # resource_shortage_msg
        jobs.loc[jobs['RecommendedWorker'] == config['resource_shortage_msg'],
                 'RecommendedVendor'] = config['resource_shortage_msg']
        return jobs

    def remove_columns(self, df, cols_to_be_removed):
        for c in cols_to_be_removed:
            if c in df.columns:
                del df[c]
        return df
