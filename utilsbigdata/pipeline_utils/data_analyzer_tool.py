import pandas as pd
import numpy as np
import requests
from utilsbigdata.pipeline_utils import retrieve_data


class waze_data_analyzer:

    def __init__(self, files_dir, extraction_date):

        #Paths and dates.
        self.files_dir = files_dir
        self.extraction_date = extraction_date
        
        # class variable shared by all instances. They'll be modified when calling functions of static nature.
        self.df_tt = pd.DataFrame() 
        self.df_r = pd.DataFrame()
        self.df_dict = pd.DataFrame()

    def run_basic_data_pipeline(self, project, freq, agg_type, iqr_distance, github_user, github_token):
        #Building the necessary paths...
        tt_dir = self.files_dir / ('travel_times_' + self.extraction_date + '.csv')
        r_dir = self.files_dir / ('routes_' + self.extraction_date + '.csv')

        #Reading travel_times, routes and dictionary
        self.df_tt = retrieve_data.read_tt_data(tt_dir)
        self.df_r = retrieve_data.read_r_data(r_dir)
        self.df_dict = retrieve_data.read_dict(github_user, github_token)
        
        #Filtering by project...
        self.df_tt = retrieve_data.filter_by_project(self.df_tt, self.df_dict, project)
        
        #Dropping duplicates...
        self.df_tt = retrieve_data.drop_duplicates(self.df_tt)
        
        #Parsing dates...
        self.df_tt = retrieve_data.parse_and_process_dates(self.df_tt, freq)
        
        #Deleting non-flow periods...
        self.df_tt = retrieve_data.delete_no_flow_periods(self.df_tt, self.df_dict)
        
        #Computing delay in s/km and velocity in km/h...
        self.df_tt = retrieve_data.compute_delay_velocity(self.df_tt, self.df_r)
        
        #Flagging outliers with iqr and mad-z...
        self.df_tt = retrieve_data.flag_with_iqr(self.df_tt, iqr_distance, agg_type)
        self.df_tt = retrieve_data.flag_with_mad_z(self.df_tt, agg_type)

    def run_advanced_data_pipeline(self):
        #TODO...
        pass