import pandas as pd
import numpy as np
import requests
from pipeline_utils import retrieve_data


class waze_data_analyzer:

    def __init__(self, files_dir, files_date, github_user = None, github_token = None):

        #Paths and dates.
        self.files_dir = files_dir
        self.files_date = files_date
        
        # class variable shared by all instances. They'll be modified when calling functions of static nature.
        self.df_tt = pd.DataFrame() 
        self.df_r = pd.DataFrame()
        self.df_dict = pd.DataFrame()

        #Building the necessary paths...
        tt_dir = self.files_dir / ('travel_times_' + self.files_date + '.csv')
        r_dir = self.files_dir / ('routes_' + self.files_date + '.csv')

        #Reading travel_times, routes and dictionary
        self.df_tt = retrieve_data.read_tt_data(tt_dir)
        self.df_r = retrieve_data.read_r_data(r_dir)
        if github_user != None and github_token != None:
            self.df_dict = retrieve_data.read_dict(github_user, github_token)

    def run_basic_data_pipeline(self, project, freq = '15min', agg_type = 'daytype', iqr_distance = 1.5, ):

        #Filtering by project...
        self.df_tt = retrieve_data.filter_by_project(self.df_tt, self.df_dict, project)
        self.df_r = retrieve_data.filter_by_project(self.df_r, self.df_dict, project)
        
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
        self.df_tt.loc[:,'outlier'] = np.where((self.df_tt['outlier_iqr']==1)|(self.df_tt['outlier_z_score']==1), 1, 0)
        
        #Cleaning df_tt from not-used columns
        self.df_tt.drop(['no_flow_periods','no_flow_boolean'], axis=1, inplace=True)

    def get_sections_order(self):
        self.df_tt['order'] = self.df_tt['name'].apply(lambda x: x.split('_')[4])

    def get_same_section_previous_time(self):
        #This should be checked by somebody else...
        self.df_tt.sort_values(by=['name', 'updatetime'], ascending=[True, True], inplace = True)
        self.df_tt['same_section'] = (self.df_tt['name']==self.df_tt['name'].shift())
        self.df_tt['not_hole'] = (self.df_tt['updatetime'] - self.df_tt['updatetime'].shift() <= pd.Timedelta('10 minutes'))
        self.df_tt.loc[(self.df_tt['same_section']==True)&(self.df_tt['not_hole']==True), '[s/km]_i,t-1'] = self.df_tt['[s/km]'].shift()

    def get_time_delta(self):
        self.df_tt.loc[(self.df_tt['same_section']==True)&(self.df_tt['not_hole']==True), 'updatetime_i,t-1'] = self.df_tt['updatetime'].shift()
        self.df_tt.loc[:, 'delta_t'] = self.df_tt['updatetime'] - self.df_tt['updatetime_i,t-1']
        self.df_tt['delta_t'] = self.df_tt['delta_t'].apply(lambda x : x.total_seconds())

    def get_previous_section_previous_time(self):
        #This should be checked by somebody else...
        self.df_tt.sort_values(by=['updatetime','main_street','sense','order'], ascending=[True, True, True, True], inplace = True)
        self.df_tt['same_section'] = ((self.df_tt['main_street']==self.df_tt['main_street'].shift())&(self.df_tt['sense']==self.df_tt['sense'].shift()))
        self.df_tt.loc[(self.df_tt['same_section']==True)&(self.df_tt['order']!=1), '[s/km]_i-1,t-1'] = self.df_tt['[s/km]_i,t-1'].shift()

    def get_next_section_previous_time(self):
        #This should be checked by somebody else...
        self.df_tt['same_section'] = ((self.df_tt['main_street']==self.df_tt['main_street'].shift(-1))&(self.df_tt['sense']==self.df_tt['sense'].shift(-1)))
        self.df_tt['same_updatetime'] = ((self.df_tt['main_street']==self.df_tt['main_street'].shift(-1))&(self.df_tt['updatetime']==self.df_tt['updatetime'].shift(-1)))
        self.df_tt.loc[(self.df_tt['same_section']==True)&(self.df_tt['same_updatetime']==True), '[s/km]_i+1,t-1'] = self.df_tt['[s/km]_i,t-1'].shift(-1)

    def merge_traffic_info(self):
        self.df_tt = self.df_tt.merge(self.df_dict[['name','traffic_lights','priority','pedestrian_crossing','NI']], on = 'name', how = 'left')

    def make_feature_explosion(self):
        """
        One hot encoding of name, weekday and floor_hour variables 
        """
        #Getting dummies only for name, weekday and floor_hour
        self.df_tt.sort_values(by=['name', 'updatetime'], ascending=[True, True], inplace = True) #just in case...
        self.df_tt = pd.get_dummies(self.df_tt, columns = ['name','weekday','floor_hour'])

    def make_network_features(self):
        """
        Calls create_network_features_matrices and merges the results
        into the travel times data frame
        """
        matrices = retrieve_data.create_network_features_matrices(self.df_r) # horizontal, vertical, angle
        for i in range(0, len(matrices)):
            self.df_tt = self.df_tt.merge(matrices[i], on = 'name', how = 'left', right_index = True)
        # print('columns on df_tt: ', list(self.df_tt.columns))
