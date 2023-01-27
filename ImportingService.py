'''
Developed by: Pranjal Ghildiyal.
Date: 1/19/2023
'''

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
import io
import logging as lg

lg.basicConfig(filename = "LogFile.log" , level = lg.INFO , format ='%(asctime)s %(levelname)s %(message)s')

# function to connect with sql server and database we have created
def connect_with_sql(sql_username, sql_password, sql_ip, sql_port, sql_database):
    lg.info("Start for making sql_conn with %s, %s, %s, %s, %s" , sql_username, sql_password, sql_ip, sql_port, sql_database)
    try:
        if sql_port == "0" or sql_port == 0:
            sql_port = "3306"
        connect_query = "mysql+pymysql://"+sql_username+":"+sql_password+"@"+sql_ip+":"+sql_port+"/"+sql_database
        engine = create_engine(connect_query)
        lg.info("Execution success with engine: %s", engine)
        return (True, engine)
    except Exception as e:
        lg.error("Execution failure")
        lg.exception("Exception: " + str(e))
        return (False, "Failed to connect")
# function to fetch information from the database
def fetch_details(query,engine):
    lg.info("Start fetch_details with %s, %s" , query,engine)
    try:
        data = pd.read_sql(query,engine)
        lg.info("Execution success: Required data imported from db")
        return (True, data)
    except Exception as e:
        lg.error("Execution failure")
        lg.exception("Exception: " + str(e))
        return (False, "No info fetched")

def df_to_sql(data, name, engine, index = False, how = 'append'):
    try: 
        data.to_sql(name = name, con = engine, if_exists= how, index = index)
        lg.info('{} sent to sql successfully!\n'.format(name))
        return (True, engine)
    except Exception as e:
        lg.warning('{} export unsuccessful! Error message:{}'.format(name, e))

class Import():
    '''
    Import()
    =========
    - imports data from sharepoint and/or database.
    - merges them.
    - resamples the merged data.

    Methods:
    --------

    - from_db(`db_configs`, `table_names`, `merge_on`):\n
        \tfrom_db: imports data from db.\n
        \t-- param `db_configs`: dict with database credentials.
        \t-- param `table_names`: list of names of tables to import.
        \t-- param ``merge_on`: list with column names to merge on.


    - folder_from_sharepoint(`sharepoint_user`, `sharepoint_password`, `teams_folder_name`, `folder_location`, `col_name`)\n
        \tfolder_from_sharepoint: imports all files from a folder on sharepoint.\n
        
        \t-- param `sharepoint_user`: username for sharepoint with permissions to data.
        \t-- param `sharepoint_password`: password of sharepoint user.
        \t-- param `teams_folder_name`: Project name for the team.
        \t-- param `folder_location`: folder path given in folder details in sharepoint.
        \t-- param `col_name`: Name of common column to merge on.
    
    - file_from_sharepoint(`sharepoint_user`, `sharepoint_password`, `teams_folder_name`, `file_location`, `col_name`)\n
        \tfile_from_sharepoint: uploads the backup data to the same or a new database.\n

        \t-- param `sharepoint_user`: username for sharepoint with permissions to data.
        \t-- param `sharepoint_password`: password of sharepoint user.
        \t-- param `teams_folder_name`: Project name for the team.
        \t-- param `folder_location`: folder path given in folder details in sharepoint.
        \t-- param `col_name`: Name of column to merge on.

    - merge(`resampling_freq`, `default_primary_column`, `**kwargs`)\n
        \tmerge: merges all the data\n

        \t-- param `resampling_freq`: frequency to resample data on.(eg: '17min' or '30S')
        \t-- param `default_primary_column`: Name of Primary column in the final dataframe.
        \t-- param `**kwargs`: any key word arguments for `asfreq` function.\n
        
        \t - possible values of kwargs:\n
        \t\t-- method = 'ffill'\n
        \t\t-- method = 'bfill'\n
    ----------------------------------------------------------------------------------------------
     '''
    
    def __init__(self):
        self.data = {}
        self.iterator = 0
        self.on = {}
        self.primary_column = 'Primary'
        self.auth_flag = 0
        
    def from_db(self, db_configs, table_names, merge_on):
        '''
        - from_db(`db_configs`, `table_names`, `merge_on`):\n
        from_db: imports data from db.\n
        -- param `db_configs`: dict with database credentials.
        -- param `table_names`: list of names of tables to import.
        -- param ``merge_on`: list with column names to merge on.
        '''
        
        self.sql_username = db_configs['sql_username']
        self.sql_password = db_configs['sql_password']
        self.sql_ip = db_configs['sql_ip']
        self.sql_port= db_configs['sql_port']
        self.sql_database = db_configs['sql_database']
        
        lg.info('Importing {} from {}'.format(table_names, self.sql_database))
        
        for table_name, col_name in zip(table_names, merge_on):
            engine = connect_with_sql(self.sql_username, self.sql_password, self.sql_ip, self.sql_port, self.sql_database)
            query = "select * from {}.{};".format(self.sql_database, table_name)
            status_n, data = fetch_details(query, engine[1])
            if not status_n:
                lg.warning('\t{} does not exist in the {}.'.format(table_name, self.sql_database))
                continue
            data = data.rename(columns = {col_name: self.primary_column})
            self.data[self.iterator] = data
            self.iterator += 1
            print(data)
            
            lg.info('\t{} imported successfully from {}.'.format(table_name, self.sql_database))
        
        return self
        
    def folder_from_sharepoint(self, sharepoint_user, sharepoint_password, teams_folder_name, folder_location, col_name):
        '''
        - folder_from_sharepoint(`sharepoint_user`, `sharepoint_password`, `teams_folder_name`, `folder_location`, `col_name`)\n
        folder_from_sharepoint: imports all files from a folder on sharepoint.\n
        
        -- param `sharepoint_user`: username for sharepoint with permissions to data.
        -- param `sharepoint_password`: password of sharepoint user.
        -- param `teams_folder_name`: Project name for the team.
        -- param `folder_location`: folder path given in folder details in sharepoint.
        -- param `col_name`: Name of common column to merge on.
        '''

        folder_location = folder_location.replace('%20', ' ')
        base_url = folder_location.split('sharepoint.com')[0] + 'sharepoint.com'
        sharepoint_base_url = folder_location.split(teams_folder_name)[0] + teams_folder_name + '/'
        folder_in_sharepoint = folder_location.replace(base_url, '')

        print(folder_location)
        print(base_url)
        print(sharepoint_base_url)
        print(folder_in_sharepoint)
        
        
        combined_data = pd.DataFrame()
        
        if self.auth_flag == 0:
            self.auth = AuthenticationContext(sharepoint_base_url)
            self.auth.acquire_token_for_user(sharepoint_user, sharepoint_password)
            self.ctx = ClientContext(sharepoint_base_url, self.auth)
            web = self.ctx.web
            self.ctx.load(web)
            self.ctx.execute_query()
            print('Connected to SharePoint: ',web.properties['Title'])
            self.auth_flag = 1

        folder = self.ctx.web.get_folder_by_server_relative_url(folder_in_sharepoint)
        sub_folders = folder.files   
        self.ctx.load(sub_folders)  
        self.ctx.execute_query()  
        for files in sub_folders:
            print(files)
            file_url = files.properties['ServerRelativeUrl']
            file_response = File.open_binary(self.ctx, file_url)
            df = pd.read_csv(io.StringIO(file_response.content.decode('utf-8')), header = None)
            print(df)
            df = df.rename(columns = {col_name: self.primary_column})
            self.data[self.iterator] = df
            self.iterator += 1
        
        return self
    
    def file_from_sharepoint(self, sharepoint_user, sharepoint_password, teams_folder_name, file_location, col_name):
        '''
        - file_from_sharepoint(`sharepoint_user`, `sharepoint_password`, `teams_folder_name`, `file_location`, `col_name`)\n
        file_from_sharepoint: uploads the backup data to the same or a new database.\n

        -- param `sharepoint_user`: username for sharepoint with permissions to data.
        -- param `sharepoint_password`: password of sharepoint user.
        -- param `teams_folder_name`: Project name for the team.
        -- param `folder_location`: folder path given in folder details in sharepoint.
        -- param `col_name`: Name of column to merge on.
        '''

        file_location = file_location.replace('%20', ' ')
        base_url = file_location.split('sharepoint.com')[0] + 'sharepoint.com'
        sharepoint_base_url = file_location.split(teams_folder_name)[0] + teams_folder_name + '/'
        file_in_sharepoint = file_location.replace(base_url, '')
        
        if self.auth_flag == 0:
            self.auth = AuthenticationContext(sharepoint_base_url)
            self.auth.acquire_token_for_user(sharepoint_user, sharepoint_password)
            self.ctx = ClientContext(sharepoint_base_url, self.auth)
            web = self.ctx.web
            self.ctx.load(web)
            self.ctx.execute_query()
            print('Connected to SharePoint: ',web.properties['Title'])
            self.auth_flag = 1
        

        file_response = File.open_binary(self.ctx, file_in_sharepoint)
        df = pd.read_csv(io.StringIO(file_response.content.decode('utf-8')), header = None)
        df = df.rename(columns = {col_name: self.primary_column})
        self.data[self.iterator] = df
        self.iterator += 1
        
        return self
    
    def merge(self, resampling_freq, default_primary_column, **kwargs):
        '''
        merge(`resampling_freq`, `default_primary_column`, `**kwargs`)\n
        merge: merges all the data\n

        - param `resampling_freq`: frequency to resample data on.(eg: '17min' or '30S')
        - param `default_primary_column`: Name of Primary column in the final dataframe.
        - param `**kwargs`: any key word arguments for `asfreq` function.\n
        
        -- possible values:\n
        -- -method = 'ffill'\n
        -- -method = 'bfill'\n
        '''
        if len(self.data.keys()) > 1:
            combined_data = pd.merge(self.data[0], self.data[1], how = 'outer', on = self.primary_column)
            for iterator in range(1, self.iterator, 1):
                combined_data = pd.merge(combined_data, self.data[iterator], how = 'outer', on = self.primary_column)
        else:
            combined_data = self.data[0]
        
        # Providing default primary column to combined_data
        combined_data = combined_data.rename(columns = {self.primary_column: default_primary_column})
        
        try:
            #Now resampling
            combined_data[self.primary_column] = pd.to_datetime(combined_data[self.primary_column])
            combined_data = combined_data.set_index(self.primary_column)
            combined_data = combined_data.asfreq(freq = resampling_freq, **kwargs)
        except Exception as e:
            lg.info('Resampling Failed! Error message: {}'.format(e))
        
        return combined_data.reset_index()