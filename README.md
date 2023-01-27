# Importing-Service
This is a Python script that includes functions to connect to a SQL server, fetch data from it, and convert a Pandas DataFrame to a SQL table. It also includes a class named "Import" that has methods to import data from a SharePoint folder or file, merge data from different sources, and resample the merged data. The class also uses logging to log various events and exceptions that occur during the execution of the script.

The Docstring is:

Import()

- imports data from sharepoint and/or database.
- merges them.
- resamples the merged data.

Methods:
=======

from_db(`db_configs`, `table_names`, `merge_on`):
------------------------------------------------
            from_db: imports data from db.

            -- param `db_configs`: dict with database credentials.
            -- param `table_names`: list of names of tables to import.
            -- param `merge_on`: list with column names to merge on.

folder_from_sharepoint(`sharepoint_user`, `sharepoint_password`, `teams_folder_name`, `folder_location`, `col_name`)
--------------------------------------------------------------------------------------------------------------------
            folder_from_sharepoint: imports all files from a folder on sharepoint.

            -- param `sharepoint_user`: username for sharepoint with permissions to data.
            -- param `sharepoint_password`: password of sharepoint user.
            -- param `teams_folder_name`: Project name for the team.
            -- param `folder_location`: folder path given in folder details in sharepoint.
            -- param `col_name`: Name of common column to merge on.

file_from_sharepoint(`sharepoint_user`, `sharepoint_password`, `teams_folder_name`, `file_location`, `col_name`)
----------------------------------------------------------------------------------------------------------------
            file_from_sharepoint: uploads the backup data to the same or a new database.

            -- param `sharepoint_user`: username for sharepoint with permissions to data.
            -- param `sharepoint_password`: password of sharepoint user.
            -- param `teams_folder_name`: Project name for the team.
            -- param `folder_location`: folder path given in folder details in sharepoint.
            -- param `col_name`: Name of column to merge on.

merge(`resampling_freq`, `default_primary_column`, `**kwargs`)
--------------------------------------------------------------
            merge: merges all the data

            -- param `resampling_freq`: frequency to resample data on.(eg: '17min' or '30S')
            -- param `default_primary_column`: Name of Primary column in the final dataframe.
            -- param `**kwargs`: any key word arguments for asfreq function.

  - possible values of kwargs:
                    -- method = 'ffill'

                    -- method = 'bfill'

