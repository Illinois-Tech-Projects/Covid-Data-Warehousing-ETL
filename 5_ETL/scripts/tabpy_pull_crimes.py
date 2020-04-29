# ===========================================
# Author: Alex Wang
# ITMD 526 || Data Warehousing - Final Project
# 04/24/2020
# ===========================================

# -----------------
# Import libraries
# ------------------

from os import path     # for getting system paths
import os.path          # for getting system paths
import re               # regular expression | cleaning column names
import zipfile          # for unzipping files

import pandas as pd     # DataFrame manipulation
import wget             # for downloading files from url

pd.set_option( 'display.width', 400 )  # set max width to display in the console
pd.set_option( 'display.max_columns', 40 )  # set max column to display in the console


# -----------------
# Functions
# ------------------

# important:  initiate empty panda DataFrame to replace df passed from tableau
def df_init():
    # important:  initiate empty DataFrame
    column_names=['date_of_occurrence', 'category', 'city']
    df=pd.DataFrame( columns=column_names )
    return df


# get download dir path for windows/mac where csv files will be downloaded
def get_download_path():
    # https://stackoverflow.com/questions/23070299/get-the-windows-download-folders-path
    """Returns the default downloads path for linux or windows"""
    if os.name == 'nt':
        import winreg
        sub_key=r'SOFTWARE\Microsoft\Windows\CurrentVersion\Explorer\Shell Folders'
        downloads_guid='{374DE290-123F-4565-9164-39C4925E467B}'
        with winreg.OpenKey( winreg.HKEY_CURRENT_USER, sub_key ) as key:
            location=winreg.QueryValueEx( key, downloads_guid )[0]
        return location
    else:
        return os.path.join( os.path.expanduser( '~' ), 'downloads' )


# clean the column names
def rename_colnames( df ):
    df.rename( columns=lambda x:re.sub( "\s\s+", " ", x ), inplace=True )  # removing extra spaces in the col names
    df.rename( columns=lambda x:x.lower().strip().replace( ' ', '_' ), inplace=True )  # lower case and stringify col name


# each data set has different col names for the data we want to return
def map_col_to_rtn( df, col ) -> object:
    """

    :type df: panda DataFrame
    """
    # important:  initiate empty DataFrame
    return_col=['date_of_occurrence', 'category', 'city']

    df[return_col[0]]=df[col[0]]
    df[return_col[1]]=df[col[1]]
    df[return_col[2]]=col[2]

    if len( col ) > 3:
        return_col.append( 'category_2' )
        df[return_col[3]]=df[col[3]]

    return df[return_col]


# download file if not exist in the download dir and read it into data frame
def read_df( filename, url, download_dir=get_download_path(), extract_file=None ):
    filePath=path.join( download_dir, filename )

    if path.exists( filePath ):
        print( 'reading ', filePath )
        return pd.read_csv( filePath )
    else:
        print( filePath, ' does not exist, downloading...' )
        wget.download( url, filePath )
        print( 'Download Finished' )
        return pd.read_csv( filePath )


# -----------------
# Main
# ------------------

# https://www.atlantapd.org/Home/ShowDocument?id=3209
def pull_atlanta_crime( df ):
    df=df_init()

    # source file
    #  download zip
    zipf=path.join( get_download_path(), 'atlanta_crime.zip' )
    wget.download( 'https://www.atlantapd.org/Home/ShowDocument?id=3209', zipf )

    zf=zipfile.ZipFile( str( zipf ) )
    zf.extract( 'COBRA-2020.csv', path=get_download_path() )
    filePath=path.join( get_download_path(), 'atlanta_crime.csv' )
    os.rename( path.join( get_download_path(), 'COBRA-2020.csv' ), filePath )

    atlanta=pd.read_csv( filePath )
    rename_colnames( atlanta )

    colnames=['occur_date', 'ucr_literal', 'atlanta', 'shift_occurrence']

    df=map_col_to_rtn( atlanta, colnames )

    return df


# https://www.denvergov.org/media/gis/DataCatalog/crime/csv/crime.csv
def pull_denver_crime( df ):
    df=df_init()

    # source file
    denver=read_df( 'denver_crime.csv', 'https://www.denvergov.org/media/gis/DataCatalog/crime/csv/crime.csv' )
    rename_colnames( denver )

    colnames=['first_occurrence_date', 'offense_category_id', 'denver', 'offense_type_id']

    df=map_col_to_rtn( denver, colnames )

    return df


# https://data.montgomerycountymd.gov/api/views/icn6-v9z3/rows.csv
def pull_maryland_crime( df ):
    df=df_init()

    # source file
    maryland=read_df( 'maryland_crime.csv', 'https://data.montgomerycountymd.gov/api/views/icn6-v9z3/rows.csv' )
    rename_colnames( maryland )

    colnames=['start_date_time', 'crime_name2', 'maryland', 'crime_name3']

    df=map_col_to_rtn( maryland, colnames )

    return df


# https://www.phoenixopendata.com/dataset/cc08aace-9ca9-467f-b6c1-f0879ab1a358/resource/0ce3411a-2fc6-4302-a33f-167f68608a20/download/crimestat.csv
def pull_phoenix_crime( df ):
    df=df_init()

    # source file
    phoenix=read_df( 'phoenix_crime.csv', 'https://www.phoenixopendata.com/dataset/cc08aace-9ca9-467f-b6c1-f0879ab1a358/resource/0ce3411a-2fc6-4302-a33f-167f68608a20/download/crimestat.csv' )
    rename_colnames( phoenix )

    # convert occurrence col to datetime type for filtering
    # convert occurrence col to datetime type for filtering
    # c['date_of_occurrence']=pd.to_datetime( c['date_of_occurrence'], infer_datetime_format=True , errors='ignore' )
    # start_date="2020-01-01"
    # c=c[c['date_of_occurrence'] > start_date]

    colnames=['occurred_on', 'ucr_crime_category', 'phoenix']
    df=map_col_to_rtn( phoenix, colnames )

    return df


# https://data.lacity.org/api/views/2nrs-mtv8/rows.csv
def pull_la_crime( df ):
    df=df_init()

    # source file
    la=read_df( 'la_crime.csv', 'https://data.lacity.org/api/views/2nrs-mtv8/rows.csv' )
    rename_colnames( la )

    # convert occurrence col to datetime type for filtering
    # c['date_of_occurrence']=pd.to_datetime( c['date_of_occurrence'], infer_datetime_format=True , errors='ignore' )
    # start_date="2020-01-01"
    # c=c[c['date_of_occurrence'] > start_date]

    colnames=['date_occ', 'crm_cd_desc', 'la']
    df=map_col_to_rtn( la, colnames )

    return df


# https://data.austintexas.gov/api/views/fdj4-gpfu/rows.csv
def pull_austin_crime( df ):
    df=df_init()
    austin=read_df( 'austin_crime.csv', 'https://data.austintexas.gov/api/views/fdj4-gpfu/rows.csv' )
    rename_colnames( austin )
    colnames=['occurred_date', 'highest_offense_description', 'austin']

    df=map_col_to_rtn( austin, colnames )
    # convert occurrence col to datetime type for filtering
    df['date_of_occurrence']=pd.to_datetime( df['occurred_date_time'] )
    start_date="2020-01-01"

    return df[df['occurred_date_time'] > start_date]


# https://data.cityofchicago.org/api/views/x2n5-8w5q/rows.csv
def pull_chicago_crime( df ):
    df=df_init()

    # source file
    chicago=read_df( 'chicago_crime.csv', 'https://data.cityofchicago.org/api/views/x2n5-8w5q/rows.csv' )
    rename_colnames( chicago )

    # convert occurrence col to datetime type for filtering
    # c['date_of_occurrence']=pd.to_datetime( c['date_of_occurrence'], infer_datetime_format=True , errors='ignore' )
    # start_date="2020-01-01"
    # c=c[c['date_of_occurrence'] > start_date]

    colnames=['date_of_occurrence', 'primary_description', 'chicago']
    df=map_col_to_rtn( chicago, colnames )

    return df
