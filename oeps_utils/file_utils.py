''' 
utilities for working with local files,
comparing with gen3 file objects,
and OEPS specific file transformation functions
'''
#conda create -n oeps-wrangler
#TODO: Add a regex flag to all replace functions
#FutureWarning: The default value of regex will change from True to False in a future version.
import pandas as pd
import openpyxl
import os
import re
from io import StringIO
import hashlib

from gen3.auth import Gen3Auth
from gen3.submission import Gen3Submission
from gen3.index import Gen3Index
import numpy as np

import shutil
from pathlib import Path
import yaml

# general gen3 functions


def get_md5sum(file_path):
    with open(file_path, "rb") as f:
        md5_hash = hashlib.md5()
        content = f.read()
        md5_hash.update(content)
    return md5_hash.hexdigest()

def get_filesize(file_path):
    return os.path.getsize(file_path)

def get_local_files(dir):
    ''' 
    collects file properties in a local directory and its subdirectories
    in a dataframe.

    created to check local files (collected here) against files
    uploaded to a gen3 commons
    
    ''' 
    #instantiate empty df to append file info
    file_props = {
        'file_path':[],
        'file_name':[],
        'md5sum':[],
        'file_size':[]
    }

    def _scan_dir(dir):
        '''
        recursively scan directory and collects all 
        files in directory and sub-directories
        '''
        for f in os.scandir(dir):
            if f.is_file():
                #append overall lists
                file_props['file_path'].append(f.path)
                file_props['file_name'].append(f.name)
                file_props['md5sum'].append(get_md5sum(f.path))
                file_props['file_size'].append(get_filesize(f.path))
            if f.is_dir():
                #run through function if not a directory that has its own files
                _scan_dir(f.path)
    #run scan function          
    _scan_dir(dir)
    return pd.DataFrame(file_props)

def get_gen3_files(index):
    """Get file objects uploaded to Commons"""
    # Note: limit=none (default) doesn't seem to work
    # TODO: accomodate for more than 1024 files (returns in pages -- augment call with pages)
    #asyncio package --- concurrent programming -- eg engineX uses
    df = pd.DataFrame(index.get_all_records(limit=1024))
    #add_submitter_id(df)
    df['updated_date'] = pd.to_datetime(df.updated_date)
    df['md5sum'] = df.hashes.map(lambda x: x.get('md5'))
    df['file_size'] = df['size'].fillna(0).astype(np.int64)
    df.rename(columns={'did':'object_id'}, inplace=True)
    df = df.sort_values('updated_date').groupby(['file_name','md5sum']).tail(1)
    df = df[['object_id','file_name','file_size','md5sum']]
    return df

class Files:

    def __init__(self,local_dir,credentials_path,endpoint):
        self.local_dir = Path(local_dir)
        if credentials_path and endpoint:
            self.credentials_path = Path(credentials_path)
            self.endpoint = endpoint
            self.auth = Gen3Auth(refresh_file=self.credentials_path.as_posix())
            self.index = Gen3Index(self.endpoint, self.auth)

            self.get_local_files()
            self.get_gen3_files()

    def get_local_files(self):
        #read in files in directory
        self.local_files_df = get_local_files(self.local_dir.as_posix())

    def get_gen3_files(self):
        #get gen3 files
        self.gen3_files_df = get_gen3_files(self.index)

    def merge_local_and_gen3_file_info(self):
        ''' 
        reads in all files of a local directory that contains files to be uploaded.
        Then gets all the complete file manifest from a gen3 commons.

        If the local file matches the gen3 file (based on file name, file size, and md5 check sum),
        gets the gen3 object id. 

        This can be used for two use cases:
        1. File uploads: to check for new files that havent been uploaded (ie that dont have an object id) -- 
            useful as currently only looks for file name in local submission history.
        2. Metadata submissions: to provide the file information necessary for metadata submission
        ''' 
        files_df = (
            self.local_files_df
            .merge(self.gen3_files_df,
                on=['file_name','md5sum','file_size'],
                how='left')#,
                #validate='one_to_one')
            .rename(columns={"object_id":"gen3_object_id"})
        )
        return files_df

    def upload_new_files(self,gen3_client_exe_path,gen3_history_path):
        ''' 
        takes in the path to a gen3-client executable and the path to 
        gen3 history (ie .gen3). 
        Deletes the local gen3 history and uploads new files
        based on the md5sum and file name

        TODO: upload all at once and with subdirectory 
        (may want to copy local files to a tmp folder as not sure its possible 
        to download a user-defined list of files with gen3 client?)

        TODO: another possibile improvement/alternative would be to support parallel 
        processing uploads via this function here (see joblib.Parallel fxn)
        '''

        self.gen3_client_exe_path = Path(gen3_client_exe_path)
        self.gen3_history_path = Path(gen3_history_path)
        files_df = self.merge_local_and_gen3_file_info()
        ## check if current file is in gen3 and upload files that are not 
        files_to_be_uploaded = (
            files_df
            .loc[files_df.gen3_object_id.isna()]
            .file_path
            .apply(os.path.realpath)
        )
        print(f'''

        Number of files to be uploaded: {files_to_be_uploaded.shape[0]}
        Number of total files: {files_df.shape[0]}

        ''')
        #as we are checking successful upload history directly through gen3, the local submission history is not needed
        # TODO: make option to not remove path
        if self.gen3_history_path.is_dir():
            shutil.rmtree(self.gen3_history_path)
        #configure gen3 client for local submissions
        creds = os.popen(f'{self.gen3_client_exe_path} configure --profile=jdc --cred={self.credentials_path} --apiendpoint={self.endpoint}')
        print(creds.read())
        #upload local data files not in gen3
        upload_output_list = []
        for f in files_to_be_uploaded:    
            output = os.popen(f"{self.gen3_client_exe_path} upload --profile=jdc --upload-path={f}")
            output_text = output.read()
            print(output_text)
            upload_output_list.append(output_text)
        self.upload_output_list = upload_output_list

    # def upload_all_files_with_subdirs(self,gen3_client_exe_path,gen3_history_path):
    #     self.gen3_client_exe_path = Path(gen3_client_exe_path)
    #     self.gen3_history_path = Path(gen3_history_path)
    #     output = os.popen(f'{self.gen3_client_exe_path} upload '\
    #         f'--profile=jdc '\
    #         f'--upload-path={self.local_dir}'\
    #         f'--include-subdirname=true'
    #     )
    #     return output.read()




#OEPS specific functions

def get_prefix(x):
    ''' 
    get prefix of file name without the spatial scale identifier
    or the file extension
    ''' 
    return x.file_name.str.replace("_[A-Z]\.csv","",regex=True)

def get_spatial_join(x):
    ''' 
    get spatial id name for joining data frames
    '''
    spatial_scale_map = {
        "Z":"ZCTA",
        "T":"GEOID",
        "S":"STATEFP",
        "C":"COUNTYFP"
    }
    return (
        x.file_name
        .str.extract("(_[A-Z]\.csv)",expand=False)
        .str.replace("_|\.csv","",regex=True)
        .replace(spatial_scale_map)
    )

def read_csv_file(file_info_series,with_file_prefix=True):
    ''' 
    get a dataframe based on a series
    with a file_path, file_join, and file_prefix 

    For the geo, columns pad with leading zeros
    based on expected length. Do not include files 
    with duplicate primary geo ids.
    ''' 
    s = file_info_series
    geo_dtypes = {
            'COUNTYFP':'str',
            'STATEFP':'str',
            'ZCTA':'str',
            'GEOID':'str',
            'TRACTCE':'str'
        }
    geo_lengths = {
            'COUNTYFP':5,
            'STATEFP':2,
            'ZCTA':5,
            'GEOID':11,
            'TRACTCE':6  
    }
    df = pd.read_csv(s.loc['file_path'],dtype=geo_dtypes)

    #missing geo ids
    is_invalid_geo_na = df[s.loc['file_join']].isna()
    num_invalid_geo_na = is_invalid_geo_na.sum()

    #not the correct geo ids given the expected length -- if no dups, this could be due to 
    #no leading 0s -- which was confirmed by SPatial Science Group
    is_invalid_geo_length = df[s.loc['file_join']].str.len()!=geo_lengths[s.loc['file_join']]
    num_invalid_geo_length = is_invalid_geo_length.sum()

    num_invalid_geo_dups = (df[s.loc['file_join']].value_counts()>1).sum()

    #if duplicates exclude so return None and record in file
    #other checks do not matter
    #aligned exclusion with Spatial Center on 11/2
    if num_invalid_geo_dups>1:
        with open('file_issues.txt','a') as f:
            f.write(f'''

            {s.loc['file_path']} warning -- file excluded due to duplicates
            Number of duplicate {s.loc['file_join']}: {str(num_invalid_geo_dups)}

            ''')

            
            return None
    
    #if nas or length issues -- correct/ filter out but do not exclude
    #but still flag in file
    elif num_invalid_geo_na>0 or num_invalid_geo_length>0:
        with open('file_issues.txt','a') as f:
            f.write(f'''

            {s.loc['file_path']} warning -- no
            duplicates so included but dropped missing geo ids and/or padded 0s

            Number of invalid length {s.loc['file_join']}: {str(num_invalid_geo_length)}
            (expected a length of {geo_lengths[s.file_join]})

            Number of missing {s.loc['file_join']}: {str(num_invalid_geo_na)}

            ''')

    #for all geo ids in the dataframe, pad with zeros if length less than expected
    #if length greater than expected, unclear what it is supposed to be so make None
    #some dataframes have foreign geo ids in addition to join (ie primary index geo id)
    for geo_name,geo_length in geo_lengths.items():
        if geo_name in df:
            is_not_expected_len = df[geo_name].str.len()<geo_length
            padded_with_zeros = df[geo_name].str.zfill(geo_length)
            df[geo_name].where(is_not_expected_len,padded_with_zeros,inplace=True)

    df_cleaned = (df
        .loc[~df[s.loc['file_join']].isna()]
        .set_index(s.loc['file_join'])
    )
    if with_file_prefix:
        df_cleaned.columns = [s.loc['file_prefix']+"_"+ col for col in df_cleaned.columns]
    return df_cleaned