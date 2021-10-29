'''
upload any new local files
and save these local files with the gen3 object ids in a csv file
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
        self.credentials_path = Path(credentials_path)
        self.endpoint = endpoint

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
        auth = Gen3Auth(refresh_file=self.credentials_path.as_posix())
        index = Gen3Index(self.endpoint, auth)
        #read in files in directory
        local_files_df = get_local_files(self.local_dir.as_posix())
        #get gen3 files
        gen3_files_df = get_gen3_files(index)

        files_df = (
            local_files_df
            .merge(gen3_files_df,
                on=['file_name','md5sum','file_size'],
                how='left',
                validate='one_to_one')
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

config = yaml.safe_load(open('config.yaml','r'))

files = Files(**config['file_params'])
files.upload_new_files(**config['file_upload_params'])
#get local files with object ids (should all have object ids if uploads were successful)
files_df = files.merge_local_and_gen3_file_info()
files_df.to_csv(config['csv_file_save_path'])


# auth = Gen3Auth(refresh_file='credentials.json')
# sub = Gen3Submission(auth)


# sub.delete_nodes("JCOIN", "TEST", ['reference_file','core_metadata_collection'])


# sub.delete_nodes("JCOIN", "OEPS", ['reference_file','core_metadata_collection'])




