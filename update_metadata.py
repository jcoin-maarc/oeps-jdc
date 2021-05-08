"""Update JDC metadata based on YAML files in metadata subdirectory"""

from gen3.auth import Gen3Auth
from gen3.submission import Gen3Submission
from gen3.index import Gen3Index
import yaml
import json
import pandas as pd
import numpy as np

ENDPOINT = 'https://jcoin.datacommons.io'
PROGRAM = 'JCOIN'
PROJECT = 'OEPS'

auth = Gen3Auth(refresh_file='credentials.json')
index = Gen3Index(ENDPOINT, auth)
sub = Gen3Submission(ENDPOINT, auth)

def update_core_metadata(sub):
    """Update Core Metadata Collection records"""
    
    template = {'type':'core_metadata_collection',
                'projects': [{'code': PROJECT}]}
    with open('metadata/core_metadata_collection.yml', 'r') as stream:
        cmdc_list = [dict(cmdc, **template) for cmdc in yaml.safe_load(stream)]
    
    # Add creator property
    creator = 'Center for Spatial Data Science (CSDS) at the University of Chicago'
    cmdc_list = [dict(cmdc, creator=creator) for cmdc in cmdc_list]
    
    records = json.loads(json.dumps(cmdc_list))
    sub.submit_record(PROGRAM, PROJECT, records)

def add_submitter_id(df, file_name='file_name', did='did'):
    """Add submitter ID to data frame"""
    
    prefix = PROGRAM + '-' + PROJECT + '_'
    df['submitter_id'] = (prefix + 
                          df[file_name].str.rsplit('.', 1).str[0] +
                          '_' + df[did].str[-4:])

def get_files(index):
    """Get file objects uploaded to Commons"""
    
    df = pd.DataFrame(index.get_all_records())
    add_submitter_id(df)
    df['updated_date'] = pd.to_datetime(df.updated_date)
    df['md5sum'] = df.hashes.map(lambda x: x.get('md5'))
    df['file_size'] = df['size'].fillna(0).astype(np.int64)
    df.rename(columns={'did':'object_id'}, inplace=True)
    
    df = df.sort_values('updated_date').groupby('file_name').tail(1)
    df = df[['object_id','file_name','submitter_id','file_size','md5sum']]
    files = df.set_index('file_name').T.to_dict('dict')
    return files

def update_reference_file(index, sub):
    """Update Reference File records"""
    
    files = get_files(index)
    template = {'type':'reference_file'}
    with open('metadata/reference_file.yml', 'r') as stream:
        rf_list = [dict(rf, **template, **files.get(rf['file_name'],{}))
                   for rf in yaml.safe_load(stream)]
    
    # Prune any files that haven't been uploaded yet
    rf_list = [f for f in rf_list if 'submitter_id' in f]
    
    # TODO Remove once pending data dictionary change has been made
    rf_list = [dict(f, data_category='Other', data_type='Other',
                    data_format='TXT') for f in rf_list]
    
    records = json.loads(json.dumps(rf_list))
    sub.submit_record(PROGRAM, PROJECT, records)


if __name__ == "__main__":
    update_core_metadata(sub)
    update_reference_file(index, sub)
