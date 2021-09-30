"""Update JDC metadata based on YAML files in metadata subdirectory"""

from gen3.auth import Gen3Auth
from gen3.submission import Gen3Submission
from gen3.index import Gen3Index
import yaml
import json
import pandas as pd
import numpy as np
import argparse


parser = argparse.ArgumentParser(
    description='Arguments to run the update functions'
)
parser.add_argument(
    '--endpoint', 
    metavar='endpoint', 
    type=str, 
    nargs=1,
    default='https://jcoin.datacommons.io',
    help='Specify the endpoint (ie the url of the data commons)'
)

parser.add_argument(
    '--program', 
    metavar='program', 
    type=str, 
    nargs=1,
    default='JCOIN',
    help='The program of the data commons'
)

parser.add_argument(
    '--project', 
    metavar='project', 
    type=str, 
    nargs=1,
    default='OEPS',
    help='The project of the data commons'
)
parser.add_argument(
    '--credentials', 
    metavar='credentials', 
    type=str, 
    nargs=1,
    default='credentials.json',
    help='The project of the data commons'
)

ENDPOINT = parser.parse_args().endpoint
PROGRAM = parser.parse_args().program
PROJECT = parser.parse_args().project

auth = Gen3Auth(refresh_file=parser.parse_args().credentials)
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
    
    # Note: limit=none (default) doesn't seem to work
    df = pd.DataFrame(index.get_all_records(limit=1024))
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
    
    records = json.loads(json.dumps(rf_list))
    sub.submit_record(PROGRAM, PROJECT, records)


if __name__ == "__main__":
    update_core_metadata(sub)
    update_reference_file(index, sub)
