''' 
reads in the files.csv file created (see config file for path)
and joins variables on each spatial level.

results in 8 files: 
- 4 spatial levels (zip, tract, county, state)
- 2 column name types (one with only variable names and the other with file prefix)

for files with issues, see the data/files_issue.txt file

also creates a metadata submission for the joined files
''' 
import yaml
from oeps_utils.file_utils import (
    get_prefix,get_spatial_join,read_csv_file,
    Files
)
import pandas as pd

#read in files and get prefix, spatial join key (ie primary index) from file name 
with open('config.yaml','r') as f:
    config = yaml.safe_load(f)
files = (
    pd.read_csv(config['csv_file_save_path'])
    .pipe(lambda x: x.loc[x.file_name.str.contains('csv$')]) #get spatial data
    .assign(
        file_prefix = lambda x: get_prefix(x),
        file_join = lambda x: get_spatial_join(x)
    )
    .pipe(lambda x: x.loc[~x.file_join.isna()]) #remove files without spatial scale id (ie locations)
)

#read in the files
files['df_with_file_prefix'] = files.apply(read_csv_file,axis=1) #read in the individual files
files['df_without_file_prefix'] = files.apply(read_csv_file,with_file_prefix=False,axis=1) #read in the individual files


geo_file_names = {
    'COUNTYFP':'data/oeps_combined_county',
    'GEOID':'data/oeps_combined_tract',
    'STATEFP':'data/oeps_combined_state',
    'ZCTA':'data/oeps_combined_zip'
}

for i,group in files.groupby('file_join'):
    print(f"concatenating all {i}")
    df_no_prefix = pd.concat(group['df_without_file_prefix'].values,axis=1)
    df_no_prefix.to_csv(f"{geo_file_names[i]}.csv")

    df_with_prefix = pd.concat(group['df_with_file_prefix'].values,axis=1)
    df_with_prefix.to_csv(f"{geo_file_names[i]}_with_file_prefix.csv")

#get file information
config = yaml.safe_load(open('config.yaml','r'))
joined_files = Files(**config['joined_file_params'])
#files.upload_all_files_with_subdirs(**config['file_upload_params'])
joined_files.upload_new_files(**config['file_upload_params'])
#update gen3 file df with newly uploaded files
joined_files.get_gen3_files()
#get local files with object ids (should all have object ids if uploads were successful)
joined_files_df = joined_files.merge_local_and_gen3_file_info()

#create metadata submissions for files
description = '''The OEPS data from individual files 
(see individual OEPS file metadata for more information) 
was combined by the JCOIN DASC group for easier lookup and use of individual variables
across files. Files were joined based on primary geographic key.
See OEPS data documentation for more information.
'''
source = '''Marynia Kolak, Qinyun Lin, Susan Paykin, Moksha Menghaney, & Angela Li. (2021, May 11). 
GeoDaCenter/opioid-policy-scan: Opioid Environment Policy Scan Data Warehouse (Version v0.1-beta). 
Zenodo. http://doi.org/10.5281/zenodo.4747876
'''
spatial_scales = ['County','Tract','State','Zip']
#make core metadata collection
core_metadata_collection = pd.DataFrame(
    {
        'submitter_id':[f'oeps_combined_{s.lower()}' for s in spatial_scales],
        'title':['Combined OEPS data']*len(spatial_scales),
        'description':[description]*len(spatial_scales),
        'source':[source]*len(spatial_scales),
        'subject':['Combined']*len(spatial_scales),
        'relation':['see OEPS metadata']*len(spatial_scales),
        'data_type':spatial_scales,
        'type':['core_metadata_collection']*len(spatial_scales),
        'projects.code':['OEPS']*len(spatial_scales),
        'creator':['JCOIN DASC']*len(spatial_scales)
    }
)
#reference file submsision
reference_file_df = (
    joined_files_df
    .rename(columns={'gen3_object_id':'object_id'})
    .assign(
        submitter_id=lambda x:x.file_name.str.replace('.csv','',regex=True),
        data_category=lambda x:x.file_name.str.extract('(state|tract|zip|county)')[0].str.title(),
        data_type='Spatial Data',
        type='reference_file',
        data_format='CSV'
    )
    .pipe(lambda x: x.loc[~x.data_category.isna()])
)

reference_file_df['core_metadata_collections.submitter_id'] = (
    reference_file_df['submitter_id']
    .str.replace("_with_file_prefix","",regex=True)
)

reference_fields = [
    'submitter_id',
    'file_name',
    'md5sum',
    'file_size',
    'object_id',
    'data_type',
    'data_format',
    'data_category',
    'core_metadata_collections.submitter_id',
    'type'

]

core_metadata_collection.to_csv('metadata/core_metadata_collections_combined.tsv',sep='\t',index=False)
reference_file_df[reference_fields].to_csv('metadata/reference_file_combined.tsv',sep='\t',index=False)