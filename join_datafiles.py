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

from oeps_utils.file_utils import get_local_files
#get file information
config = yaml.safe_load(open('config.yaml','r'))
files = Files(**config['joined_file_params'])
#files.upload_all_files_with_subdirs(**config['file_upload_params'])
files.upload_new_files(**config['file_upload_params'])
#get local files with object ids (should all have object ids if uploads were successful)
files_df = files.merge_local_and_gen3_file_info()

#create metadata submissions for files

##submitter_id	title	description	source	relation	subject	data_type	type	creator	projects.code

#make core metadata collection
#reference file submsision



