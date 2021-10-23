''' 
create core metadata submission tsv 
and reference file csvs (one for data files and one for metadata files):

note, many_to_many relationships so data files and metadata files are in multiple 
records --- each with a unique submitter id
'''

import pandas as pd
import yaml
import numpy as np
import re

#read in file dataframe created from the upload_data.py file
config = yaml.safe_load(open('config.yaml','r'))
files_df = pd.read_csv(config['csv_file_save_path'])

#add metadata from file names
contains = files_df.file_name.str.contains
#spatial types
is_state = contains("_S|state")
is_county = contains("_C|county")
is_zip = contains("_Z|zcta")
is_tract = contains("_T|tract")
is_location = contains("us-wide-moudsCleaned")
spatial_cond_list = [is_state,is_county,is_zip,is_tract,is_location]
spatial_choice_list = ['State','County','Zip','Tract','Location']
files_df['file_spatial_type'] = np.select(spatial_cond_list,spatial_choice_list,None)
#data types
is_gpkg = contains("\.gpkg$")
is_csv = contains("\.csv$")
is_md = contains("\.md$")
is_geographic = contains("dbf$|prj$|shp$|shx$")
is_crosswalk = contains('COUNTY_ZIP|TRACT_ZIP|ZIP_COUNTY|ZIP_TRACT')
data_type_cond_list = [is_gpkg,is_csv,is_md,is_geographic,is_crosswalk]
data_type_choice_list = ['gpkg','csv','md','geographic','crosswalk']
files_df['file_data_type'] = np.select(data_type_cond_list,data_type_choice_list,None)


#TODO: variable names and descriptions
#upload metadata
tbl = []
variable_constructs = []
with open(config['constructs_md'],'r',encoding='utf-8') as f:
    for line in f:
        if re.search("^###",line):
            variable_construct = re.sub("^### |\n","",line)
        
        if re.search(".*\|.*\|.*",line) \
            and not re.search("Variable Construct",line) \
            and not re.search(":-----",line):
            row = line.split("|")
            tbl.append(row)
            #for each table line, get the variable construct
            variable_constructs.append(re.sub(" |Variables","",variable_construct))

metadata_df = pd.DataFrame(tbl).loc[:,1:5]
metadata_df.columns = [
    'Variable Construct',
    'Variable Proxy',
    'Source',
    'Metadata',
    'Spatial Scale'
]
metadata_df['Themes'] = variable_constructs
#some variable constructs have same name so just use unique ids
metadata_df['id'] = (
    metadata_df['Themes'].str.lower() + 
    metadata_df.index.astype(str) 
)
# for data files -- get file prefixes to join with data file dataframe
metadata_df['metadata_for_data'] = (
    metadata_df.Metadata
    .str.replace(" ","")
    .str.extract('([A-Za-z]+\d\d|GeographicBoundaries|CrosswalkFiles)')[0]
    .str.replace("GeographicBoundaries","geographic") #geographic
    .str.replace("CrosswalkFiles","crosswalk") #crosswalk
)
#for markdown metadata files -- join on file name
metadata_df['metadata_for_markdown'] = metadata_df['Metadata'].str.replace(".*/metadata/|\)| ","")

# get file metadata names from file names
data_file_df = files_df.query("file_data_type!='md'")
data_file_df['metadata_variable_constructs']  = ( 
    data_file_df.file_name
    .str.replace("_.*\..*","")
)
#geographic and crosswalk are gleaned from data types and not file names
is_geo = data_file_df.file_data_type=='geographic'
is_crosswalk = data_file_df.file_data_type=='crosswalk'
data_file_df.loc[is_geo,'metadata_variable_constructs'] = 'geographic'
data_file_df.loc[is_crosswalk,'metadata_variable_constructs'] = 'crosswalk'


#make tsv files for submission
#reference file: get file name, spatial, and type of file
#core_metadata_collection : 
# ## core_metadata_collection.submitter_id, Variable Construct, Variable Proxy, Source, metadata_file
## TODO: get data limitations, data source, etc from extracted
core_metadata_collection_fields = {
    'id':'submitter_id',
    'Variable Construct':'title', 
    'Variable Proxy':'description',
     'Source':'source', 
     'Metadata':'relation',
    'Themes':'subject'
}

reference_fields = {
    'submitter_id':'submitter_id',
    'data_category':'data_category',
    'md5sum':'md5sum',
    'file_size':'file_size',
    'file_name':'file_name',
    'id':'core_metadata_collections.submitter_id',
    'file_spatial_type':'data_format',
    'file_data_type':'data_type',
    'gen3_object_id':'object_id'
}

core_metadata_collection = metadata_df\
    [core_metadata_collection_fields]\
    .assign(type='core_metadata_collection')\
    .rename(columns=core_metadata_collection_fields)

reference_data_df = data_file_df.set_index('metadata_variable_constructs')\
    .join(metadata_df.set_index('metadata_for_data'))\
    .assign(
        submitter_id = lambda x: x.id + "_" +\
            x.file_name\
            .str.lower().str.replace("\.","_"),
        data_category = 'data',
        type='reference_file'
    )\
    [reference_fields.keys()]\
    .rename(columns=reference_fields)

reference_md_df = metadata_df\
    .set_index('metadata_for_markdown')\
    .join(files_df.set_index('file_name'))\
    .assign(
        file_name = lambda x: x.index,
        file_data_type = 'markdown',
        file_spatial_type='documentation',
        data_category='documentation'
    )\
    .assign(
        submitter_id = lambda x: x.id + "_"
            + x.file_name.str.lower().str.replace("\.","_"),
        type='reference_file'
    )\
    [reference_fields.keys()]\
    .rename(columns=reference_fields)


reference_data_df.to_csv("metadata/reference_data_df.tsv",sep='\t',index=False)
reference_md_df.to_csv("metadata/reference_md_df.tsv",sep='\t',index=False)
core_metadata_collection.to_csv("metadata/core_metadata_collection.tsv",sep='\t',index=False)