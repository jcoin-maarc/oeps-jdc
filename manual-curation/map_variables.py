''' 
reads in the json objects created from the .js files from the Spatial Center at UChicago's 
OEPS web application and joins with the entire list of variables to see which are mapped.

note: the stringify_js_objects.html was taken from the current js objects in the OEPS repo (explorer branch)


'''
import json
import pandas as pd
from pathlib import Path
import numpy as np

with open('variables-oeps.json','r') as f:
    variables = pd.DataFrame(json.load(f)).set_index('numerator')

with open('data-files-oeps.json','r') as f:
    data_json = json.load(f)

data = (
    pd.concat([pd.Series(d['tables']) for d in data_json]).to_frame()
    .assign(file=lambda x: x[0].apply(lambda x:x['file']))
)
data.index.name = 'numerator' #consistent with variable index name


variables_with_file_name = data.join(variables).set_index(['file','nProperty'])

def get_csv_columns(df):
    cols = df.file_path.apply(lambda p: pd.read_csv(Path(p)).columns.values)
    return cols

data_files = pd.read_csv("files.csv").\
    pipe(lambda df: df.loc[df.file_name.str.contains("csv$")]).\
    assign(columns=lambda df: get_csv_columns(df)).\
    set_index('file_name')


data_files_and_variables = pd.concat(
    [pd.Series(x)
    .rename('nProperty')
    .to_frame()
    .assign(file=i) 
    for i,x in data_files['columns'].iteritems()]
).\
set_index(['file','nProperty']).\
join(variables_with_file_name)


# code to add more variables to manually curated file 
# ie after initial creation by above code
# manual_curation = pd.read_csv('metadata/mapped_variables.csv')
# determine_geo = lambda geoid: manual_curation.nProperty==geoid

# is_geo_list = [
#     determine_geo(x)
#     for x in ['STATEFP','COUNTYFP','ZCTA','TRACTCE','GEOID']
# ]
# geo_desc_list = [
#     '2-digit State code',
#     '5-digit County code (state + county)',
#     '5-digit ZIP Code Tract Area (ZCTA)',
#     '6-digit Census Tract designation',
#     'Unique 11-digit ID for Census Tracts (state + county + tract)'
# ]

# manual_curation['variable'] = np.select(
#     is_geo_list,
#     geo_desc_list,
#     manual_curation['variable']
# )

# manual_curation.to_csv('mapped_variables.csv')

#read in file dataframe created from the upload_data.py file
config = yaml.safe_load(open('config.yaml','r'))

#replace_space_with_percent = lambda s:s.file_name.str.replace(" ","%",regex=False)
files_df = (
    pd.read_csv(config['csv_file_save_path'])
    # .assign(
    #     file_name=lambda s: replace_space_with_percent(s) 
    #)
)
#add metadata from file names
contains = files_df.file_name.str.contains
#spatial types
is_state = contains("_S|state")
is_county = contains("_C|counties")
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
is_xlsx = contains('.xlsx$')
data_type_cond_list = [is_gpkg,is_csv,is_md,is_geographic,is_crosswalk]
data_type_choice_list = ['gpkg','Geographic Data','Documentation','Geographic Boundaries','Geographic Crosswalk']

files_df['file_data_type'] = np.select(data_type_cond_list,data_type_choice_list,None)

data_format_cond_list = [is_gpkg,is_csv,is_md,is_geographic,is_xlsx]
data_format_choice_list = ['GPKG','CSV','MD','SHAPEFILE','XLSX']
files_df['file_data_format'] = np.select(data_format_cond_list,data_format_choice_list)

#