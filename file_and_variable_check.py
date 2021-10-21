'''
extracts and structures all construct,file,and variable
level metadata for the OEPS data repo
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

def get_markdown_sections(md_path,md_name):
    with open(md_path,'r',encoding='utf-8') as f:
        text = ' '.join(f.readlines())
        text_series = pd.Series(re.split('\n ### ',text,re.MULTILINE))
        text_series.index = [ 
            re.sub(":|\n",'',x[0])
            for x in text_series.str.extract("(^.*\n)").values
        ]
        text_series.name = md_name
    return text_series.pipe(lambda x: x.str.replace(x.index+"\n | ","",regex=False))

def get_left_str(series,extract_pat,repl_pat=".*:|\n",strip_pat=' '):
    extracted_series = series.str.\
        extract(f"({extract_pat})",re.I)[0].str.\
        replace(repl_pat,"").str.\
        strip(strip_pat)
    return extracted_series

def get_md5sum(file_path):
    with open(file_path, "rb") as f:
        md5_hash = hashlib.md5()
        content = f.read()
        md5_hash.update(content)
    return md5_hash.hexdigest()

def get_filesize(file_path):
    return os.path.getsize(file_path)

def get_files(dir):
    for f in os.scandir(dir):
        if f.is_file():
            #data (ie one of the construct data files at top level dir)
            #TODO: change order from lowest to highest level
            if dir.endswith('data_final'):
                if f.name.endswith('csv'):
                    data_type = 'csv'
                    
                    if '_S' in f.name:
                        spatial = 'state'
                    elif '_Z' in f.name:
                        spatial = 'zcta'
                    elif '_T' in f.name:
                        spatial = 'tract'
                    elif '_C' in f.name:
                        spatial = 'county'
                    else:
                        print("Unknown spatial scale:")
                        print(f.name)
                        spatial = ''
                else:
                    data_type = None
                    spatial = None
            #locations (right now just mouds
            elif dir.endswith('moud'):
                spatial = 'locations'
                if f.name.endswith('csv'):
                    data_type = 'csv'
                elif f.name.endswith('gpkg'):
                    data_type = 'gpkg'
            #crosswalks
            elif dir.endswith('crosswalk'):
                data_type = 'crosswalk'
                spatial = f.name.lower().replace('xlsx','')
            #shapefiles
            elif re.search("dbf$|prj$|shp$|shx$",f.name):
                data_type = 'geographic'
                spatial = re.search("county|state|tract|zcta",dir).group(0)
            #metadata
            elif dir.endswith('metadata') and f.name.endswith(".md"):
                data_type = 'md'
                spatial = None
                #fxn to scrape md file
                md_props.append(get_markdown_sections(f.path,f.name))
            else:
                print(f"{f.name} is an unrecgonized file type")
                data_type = None
                spatial = None

            #get column names
            if f.name.endswith('csv'):
                cols = pd.read_csv(f.path).columns.values
            else:
                cols = None

            #append overall lists
            file_paths.append(f.path)
            file_names.append(f.name)
            data_types.append(data_type)
            spatial_types.append(spatial)
            column_names.append(cols)
            #file properties
            md5sums.append(get_md5sum(f.path))
            file_sizes.append(get_filesize(f.path))
        if f.is_dir():
            #run through function if not a directory that has its own files
            get_files(f.path)


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

#path to opioid policy scan repo
oeps_dir = 'c:/Users/kranz-michael/projects/opioid-policy-scan/data_final'
md_props = []
#md info list
md_test = "c:/Users/kranz-michael/projects/opioid-policy-scan/README.md"


md_files = []
file_paths = []
file_names = []
spatial_types = []
data_types = []
column_names = []
file_sizes = []
md5sums = []


get_files(oeps_dir)
file_df = pd.DataFrame(
    {
        'file_path':file_paths,
        'file_name':file_names,
        'file_column_names':column_names,
        'file_spatial_types':spatial_types,
        'file_data_types':data_types,
        'file_md5sums': md5sums,
        'file_sizes':file_sizes
    }
)

is_geo = file_df.file_data_types=='geographic'
is_crosswalk = file_df.file_data_types=='crosswalk'

file_df['metadata'] = file_df.file_name\
    .str.replace("_.*\..*","")\
    .str.replace("COVID\d\d","COVID")

file_df.loc[is_geo,'metadata'] = 'geographic'
file_df.loc[is_crosswalk,'metadata'] = 'crosswalk'


## check if current file is in gen3 and upload files that are not 

ENDPOINT = 'https://jcoin.datacommons.io/'
PROGRAM = 'JCOIN'
PROJECT = 'OEPS'

auth = Gen3Auth(refresh_file='credentials.json')
index = Gen3Index(ENDPOINT, auth)
sub = Gen3Submission(ENDPOINT, auth)


#read in files in directory
local_files_df = file_df.rename(columns={'file_md5sums':'md5sum',
    'file_sizes':'file_size',
    'file_name':'file_name'})
gen3_files_df = get_gen3_files(index)

files_df = local_files_df\
    .merge(gen3_files_df,on=['file_name','md5sum','file_size'],how='left',validate='one_to_one')\
    .rename(columns={"object_id":"gen3_object_id"})

files_df.to_csv('files_df.csv')
#upload data
#path to gen3 client
gen3_client_exe_path = os.path.realpath('c:/Users/kranz-michael/Documents/gen3-client.exe')
#paths to files transformed with operating system syntax
files_to_be_uploaded = files_df\
    .loc[files_df.gen3_object_id.isna()]\
    .file_path\
    .apply(os.path.realpath)
credentials_path = 'credentials.json'

#configure gen3 client for local submissions
creds = os.popen(f'{gen3_client_exe_path} configure --profile=jdc --cred={credentials_path} --apiendpoint={ENDPOINT}')
print(creds.read())

#upload data not in gen3
upload_output_list = []
for f in files_to_be_uploaded:    
    output = os.popen(f"{gen3_client_exe_path} upload --profile=jdc --upload-path={f}")
    output_text = output.read()
    print(output_text)
    upload_output_list.append(output_text)

#upload metadata

# the following commented out code extracts info from individual metadata files. 
# kept for future use but may need to change get_markdown fxn header split reg exs

#list files in each md
#make pd.DataFrame for variables with column for category
# meta_df = pd.DataFrame(md_props)
# #%%

# #put loc and name for author etc
# meta = get_left_str(meta_df.loc[:,'**Meta Data Name**'],"meta.*name.*:.*\n")
# meta_df.insert(0,'metadata_name',meta)

# modified = get_left_str(meta_df.loc[:,'**Meta Data Name**'],"modified.*:.*\n")
# meta_df.insert(1,'modified_by',modified)

# author = get_left_str(meta_df.loc[:,'**Meta Data Name**'],"author.*:.*\n")
# meta_df.insert(2,'author',author)

# data_loc = meta_df.loc[:,'Data Location'].str.\
#     replace("Data Location.*\n","").str.strip("\n")

# meta_df.insert(3,'data_location',data_loc)

# source_desc = meta_df.loc[:,'Data Source(s) Description'].str.\
#     replace("Data Source\(s\) Description.*\n","").str.strip("\n")
# meta_df.insert(4,'desc_source_description',source_desc)

# source_tables = meta_df.loc[:,'Description of Data Source Tables'].str.\
#     replace("Description of Data Source Tables.*\n","").str.strip("\n")
# meta_df.insert(5,'desc_source_tables',source_tables)

# desc_data_proc = meta_df.loc[:,'Description of Data Processing'].str.\
#     replace("Description.*\n","").str.strip("\n")
# meta_df.insert(6,'desc_data_processing',desc_data_proc)

# data_limitations = meta_df.loc[:,'Data Limitations'].str.\
#     replace("Data Limitations.*\n","").str.strip("\n")
# meta_df.insert(7,'data_limitations',data_limitations)

# comments = meta_df.loc[:,'Comments/Notes'].str.\
#     replace("Comments/Notes.*\n","").str.strip("\n")
# meta_df.insert(8,'comments',comments)

# meta_df['construct'] = meta_df.data_location.str.\
#     extract("(PS|Health|Access|DS|BE|COVID|EC|crosswalk|geographic)")[0]

# meta_df['metadata'] = meta_df.data_location.str.\
#     extract("(((PS|Health|Access|DS|BE|EC|crosswalk|geographic)\d\d)|COVID|geographic|crosswalk)")[0]

# var_and_definitions = meta_df.loc[:,'Key Variable and Definitions'].str.\
#     replace("Key Variable and Definitions.*\n","").str.strip("\n")


# def convert_md_to_df(md_name,md_table):
#     string_table = StringIO(md_table.strip('\n '))
#     df = pd.read_csv(string_table,sep="|").filter(regex='Variable|Description')
#     df['md_name'] = md_name
#     df.columns = [re.sub("^ | $",'',x) for x in df.columns]

#     if 'Variable' in df:
#         is_not_format_row = df['Variable'].str.extract("(:-)").isna().values
#         return df.loc[is_not_format_row,:]
#     else:
#         return df

# vars_df = pd.concat(
#     [
#         convert_md_to_df(md_name,md_table) 
#         for md_name,md_table in var_and_definitions.iteritems()
#     ]
# )

# # join files to metadata

# # COVID split into multiple entries
# test_csv_dfs = file_df.set_index('metadata')[['file_name','file_spatial_types','file_data_types']].\
#     join(meta_df.set_index('metadata').loc[:,'metadata_name':]).\
#     query("file_data_types=='csv'")

# join on metadata 

# # join on metadata for data files
# data_files_with_metadata_df = file_df.set_index('metadata').\
#     join(meta_df.set_index('metadata')).\
#     pipe(lambda df: df.loc[~df.file_spatial_types.isna()]) #all data files have some sort of spatial type

# # join on file name for markdown files
# md_files_with_metadata_df = file_df.set_index('metadata').\
#     join(meta_df.set_index('metadata')).\
#     pipe(lambda df: df.loc[df.file_spatial_types.isna()]) #markdown files dont have spatial type
# files_with_metadata_df = pd.concat([data_files_with_metadata_df,md_files_with_metadata_df])



# get variable construct table from README.md file (this seems to be most up to date as opposed to data doc)
tbl = []
variable_constructs = []
with open(md_test,'r',encoding='utf-8') as f:
    for line in f:
        if re.search("^###",line):
            variable_construct = re.sub("^### |\n","",line)
        
        if re.search(".*\|.*\|.*",line) \
            and not re.search("Variable Construct",line) \
            and not re.search(":-----",line):
            row = line.split("|")
            tbl.append(row)
            #for each table line, get the variable construct
            variable_constructs.append(variable_construct)

metadata_df = pd.DataFrame(tbl).loc[:,1:5]
metadata_df.columns = [
    'Variable Construct',
    'Variable Proxy',
    'Source',
    'Metadata',
    'Spatial Scale'
]
metadata_df['Variable Constructs'] = variable_constructs

#  get file prefixes to join with file dataframe
metadata_df['metadata_for_data'] = (
    metadata_df['Metadata']
    .str.replace("/ .*| /.*|/.*|\[|\]|\(.*| ","",re.DOTALL)
    .str.replace("GeographicBoundaries","geographic") #geographic
    .str.replace("CrosswalkFiles","crosswalk") #crosswalk
)
metadata_df['metadata_file_name'] = metadata_df['Metadata'].str.replace(".*/metadata/|\)| ","")
metadata_df['id'] = metadata_df['Variable Construct']\
    .str.strip(" ")\
    .str.lower()\
    .str.replace(" |\(|\)|\&|\/","_")\
+ metadata_df.index.astype(str) #some variable constructs have same name


## format and update metadata 
import pandas as pd
files_df = pd.read_csv("files_df.csv")

# get file metadata names from file names
data_file_df = files_df.query("file_data_types!='md'")
data_file_df['metadata_variable_constructs']  = ( 
    data_file_df.file_name
    .str.replace("_.*\..*","")
)

#geographic and crosswalk are gleaned from data types and not file names
is_geo = data_file_df.file_data_types=='geographic'
is_crosswalk = data_file_df.file_data_types=='crosswalk'
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
    'Variable Constructs':'subject'
}

reference_fields = {
    'submitter_id':'submitter_id',
    'data_category':'data_category',
    'md5sum':'md5sum',
    'file_size':'file_size',
    'file_name':'file_name',
    'id':'core_metadata_collections.submitter_id',
    'file_spatial_types':'data_format',
    'file_data_types':'data_type',
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
    .set_index('metadata_file_name')\
    .join(files_df.set_index('file_name'))\
    .assign(
        file_name = lambda x: x.index,
        file_data_types = 'markdown',
        file_spatial_types='documentation',
        data_category='documentation'
    )\
    .assign(
        submitter_id = lambda x: x.id + "_"
            + x.file_name.str.lower().str.replace("\.","_"),
        type='reference_file'
    )\
    [reference_fields.keys()]\
    .rename(columns=reference_fields)


reference_data_df.to_csv("reference_data_df.tsv",sep='\t',index=False)
reference_md_df.to_csv("reference_md_df.tsv",sep='\t',index=False)
core_metadata_collection.to_csv("core_metadata_collection.tsv",sep='\t',index=False)