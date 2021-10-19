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
#path to opioid policy scan repo

#for each file:
#get md5sum
#get list of variables

oeps_dir = 'c:/Users/kranz-michael/projects/opioid-policy-scan/data_final'
md_props = []
#md info list
md_test = "c:/Users/kranz-michael/projects/opioid-policy-scan/data_final/metadata/Health_DrugDeaths.md"
md_files = []
def get_markdown_sections(md_path,md_name):
    with open(md_path,'r',encoding='utf-8') as f:
        text = ' '.join(f.readlines())
        text_series = pd.Series(re.split('\n ### ',text))
        text_series.index = [ 
            re.sub(":|\n",'',x[0])
            for x in text_series.str.extract("(^.*:|^.*\n)").values
        ]
        text_series.name = md_name
    return text_series

def get_left_str(series,extract_pat,repl_pat=".*:|\n",strip_pat=' '):
    extracted_series = series.str.\
        extract(f"({extract_pat})",re.I)[0].str.\
        replace(repl_pat,"").str.\
        strip(strip_pat)
    return extracted_series

#moud
#metadata
#geometryFiles
file_paths = []
file_names = []
spatial_types = []
data_types = []
column_names = []
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
        if f.is_dir():
            get_files(f.path)

get_files(oeps_dir)

#%%
# pd.DataFrame(md_props).replace({'\n':' '},regex=True)
file_df = pd.DataFrame(
    {
        'file_path':file_paths,
        'file_name':file_names,
        'file_column_names':column_names,
        'file_spatial_types':spatial_types,
        'file_data_types':data_types
    }
)

is_geo = file_df.file_data_types=='geographic'
is_crosswalk = file_df.file_data_types=='crosswalk'

file_df['metadata'] = file_df.file_name\
    .str.replace("_.*\..*","")\
    .str.replace("COVID\d\d","COVID")

file_df.loc[is_geo,'metadata'] = 'geographic'
file_df.loc[is_crosswalk,'metadata'] = 'crosswalk'

#list files in each md
#make pd.DataFrame for variables with column for category
meta_df = pd.DataFrame(md_props)
#%%

#put loc and name for author etc
meta = get_left_str(meta_df.loc[:,'**Meta Data Name**'],"meta.*name.*:.*\n")
meta_df.insert(0,'metadata_name',meta)

modified = get_left_str(meta_df.loc[:,'**Meta Data Name**'],"modified.*:.*\n")
meta_df.insert(1,'modified_by',modified)

author = get_left_str(meta_df.loc[:,'**Meta Data Name**'],"author.*:.*\n")
meta_df.insert(2,'author',author)

data_loc = meta_df.loc[:,'Data Location'].str.\
    replace("Data Location.*\n","").str.strip("\n")

meta_df.insert(3,'data_location',data_loc)

source_desc = meta_df.loc[:,'Data Source(s) Description'].str.\
    replace("Data Source\(s\) Description.*\n","").str.strip("\n")
meta_df.insert(4,'desc_source_description',source_desc)

source_tables = meta_df.loc[:,'Description of Data Source Tables'].str.\
    replace("Description of Data Source Tables.*\n","").str.strip("\n")
meta_df.insert(5,'desc_source_tables',source_tables)

desc_data_proc = meta_df.loc[:,'Description of Data Processing'].str.\
    replace("Description.*\n","").str.strip("\n")
meta_df.insert(6,'desc_data_processing',desc_data_proc)

data_limitations = meta_df.loc[:,'Data Limitations'].str.\
    replace("Data Limitations.*\n","").str.strip("\n")
meta_df.insert(7,'data_limitations',data_limitations)

comments = meta_df.loc[:,'Comments/Notes'].str.\
    replace("Comments/Notes.*\n","").str.strip("\n")
meta_df.insert(8,'comments',comments)

meta_df['construct'] = meta_df.data_location.str.\
    extract("(PS|Health|Access|DS|BE|COVID|EC|crosswalk|geographic)")[0]

meta_df['metadata'] = meta_df.data_location.str.\
    extract("(((PS|Health|Access|DS|BE|EC|crosswalk|geographic)\d\d)|COVID|geographic|crosswalk)")[0]

var_and_definitions = meta_df.loc[:,'Key Variable and Definitions'].str.\
    replace("Key Variable and Definitions.*\n","").str.strip("\n")


def convert_md_to_df(md_name,md_table):
    string_table = StringIO(md_table.strip('\n '))
    df = pd.read_csv(string_table,sep="|").filter(regex='Variable|Description')
    df['md_name'] = md_name
    df.columns = [re.sub("^ | $",'',x) for x in df.columns]

    if 'Variable' in df:
        is_not_format_row = df['Variable'].str.extract("(:-)").isna().values
        return df.loc[is_not_format_row,:]
    else:
        return df

vars_df = pd.concat(
    [
        convert_md_to_df(md_name,md_table) 
        for md_name,md_table in var_and_definitions.iteritems()
    ]
)

# join files to metadata

# COVID split into multiple entries
test_csv_dfs = file_df.set_index('metadata')[['file_name','file_spatial_types','file_data_types']].\
    join(meta_df.set_index('metadata').loc[:,'metadata_name':]).\
    query("file_data_types=='csv'")

# join on metadata 

# join on metadata for data files
data_files_with_metadata_df = file_df.set_index('metadata').\
    join(meta_df.set_index('metadata')).\
    pipe(lambda df: df.loc[~df.file_spatial_types.isna()]) #all data files have some sort of spatial type

# join on file name for markdown files
md_files_with_metadata_df = file_df.set_index('metadata').\
    join(meta_df.set_index('metadata')).\
    pipe(lambda df: df.loc[df.file_spatial_types.isna()]) #markdown files dont have spatial type
files_with_metadata_df = pd.concat([data_files_with_metadata_df,md_files_with_metadata_df])

variable_construct_path = 'c:/Users/kranz-michael/projects/phs-rcg/oeps-jdc/variable_constructs.xlsx'
variable_constructs = pd.read_excel(variable_construct_path)
variable_constructs['metadata'] = (
    variable_constructs['Metadata']
    .str.replace("/ .*| /.*|/.*","",re.DOTALL)
    #.pipe(lambda s: s.where(s.str.extract('(COVID)')[0].isna(),'COVID'))
    .pipe(lambda s: s.where(s!='Geographic Boundaries','geographic'))
    .pipe(lambda s: s.where(s!='Crosswalk Files','crosswalk'))
    .str.replace(" ","") #typos
)
variable_constructs['dummy'] = 1

file_df['metadata_variable_constructs']  = (
    file_df.file_name
    .str.replace("_.*\..*","")
)

is_geo = file_df.file_data_types=='geographic'
is_crosswalk = file_df.file_data_types=='crosswalk'
file_df.loc[is_geo,'metadata_variable_constructs'] = 'geographic'
file_df.loc[is_crosswalk,'metadata_variable_constructs'] = 'crosswalk'

test_df = file_df.set_index('metadata_variable_constructs').join(variable_constructs.set_index('metadata')[['dummy']])


# 
