''' 
reads in the json objects created from the .js files from the Spatial Center at UChicago's 
OEPS web application and joins with the entire list of variables to see which are mapped.

note: the stringify_js_objects.html was taken from the current js objects in the OEPS repo (explorer branch)
'''
import json
import pandas as pd
from pathlib import Path

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

