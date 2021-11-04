# Incorporating Opioid Environment Policy Scan into JCOIN Data Commons

This project contains utilities to facilitate incorporating the Opioid
Environment Policy Scan (OEPS) into the JCOIN Data Commons. The OEPS was
developed for the Justice Community Opioid Innovation Network (JCOIN) by
Marynia Kolak, Qinyun Lin, Susan Paykin, Moksha Menghaney, and Angela Li of
the Healthy Regions and Policies Lab and Center for Spatial Data Science at
the University of Chicago.

## Download the gen3 client
Before uploading data, you must download and unzip the gen3 client from [here](https://github.com/uc-cdis/cdis-data-client/releases/tag/2021.11).

Remember to select the client consistent with your operating system (e.g., linux, mac, or windows).

## Configure the configuration file

After downloading the gen3 client, fill out the paramters in the config.yaml file to point to the gen3 client, gen3 client history (if you havent run the gen3 client yet then just put None), and local file directory. 

Additionally, specify where you would like to save the csv file containing file property information and the gen3 object ids. 

This config file will be used to upload files and create the metadata submission files.


```yaml
file_params:
  #path to opioid policy scan repo
  local_dir: 'c:/Users/kranz-michael/projects/opioid-policy-scan/data_final'
  #url of the JCOIN commons
  endpoint: 'https://jcoin.datacommons.io/'
  #path to the credentials.json that is created from navigating to https://jcoin.datacommons.io/identity# --> Create API Key
  credentials_path: 'credentials.json'
#path to gen3 client / history created by gen3 client configuration/submissions
file_upload_params:
  gen3_client_exe_path: 'c:/Users/kranz-michael/Documents/gen3-client.exe'
  gen3_history_path : 'C:/Users/kranz-michael/.gen3'
#path to where you want to save local files with file properties and gen3 object ids
csv_file_save_path: 'files.csv'
#path to the markdown file containing all the OEPS variable constructs
constructs_md : "c:/Users/kranz-michael/projects/opioid-policy-scan/README.md"
# parameters for creating the joined data files and metadata
joined_file_params:
  local_dir: 'data'
  endpoint: 'https://jcoin.datacommons.io/'
  credentials_path: 'credentials.json'
```

## Uploading Files

Files may be uploaded using the upload_files.py script:

```bash
> cd oeps_jdc
> python upload_files.py
```

Uploading files makes use of the [Gen3 Client](https://github.com/uc-cdis/cdis-data-client/releases)

This script will also create a csv file containing local files with their mapped
gen3 object_ids, file size, md5 check sum to create the metadata submission tsvs (see below)


## Updating Metadata

Uploaded files may be mapped to their metadata by creating tsv submissions 
and then uploading via the SDK or on the commons website.

Below creates the submission tsvs:

```bash
> cd oeps_jdc
> python create_metadata_submissions.py
```

Before running, make sure that you
have installed the dependencies in `requirements.txt` (ideally into a virtual
environment).

 In addition, you'll need to generate an API key from within the
JDC (under Profile) (and specify the path in the config file -- see above). 

## Mapping variable descriptions to variable names and metadata

WIP intermediate solution

the `data-dictionary-creation` folder contains code used to create the semi-manually curated variable mappings csv. This csv is useful because it provides a mapping of individual variables to variable constructs and files to create datasets containing as many variables as desired with an easy look up method 
(ie this file)

This `mapped_variable.csv` is located in the `data-dictionary-creation` directory.

The current process, is semi-automated. All materials are located in this directory and is as follows:

1. Create 2 json objects called `data-files-oeps.json` and `variables-oeps.json` by
copy/pasting the `data` and `variable` variables from the opioid-policy-scan repositories 
[map-config.js file (in the explorer branch)](https://github.com/GeoDaCenter/opioid-policy-scan/blob/explorer/map-config.js) into the script section of the `stringify_js_objects.html.`

The `variable` object contains the most up to date variable descriptions connected to each variable within each file name identifier.

The `data` variable connects the file name identifiers in `variable` to the actual file names.

The 2 JSON objects in the HTML output are saved as `data-files-oeps.json` and `variable-oeps.json` respectively.

2. run mapped_variables.py to get the complete list of variables tied to each file and the mapped variable descriptions.

Note, the majority of the variables are not mapped as they are input variables or are not yet included in
OEPS external tool.


This solution was decided upon to provide most up to date variable information (as any changes to variables in the OEPS tool are manually changed by the OEPS team here). Another source of data documentation is this [google doc here](https://docs.google.com/document/d/18NPWpuUfFTrKll9_ERHzVDmpNCETTzwjJt_FsIvmSrc/edit).

Future work after the data documentation becomes more stable may want to pull directly from the 'official' data documentation page.

## Joined Files

 The OEPS dataset is spread across multiple files. To make the rich data easier to use both in terms of findings individual variables and 
specific use cases that require a complete (or large) set of variables (such as principal compoennts analysis or other high dimensional statitistical techniques), joined files at each level are created with the `joined_datafiles.py` script.

This script reads in the files.csv file created (from the upload_files.py script)
and joins variables on each spatial level.

**It results in 8 files:**
- 4 spatial levels (zip, tract, county, state)
- 2 column name types (one with only variable names and the other with file prefix)

Additionally, to locate files with issues (e.g., duplicate geograhic ids or missing ids), a `files_issue.txt` is created. Note, files with duplicate geographic ids or missings ids are not included in the joined dataset.

Additionally, this script also creates metadata submission tsv files for the joined files.

