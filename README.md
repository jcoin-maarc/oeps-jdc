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
```

## Uploading Files

Files may be uploaded using the upload_files.py script:

```bash
> cd oeps_jdc
> python upload_files.py
```

Uploading files makes use of the [Gen3 Client](https://github.com/uc-cdis/cdis-data-client/releases)

This script will also create a folder containing local files with their mapped
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

## Manual curation of mapped variables

the manual-curation folder contains code used to create the semi-manually curated variable mappings csv. This csv is useful because it provides a mapping of individual variables to variable constructs and files to create datasets containing as many variables as desired with an easy look up method 
(ie this file)

This `mapped_variable.csv` is located in the `metadata` directory.