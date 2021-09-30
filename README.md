# Incorporating Opioid Environment Policy Scan into JCOIN Data Commons

This project contains utilities to facilitate incorporating the Opioid
Environment Policy Scan (OEPS) into the JCOIN Data Commons. The OEPS was
developed for the Justice Community Opioid Innovation Network (JCOIN) by
Marynia Kolak, Qinyun Lin, Susan Paykin, Moksha Menghaney, and Angela Li of
the Healthy Regions and Policies Lab and Center for Spatial Data Science at
the University of Chicago.

## Uploading Files

Files may be uploaded by navigating into this repository and using the upload_data.sh script:

```
> cd oeps-jdc
> ./upload_data.sh --upload-data
```

Uploading files makes use of the [Gen3 Client](https://github.com/uc-cdis/cdis-data-client/releases)


## Updating Metadata

Uploaded files may be mapped to their metadata by running navigating to this repository and using the upload_data.sh as well:

```
> cd oeps-jdc
> ./upload_data.sh --update-metadata
```

This option makes use of
`update_metadata.py` (Python 3 required). Before doing so, make sure that you
have installed the dependencies in `requirements.txt` (ideally into a virtual
environment).

 In addition, you'll need to generate an API key from within the
JDC (under Profile) and place the `credentials.json` file in this repository. Alternatively, you can specify a path to your `credentials.json`:

```
> cd oeps-jdc
> ./upload_data.sh --update-metadata --credential-path <full path to your credentials.json>

```

This script uses the metadata stored in the `metadata` subdirectory. These
metadata may be updated at any time by editing the YAML files and rerunning
the script.


#TODO: add more documentation on other flags (especially including the data path as the default is currently a local directory)