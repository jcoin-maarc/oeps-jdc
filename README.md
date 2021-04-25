# Incorporating Opioid Environment Policy Scan into JCOIN Data Commons

This project contains utilities to facilitate incorporating the Opioid
Environment Policy Scan (OEPS) into the JCOIN Data Commons. The OEPS was
developed for the Justice Community Opioid Innovation Network (JCOIN) by
Marynia Kolak, Qinyun Lin, Susan Paykin, Moksha Menghaney, and Angela Li of
the Healthy Regions and Policies Lab and Center for Spatial Data Science at
the University of Chicago.

## Uploading Files

Files may be uploaded using the
[Gen3 Client](https://github.com/uc-cdis/cdis-data-client/releases), e.g.:
    
    nohup gen3-client upload --profile=jdc --upload-path=*.csv

Verify all files were uploaded successfully with `tail nohup.out`. Of course,
if the file list is too long, then this will exceed the maximum argument size,
and you'll have to use `xargs`, e.g.:
    
    #!/bin/bash
    for f in *.csv; do
        gen3-client upload --profile=jdc --upload-path=$f
    done

## Updating Metadata

Uploaded files may be mapped to their metadata by running the script
`update_metadata.py` (Python 3 required). Before doing so, make sure that you
have installed the dependencies in `requirements.txt` (ideally into a virtual
environment). In addition, you'll need to generate an API key from within the
JDC (under Profile) and place the `credentials.json` file at the root of the
project.

This script uses the metadata stored in the `metadata` subdirectory. These
metadata may be updated at any time by editing the YAML files and rerunning
the script.
