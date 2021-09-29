#!/bin/bash
OEPS_REPO='/mnt/c/Users/kranz-michael/projects/opioid-policy-scan/data_final'
CREDENTIALS_PATH='/mnt/c/Users/kranz-michael/Downloads/credentials.json'
#ENDPOINT='https://qa-jcoin.planx-pla.net'
ENDPOINT='https://jcoin.datacommons.io/'

./gen3-client configure --profile=jdc --cred=$CREDENTIALS_PATH --apiendpoint=$ENDPOINT

./gen3-client upload --profile=jdc --upload-path=$OEPS_REPO


# oeps_files=$(find $OEPS_REPO)

# for f in $oeps_files; do
#     echo "Uploading ${f}"
#     ./gen3-client upload --profile=jdc --upload-path=$f

# done