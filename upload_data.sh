#!/bin/bash

ENDPOINT='https://jcoin.datacommons.io'
OEPS_REPO='/mnt/c/Users/kranz-michael/projects/opioid-policy-scan/data_final'
CREDENTIALS_PATH='credientials.json'

# if no arguments are provided, return usage function
#MBK: no usage fxn so commented out for now
if [ $# -eq 0 ]; then
    #usage # run usage function
    #exit 1
    echo 'No commands but we have defaults'
    echo ENDPOINT: $ENDPOINT
    echo OEPS_REPO: $OEPS_REPO
    echo CREDENTIALS_PATH: $CREDENTIALS_PATH
fi

for arg in "$@"; do
    case $arg in
        -d | --data-dir)
            OEPS_REPO=$2
            shift # shift removes argument from from `$@`
            shift # 2nd shift remove value
            ;;
        -c | --credentials-path)
            CREDENTIALS_PATH=$2
            shift # Remove argument (-t) name from `$@`
            shift # Remove argument value (latest) from `$@`
            ;;
        -u | --data-commons-url)
            ENDPOINT=$2
            shift # Remove argument (-t) name from `$@`
            shift # Remove argument value (latest) from `$@`
            ;;
        -m | --update-metadata)
            is_metadata=true
            shift # Remove argument (-t) name from `$@`
            ;;
        -r | --upload-data)
            is_data=true
            shift # Remove argument (-t) name from `$@`
            ;;
        -h | --help)
            #usage # run usage function on help
            ;;
        *)
            #usage # run usage function if wrong argument provided
            ;;
    esac
done

if [[ $is_data ]]; then
    ./gen3-client configure --profile=jdc --cred=$CREDENTIALS_PATH --apiendpoint=$ENDPOINT

    ./gen3-client upload --profile=jdc --upload-path=$OEPS_REPO
else
    echo "Data upload not selected"
fi


# oeps_files=$(find $OEPS_REPO)

# for f in $oeps_files; do
#     echo "Uploading ${f}"
#     ./gen3-client upload --profile=jdc --upload-path=$f

# done


if [[ $is_metadata ]]; then
    python update_metadata.py \
    --endpoint $ENDPOINT \
    --program 'JCOIN' \
    --project 'OEPS' \
     --credentials $CREDENTIALS_PATH
else
    echo "Metadata update not selected"
fi