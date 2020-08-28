#!/bin/bash
declare -i job_file_present
declare -i vendor_file_present


while true
    do
    echo "--------------------------------------------------------------------------"
    echo "checking for Planner data $S3_PATH"
    echo "--------------------------------------------------------------------------"
    job_file_present=`s3cmd -c /home/pmf/s3-nsn-franklinpark ls $S3_PATH | awk '{print $4}' | grep job_site | wc -l`
    vendor_file_present=`s3cmd -c /home/pmf/s3-nsn-franklinpark ls $S3_PATH  | awk '{print $4}' | grep crew | wc -l`
    if (( "$job_file_present" >= 1 )) && (( "$vendor_file_present" >= 1 ));
    then
         echo "--------------------------------------------------------------------------"
         echo "Job details and Crew information both are available | Starting Evaluation ..."
         echo "--------------------------------------------------------------------------"
         s3cmd -c /home/pmf/s3-nsn-franklinpark setacl --recursive $S3_PATH*
         /usr/bin/python /home/pmf/vendor_allocation_module/gc_allocation.py $S3_END $S3_ACC $S3_SEC $S3_BUCKET $S3_CONFIG_FILE
    fi;
    if [ "$job_file_present" == 0 ];
    then
        echo "Job information is not present"
    fi;
    if [ "$vendor_file_present" == 0 ];
        then
            echo "Vendor information is not present"
        fi;
    sleep 10m;
done
