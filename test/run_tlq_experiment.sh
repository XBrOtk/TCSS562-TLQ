#!/bin/bash

args="--runs 1 --threads 1 --warmupBuffer 0 --combineSheets 0 --sleepTime 0 --openCSV 0"

bucketname="test.bucket.562f20.leb"
dblink="tlq.cluster-ca0mwlyio7u3.us-east-2.rds.amazonaws.com"
dbname="tlq"
username="admin"
password="password"
tablename="562_test"
region="Asia"

result_folder="tlq_results"
if [ -d "./$result_folder" ]
then
    rm -rf ./$result_folder
    mkdir ./$result_folder
else
    mkdir ./$result_folder
fi

for lang in Java Python
do
    aws lambda update-function-configuration --function-name Transform$lang --memory-size 3008 --timeout 900
    aws lambda update-function-configuration --function-name Load$lang --memory-size 3008 --timeout 900
    aws lambda update-function-configuration --function-name Query$lang --memory-size 3008 --timeout 900
done

for size in 1000 5000 10000 50000 100000
do
    size_folder=size_$size
    mkdir ./$result_folder/$size_folder
    for i in 1 2 3 4
    do
        for lang in Java Python
        do
            mkdir ./$result_folder/$size_folder/$lang

            transform_payload=""
            load_payload=""
            query_payload=""

            if [[ "$lang" == "Java" ]]
            then
                transform_payload="[{\"bucketName\":\"$bucketname\",\"fileName\":\"${size}_Sales_Records.csv\"}]"
                load_payload="[{\"bucketName\":\"$bucketname\",\"fileName\":\"Processed_${size}_Sales_Records.csv\",\"tableName\":\"$tablename\",\"dbLink\":\"$dblink\",\"dbName\":\"$dbname\",\"username\":\"$username\",\"password\":\"$password\"}]"
                query_payload="[{\"tableName\":\"$tablename\",\"dbLink\":\"$dblink\",\"dbName\":\"$dbname\",\"username\":\"$username\",\"password\":\"$password\",\"Region\":\"$region\"}]"
            else
                transform_payload="[{\"bucketname\":\"$bucketname\",\"key\":\"${size}_Sales_Records.csv\"}]"
                load_payload="[{\"bucketname\":\"$bucketname\",\"key\":\"Processed_${size}_Sales_Records.csv\",\"tablename\":\"$tablename\",\"dblink\":\"$dblink\",\"dbname\":\"$dbname\",\"username\":\"$username\",\"password\":\"$password\"}]"
                query_payload="[{\"tablename\":\"$tablename\",\"dblink\":\"$dblink\",\"dbname\":\"$dbname\",\"username\":\"$username\",\"password\":\"$password\",\"Region\":\"$region\"}]"
            fi

            echo
            echo
            echo "----- Iteration $i, size $size, $lang -------"
            echo
            echo

            echo
            echo "Transform"
            echo
            ./faas_runner.py -o ./$result_folder/$size_folder/$lang --function Transform$lang $args --payloads $transform_payload

            echo
            echo "Load"
            echo
            ./faas_runner.py -o ./$result_folder/$size_folder/$lang --function Load$lang $args --payloads $load_payload

            echo
            echo "Query"
            echo
            ./faas_runner.py -o ./$result_folder/$size_folder/$lang --function Query$lang $args --payloads $query_payload
        done
    done
done

echo "Finished"