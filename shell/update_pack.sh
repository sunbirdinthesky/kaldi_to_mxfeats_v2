#!/bin/bash
. path.sh

if [ ! $# -eq 1 ]; then
    echo "update_pack.sh target_hdfs_dir"
    exit 1;
fi

target_hdfs_dir=$1

rm -f ./tmp/process_env.jar
hdfs dfs -rm $target_hdfs_dir/process_env.jar
jar cvf ./tmp/process_env.jar  conf/* bin/* utils/* steps/* data_input/* lib/* shell/*
hdfs dfs -put ./tmp/process_env.jar $target_hdfs_dir/
