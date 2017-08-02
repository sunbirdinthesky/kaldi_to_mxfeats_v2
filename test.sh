#!/bin/bash -x 

#######################You need to change the following configurations###############
#queue=debugqueue  
queue=default
local_process_dir="." #change to local work dir
hdfs_dir="hdfs://yz-cpu-vm001.hogpu.cc:8020/user/chunqi.wang/test_mr" # the HDFS dir to save all data while script is running
hdfs_rir_noise_tar="hdfs://yz-cpu-vm001.hogpu.cc:8020/user/ding.liu/data/perturb_data.tar"
hdfs_aec_noise_tar="hdfs://yz-cpu-vm001.hogpu.cc:8020/user/ding.liu/perturb_data.tar"
alignment_resource="hdfs://yz-cpu-vm001.hogpu.cc:8020/user/ding.liu/data/resource_for_alignment"
snrs="20:19:18:17:16:15:14:13:12:11:10:9:8:7:6:5"
feat_out_prefix="20170708"
perturb_ratio=0.5 # how many clean data to be reverberated and noised 
random_seed=777

# speech feature shape
width=40 # feature dimension
height=21 # context
channels=3 # with delta and delta delta

# perturb_cmds="'origin' 'speed 1.1' 'speed 0.9' 'pitch 50' 'pitch 100'"
perturb_cmds="'origin' 'speed 1.1'"
num_spk_to_combine=2 # defalut is 100

# You need to provide the input with HDFS kaldi data and wave chunk information
kaldi_data_and_wave_chunk_list=$local_process_dir/data_input/total_hdfs_wholeset_clean_extract_input_shuffled_10.txt

rvb_with_noise=true # if reverberate and add noise
use_three_channel=true
use_four_channel=false
max_noise_per_minute=15

num_lines_to_convert=1 # default is 30
sentences_per_block=5000 # default is 5000
need_convert_alignment=false
###############End of configurations###############

if [[ $queue = "debugqueue" ]]; then
    process_env_hdfs_file=${hdfs_dir}/data_process_env/debugqueue/process_env.jar
elif [[ $queue = "default" ]]; then
    process_env_hdfs_file=${hdfs_dir}/data_process_env/default/process_env.jar
else
    echo "Wrong queue: $queue."
    exit 1
fi

# Find all generated combined kaldi data from last step as the input for next steps
perturb_input=${local_process_dir}/tmp/all_combined_kaldi_data_input                        #the name of combined_kaldi_data list 
perturb_input_name=`basename $perturb_input`                                                #the real name(with out the path) of the list file

output_mapred_log=$hdfs_dir/tmp
hdfs dfs -rm -r ${output_mapred_log}/aec_and_extract
#aec_and_extract.sh
yarn jar /usr/hdp/2.5.0.0-1245/hadoop-mapreduce/hadoop-streaming.jar                  \
-D mapred.task.timeout=60000000                                                       \
-D mapred.max.map.failures.percent=5                                                  \
-D mapred.job.queue.name=$queue                                                       \
-output       $output_mapred_log/aec_and_extract                                      \
-mapper       "getconf ARG_MAX"
