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

input_file_list=`basename $kaldi_data_and_wave_chunk_list`
if [[ ! -f $kaldi_data_and_wave_chunk_list ]]; then
    echo "Could not find the input with HDFS kaldi data and wave chunk information."
    exit 1
fi

if [[ $width = 40 ]]; then
    fbank_config="${local_process_dir}/conf/fbank_40.conf"
elif [[ $width = 80 ]]; then
    fbank_config="${local_process_dir}/conf/fbank_80.conf"
else
    echo "Not support feature dimension: $width"
    exit 1
fi

if [[ $need_convert_alignment = false ]]; then
    num_pdf=2831
else
    num_pdf=6037
fi

output_kaldi_dir=$hdfs_dir/combined_kaldi_data
output_fbank_dir=$hdfs_dir/combined_fbank_feat
output_align_dir=$hdfs_dir/combined_alignment
output_mxfeat_dir=$hdfs_dir/mxfeat
output_log=$hdfs_dir/log
output_mapred_log=$hdfs_dir/mapred_log
ali_log_dir=$hdfs_dir/log/ali_log
hdfs_cmvn_dir=$hdfs_dir/combined_mapper_cmvn
hdfs_prior_dir=$hdfs_dir/combined_mapper_prior


#edited by chunqi.wang@17-07-25 10:37, deleted some useless item, added parallel computation
hdfs dfs -rm -r $hdfs_dir
hdfs dfs -mkdir -p $hdfs_dir
hdfs dfs -mkdir $output_kaldi_dir &
hdfs dfs -mkdir $output_fbank_dir &
hdfs dfs -mkdir $output_align_dir &
hdfs dfs -mkdir $output_mxfeat_dir &
hdfs dfs -mkdir $output_log &
hdfs dfs -mkdir $output_mapred_log &
hdfs dfs -mkdir $hdfs_prior_dir &
wait
hdfs dfs -mkdir -p $ali_log_dir &
hdfs dfs -mkdir -p $output_mxfeat_dir &
hdfs dfs -mkdir -p $hdfs_cmvn_dir &
wait
hdfs dfs -chmod -R 777 $hdfs_dir
rm -rf ./tmp/*
#end edition

process_env_hdfs_dir=`dirname $process_env_hdfs_file`
hdfs dfs -mkdir -p $process_env_hdfs_dir
. ./update_pack.sh $(dirname $process_env_hdfs_file)
hdfs dfs -put -f $local_process_dir/data_input/ $hdfs_dir

# 1. Combine all input kaldi data each mapper 
# 2. perturb with speed and pitch commands 
# 3. extract features 
# 4. compute alignments for original and perturbed with speed commands ones
#     note: width    : the width(dim) of the frame
#           heigth   : context of the input
#           channels : 1 = original
#                      2 = original + delta
#                      3 = original + delta + delta delta
#           output_kaldi_dir : dir to store combined_kaldi_data
#           output_fbank_dir : dir to store combined_fbank_feat
#           output_align_dir : dir to store combined_alignment data
#           output_log       : dir to store logs for "combine_extract_align.sh"
#           perturb_cmds     : cmd about how many kinds of data and what kind of data we want
yarn jar /usr/hdp/2.5.0.0-1245/hadoop-mapreduce/hadoop-streaming.jar    \
-D mapred.line.input.format.linespermap=$num_spk_to_combine             \
-D mapred.task.timeout=60000000                                         \
-D mapred.max.map.failures.percent=5                                    \
-D mapreduce.map.memory.mb=6000                                         \
-D mapred.job.queue.name=$queue                                         \
-archives     ${process_env_hdfs_file}#process_env                      \
-files        $alignment_resource                                       \
-input        $hdfs_dir/data_input/${input_file_list}                   \
-inputformat  org.apache.hadoop.mapred.lib.NLineInputFormat             \
-output       $output_mapred_log/mapred_combine_extract_align_log       \
-mapper       "./process_env/shell/combine_extract_align.sh             \
                  --width             $width                            \
                  --height            $height                           \
                  --channels          $channels                         \
                  $output_kaldi_dir $output_fbank_dir $output_align_dir $output_log $perturb_cmds"


# Find all generated combined kaldi data from last step as the input for next steps
perturb_input=${local_process_dir}/tmp/all_combined_kaldi_data_input                        #the name of combined_kaldi_data list 
perturb_input_name=`basename $perturb_input`                                                #the real name(with out the path) of the list file
hdfs dfs -find $output_kaldi_dir -name "*_kaldi_data_*" | shuf > $perturb_input             #get and shuf the data, then write it into the list file
hdfs dfs -put -f $perturb_input $hdfs_dir/data_input                                        #upload list file
num_lines=`wc -l $perturb_input | cut -d ' ' -f 1`                                          #conculate the length of the list file
num_lines_for_perturb=$(echo "$num_lines * $perturb_ratio" | bc)                            #conculate how many files need to be pertubed
num_lines_for_perturb=${num_lines_for_perturb%.*}                                           #remove the decimal part
head -${num_lines_for_perturb} ${perturb_input} > ${perturb_input}_ratio_${perturb_ratio}   #generate new list file
hdfs dfs -put -f ${perturb_input}_ratio_${perturb_ratio} $hdfs_dir/data_input/              #upload new list file

##aec_and_extract.sh
#yarn jar /usr/hdp/2.5.0.0-1245/hadoop-mapreduce/hadoop-streaming.jar                  \
#-D mapred.line.input.format.linespermap=1                                             \
#-D mapred.task.timeout=60000000                                                       \
#-D mapred.max.map.failures.percent=5                                                  \
#-D mapred.map.max.attempts=1                                                          \
#-D mapred.job.queue.name=$queue                                                       \
#-archives     ${process_env_hdfs_file}#process_env,${hdfs_aec_noise_tar}#perturb_data \
#-files        $alignment_resource                                                     \
#-input        $hdfs_dir/data_input/${perturb_input_name}_ratio_${perturb_ratio}       \
#-inputformat  org.apache.hadoop.mapred.lib.NLineInputFormat                           \
#-output       $output_mapred_log/aec_and_extract                                      \
#-mapper       "./process_env/shell/aec_and_extract.sh                   \
#                --fbank-config            $fbank_config                 \
#                --random-seed             $random_seed                  \
#                --output-perturb-path     $output_kaldi_dir             \
#                --snrs                    $snrs                         \
#                --max-noise-per-minute    $max_noise_per_minute         \
#                --hdfs-rir-noise-tar-dir  $hdfs_aec_noise_tar           \
#                --output-fbank-dir        $output_fbank_dir             \
#                --hdfs-dir                $hdfs_dir                     \
#                --output-fbank-log        $output_log"


# Reverberate nearfield data
# note :
#     only_add_noise : add noise only(do not do reverb)
#     rvb_with_noise : reverb with noise
#     fbank_config   : config files for making fbank files
#     random_seed    : random_seed for doing reverb
#     output-perturb-part  : output dir to store kaldi data (wav.scp, spk2utt and so on)
#     use_three_channel    : ?
#     use_four_channel     : ?
#     snrs                 : snr for reverb and noise
#     max_noise_per_minute : how many noise parts can be added per minute (record length, not real time)
#     hdfs_rir_noise_tar   : tar file contains noise and rir files
#     output_fbank_dir     : fbank store dir
#     hdfs_dir             : hdfs target dir
#     output_mapred_log    : log file path for "perturb_and_extract_combined.sh" 
only_add_noise=false
yarn jar /usr/hdp/2.5.0.0-1245/hadoop-mapreduce/hadoop-streaming.jar                          \
-D mapred.line.input.format.linespermap=1                                                     \
-D mapred.task.timeout=60000000                                                               \
-D mapred.max.map.failures.percent=5                                                          \
-D mapred.map.max.attempts=2                                                                  \
-D mapred.job.queue.name=$queue                                                               \
-archives     ${process_env_hdfs_file}#process_env,${hdfs_rir_noise_tar}#perturb_data         \
-input        $hdfs_dir/data_input/${perturb_input_name}_ratio_${perturb_ratio}               \
-inputformat  org.apache.hadoop.mapred.lib.NLineInputFormat                                   \
-output       $output_mapred_log/rvb_mapred_perturb_and_extract_combined_noise_$random_seed   \
-mapper       "./process_env/shell/perturb_and_extract_combined.sh      \
                --only-add-noise            $only_add_noise             \
                --rvb-with-noise            $rvb_with_noise             \
                --fbank-config              $fbank_config               \
                --random-seed               $random_seed                \
                --output-perturb-path       $output_kaldi_dir           \
                --use-three-channel         $use_three_channel          \
                --use-four-channel          $use_four_channel           \
                --snrs                      $snrs                       \
                --max-noise-per-minute      $max_noise_per_minute       \
                --hdfs-rir-noise-tar-dir    $hdfs_rir_noise_tar         \
                --output-fbank-dir          $output_fbank_dir           \
                --hdfs-dir                  $hdfs_dir                   \
                --output-fbank-log          $output_log"


# Add noise to both nearfield and farfield data
# note :
#     only_add_noise : add noise only(do not do reverb)
#     rvb_with_noise : reverb with noise
#     fbank_config   : config files for making fbank files
#     random_seed    : random_seed for doing reverb
#     output-perturb-part : output dir to store kaldi data (wav.scp, spk2utt and so on)
#     use_three_channel   : ?
#     use_four_channel    : ?
#     snrs                : snr for reverb and noise
#     max_noise_per_minute : how many noise parts can be added per minute (record length, not real time)
#     hdfs_rir_noise_tar   : tar file contains noise and rir files
#     output_fbank_dir     : fbank store dir
#     hdfs_dir             : hdfs target dir
#     output_mapred_log    : log file path for "perturb_and_extract_combined.sh" 
only_add_noise=true
yarn jar /usr/hdp/2.5.0.0-1245/hadoop-mapreduce/hadoop-streaming.jar                            \
-D mapred.line.input.format.linespermap=1                                                       \
-D mapred.task.timeout=60000000                                                                 \
-D mapred.max.map.failures.percent=5                                                            \
-D mapred.map.max.attempts=2                                                                    \
-D mapred.job.queue.name=$queue                                                                 \
-archives     ${process_env_hdfs_file}#process_env,${hdfs_rir_noise_tar}#perturb_data           \
-input        $hdfs_dir/data_input/${perturb_input_name}_ratio_${perturb_ratio}                 \
-inputformat  org.apache.hadoop.mapred.lib.NLineInputFormat                                     \
-output       $output_mapred_log/noise_mapred_perturb_and_extract_combined_noise_$random_seed   \
-mapper       "./process_env/shell/perturb_and_extract_combined.sh      \
                --only-add-noise          $only_add_noise               \
                --rvb-with-noise          $rvb_with_noise               \
                --fbank-config            $fbank_config                 \
                --random-seed             $random_seed                  \
                --output-perturb-path     $output_kaldi_dir             \
                --use-three-channel       $use_three_channel            \
                --use-four-channel        $use_four_channel             \
                --snrs                    $snrs                         \
                --max-noise-per-minute    $max_noise_per_minute         \
                --hdfs-rir-noise-tar-dir  $hdfs_rir_noise_tar           \
                --output-fbank-dir        $output_fbank_dir             \
                --hdfs-dir                $hdfs_dir                     \
                --output-fbank-log        $output_log"


# Find all feats.scp and the ali files from original and perturbed with speed
hdfs dfs -find $output_kaldi_dir -name "feats.scp" > $local_process_dir/tmp/all_feats_scp_list
hdfs dfs -find $output_align_dir -name "*ali.1.gz" > $local_process_dir/tmp/all_ali_gz_list


# Map each feats.scp with its ali file
# because of we generated a lot of new fbank(wav) records, we must generate new ali_pdf records for them
python ./steps/mapping.py                    \
  $local_process_dir/tmp/all_mxfeat_input    \
  $local_process_dir/tmp/all_feats_scp_list  \
  $local_process_dir/tmp/all_ali_gz_list
shuf $local_process_dir/tmp/all_mxfeat_input > $local_process_dir/tmp/all_mxfeat_input_shuffled
hdfs dfs -put -f $local_process_dir/tmp/all_mxfeat_input_shuffled $hdfs_dir/data_input


# Map each feat and ali on utterance level and convert them into mxnet format
yarn jar /usr/hdp/2.5.0.0-1245/hadoop-mapreduce/hadoop-streaming.jar          \
-D mapred.line.input.format.linespermap=$num_lines_to_convert                 \
-D mapred.task.timeout=60000000                                               \
-D mapred.max.map.failures.percent=5                                          \
-D mapred.map.max.attempts=1                                                  \
-D mapred.job.queue.name=$queue                                               \
-archives     ${process_env_hdfs_file}#process_env                            \
-files        $alignment_resource                                             \
-input        $hdfs_dir/data_input/all_mxfeat_input_shuffled                  \
-inputformat  org.apache.hadoop.mapred.lib.NLineInputFormat                   \
-output       $output_mapred_log/mapred_mxfeat_log                            \
-mapper       "./process_env/shell/prepare_mxfeature_from_combined_kaldi.sh   \
                  --need-convert-alignment  $need_convert_alignment           \
                  --hdfs-mxfeature-dir      $output_mxfeat_dir                \
                  --hdfs-cmvn-dir           $hdfs_cmvn_dir                    \
                  --hdfs-prior-dir          $hdfs_prior_dir                   \
                  --ali-log-dir             $ali_log_dir                      \
                  --sentences-per-block     $sentences_per_block              \
                  --feat-out-prefix         $feat_out_prefix"

# Compute pdf prior
hdfs dfs -get $hdfs_prior_dir ./tmp/
local_prior_name=`basename $hdfs_prior_dir`
cat ./tmp/$local_prior_name/* > ./tmp/total_prior_tmp
python ./steps/compute_priors.py ./tmp/total_prior_tmp $num_pdf final_pdf_prior_prob

# Compute mxnet meanvar
hdfs dfs -get $hdfs_cmvn_dir ./tmp/
local_cmvn_name=`basename $hdfs_cmvn_dir`
cat ./tmp/$local_cmvn_name/* | grep -v "\[" | sed "s/\]//g"  > ./tmp/total_cmvn_tmp
python ./steps/save_meanvar.py ./tmp/total_cmvn_tmp meanvar
