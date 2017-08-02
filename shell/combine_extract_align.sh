#!/bin/bash -x

export LD_LIBRARY_PATH=./process_env/lib/
export PATH=$PATH:./process_env/

# Begin configuration section.
nj=1
cmd=./process_env/utils/run.pl
fbank_config=./process_env/conf/fbank_40.conf
compress=true
# Begin alignment configuration.
stage=0
# 1. scale_opts
self_loop_scale=0.1
acoustic_scale=0.1
transition_scale=1.0
beam=10
retry_beam=15
# 2. nnet_forward_opts
no_softmax=false # set false for mxnet models and set true for kaldi models
prior_scale=1.0
width=80 # feature dimension
height=21 # context
channels=3 # including delta, delta-delta
# 3. adaptation
ivector=            # rx-specifier with i-vectors (ark-with-vectors),
# 4. lat settings
align_to_lats=false # optionally produce alignment in lattice format
graph_batch_size=125 # default is 250
# lat_decode_opts
lat_decode_acoustic_scale=0.1
lat_decode_beam=20
lattice_beam=10
# lat_graph_scale
lat_graph_transition_scale=1.0
lat_graph_self_loop_scale=0.1

# 5. GPU setting
use_gpu="no" # yes|no|optionaly
# End alignment configuration.

hisf_1mic="./process_env/bin/hisf_linux_1mic stdin.wav stdout.wav 2 0 0 0 1 300 2 0 a.ini |"
highpass_cmd="highpass 100"
# End configuration options.

if [ -f path.sh ]; then . ./path.sh; fi
. ./process_env/utils/parse_options.sh || exit 1;
. ./process_env/utils/get_timing.sh || exit 1;

output_kaldi_dir=$1
output_fbank_dir=$2
output_align_dir=$3
output_log=$4
shift 4

lang=./resource_for_alignment/lang
srcdir=./resource_for_alignment/relu_dnn_reverb_haitian_and_accent_with_cmd_more_noise
if [[ $width = 80 ]]; then
    fbank_config=./process_env/conf/fbank_80.conf
    nnet_json=./resource_for_alignment/mxnet_models/cnn_perturb_speech_cl15r5_b512-9-symbol.json
    nnet_params=./resource_for_alignment/mxnet_models/cnn_perturb_speech_cl15r5_b512-9-0008.params_nhwc
    nnet_cmvn=./resource_for_alignment/mxnet_models/cmvn_80.cmvn
elif [[ $width = 40 ]]; then
    fbank_config=./process_env/conf/fbank_40.conf
    nnet_json=./resource_for_alignment/mxnet_models/deepDnn_nnvm_base_lc15rc5.json
    nnet_params=./resource_for_alignment/mxnet_models/cnn_perturb_combinedHisfBig_speech-5-0008.params_nhwc
    nnet_cmvn=./resource_for_alignment/mxnet_models/cmvn_40.cmvn
else
    echo "Not support for the feature dimension: $width"
    exit 1
fi

# Begin setting HDFS output dirs
mapper=`printenv mapred_task_id | cut -d "_" -f 5`
hdfs_tgt_combined_kaldi_data_dir=${output_kaldi_dir}
hdfs_tgt_combined_fbank_feat_dir=${output_fbank_dir}
hdfs_tgt_combined_align_dir=${output_align_dir}
hdfs_tgt_combined_fbank_log_dir=${output_log}
# End setting HDFS output dirs


start_time=`date +%s.%N`
idx=0
kaldi_data_dirs=()
wave_files=()
hdfs_wave_files=()
while read line #read from stdin
do
    hadoop_key=`echo $line | awk '{print $1}'`
    hdfs_kaldi_data_dir=`echo $line | awk '{print $2}'`   
    hdfs dfs -get $hdfs_kaldi_data_dir
    local_kaldi_data_dir=`basename $hdfs_kaldi_data_dir`
    kaldi_data_dirs[idx]=$local_kaldi_data_dir    #kaldi data dirs read from "kaldi_data_and_wave_chunk_list"

    hdfs_wave_file=`echo $line | awk '{print $3}'`
    hdfs_wave_files[idx]=$hdfs_wave_file    #path to wav chunk on the hdfs system
    hdfs dfs -get $hdfs_wave_file
    local_wav_file=`basename $hdfs_wave_file`
    wave_files[idx]=$local_wav_file    #download wav chunk from hdfs to the node, and this is the list of them

    idx=$((idx + 1))
done
end_time=`date +%s.%N`
echo "hdfs get time:"
getTiming $start_time $end_time


start_time=`date +%s.%N`
perturb_idx=0
for i in $(seq $#)  #i = 1, 2, 3 ...... n_params - 2, n_params - 1, n_params
do
    if [ i -le 4] #pass the first four param
    then 
      debug=${!i}
      echo "param_i = "${debug}
      continue 
    fi

    perturb=${!i}   #perturb = params_1, params_2 ..... params_n
    value=`echo $perturb | cut -d ' ' -f 2`
    key_prefix=$(echo $perturb | sed -e 's/[ \t]//g')_
    perturb_suffix=$(echo $perturb | sed -e 's/[ \t]//g')_${mapper}
    if [[ $perturb =~ "origin" ]]; then
        perturb_cmd=""
    else
        perturb_cmd=$perturb
    fi
    idx=0
    perturb_kaldi_data_dirs=()

    rm -f total_wav_scp_ori
    for kaldi_dir in ${kaldi_data_dirs[*]}
    do
        current_dir=$(pwd)
        wave_file=${wave_files[idx]} #name of wave chunk file 
        old_wav_path=`cat ${kaldi_dir}/wav.scp | cut -d ' ' -f 2`
        old_wav_prefix=`dirname $old_wav_path`
        sed -i "s|$old_wav_prefix|$current_dir|g" ${kaldi_dir}/wav.scp   #notice : wav.scp has only one line, so don`t worry about changing the path from correct to wrong

        # Make a copy of kaldi data for each perturb_cmd and add the perturb prefix to keys to avoid duplicated keys
        dir_perturb=${kaldi_dir}_${perturb_suffix}
        mkdir -p ${dir_perturb}
        ./process_env/utils/copy_data_dir.sh --spk-prefix $key_prefix --utt-prefix $key_prefix ${kaldi_dir} ${dir_perturb} #copy kaldi dir from one dir to another after added prefix
        ./process_env/utils/validate_data_dir.sh --no-feats ${dir_perturb} || exit 1  #valid copyed dir, exit if failed
        wave_name=`head -1 ${dir_perturb}/wav.scp | cut -d ' ' -f 1` #infact, it`s still only one line in wav.scp

        # Haitian farfield waves have 8 channels, just ignore "hisf_1mic" and don`t think about it
        if [[ $wave_file =~ "DISTANCE_" ]]; then
            #highpass, remove low-frequency noise. also add sound track 1(channel 1) to the new file
            echo "$wave_name sox $current_dir/$wave_file -twav - remix 1 $highpass_cmd  $perturb_cmd | $hisf_1mic " > ${dir_perturb}/wav.scp 
        else
            #just do highpass, because of it has only one sound track
            echo "$wave_name sox $current_dir/$wave_file -twav - $highpass_cmd $perturb_cmd | $hisf_1mic " > ${dir_perturb}/wav.scp
        fi

        # Recompute the kaldi data for those perturbed with the speed cmd
        # after we changed the speaker`s talking speed, the length of the wav file will changed. so we need recompute the alignment data and 
        start_time=`date +%s.%N`
        if [[ $perturb =~ "speed" ]]; then
            wav_path=`cat ${kaldi_dir}/wav.scp | cut -d ' ' -f 2` #of course, wav.scp has only one line
            while read seg_line   #this part of code will cut the wav from a chunk to its origin
            do
                start=`echo $seg_line | cut -d ' ' -f 3`
                end=`echo $seg_line | cut -d ' ' -f 4`
                wav_name=`echo $seg_line | cut -d' ' -f 1`
                dur=$(echo "scale=5; $end - $start" | bc)
                sox $wav_path ${dir_perturb}/${wav_name}.wav trim $start $dur
                echo "${wav_name} ${dir_perturb}/${wav_name}.wav" >> ${dir_perturb}/seg_wav.scp
            done < ${dir_perturb}/segments  #ATTENTION!!! input is here, stdin has been redirected to this file
                              #in file segments, the format is: "wav_origin_name  wav_chunk_name  origin_wav`s_start_time_in_chunk_wav  origin_wav`s_end_time_in_chunk_wav"

            #>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>mark why not just change the file wav.duration <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
            awk -v perturb="$perturb" '{print $1, "sox", $2, "-twav -", perturb, "|"}' ${dir_perturb}/seg_wav.scp > ${dir_perturb}/perturbed_seg_wav.scp
            ./process_env/bin/wav-to-duration --read-entire-file=true scp:${dir_perturb}/perturbed_seg_wav.scp ark,t:${dir_perturb}/wav.duration
            awk -v wav_chunk=$wave_name 'BEGIN{offset=0} {cur_start=offset; cur_end=offset + $2; offset += $2; print $1, wav_chunk, cur_start, cur_end; }' ${dir_perturb}/wav.duration > ${dir_perturb}/segments
            ./process_env/bin/wav-to-duration --read-entire-file=true scp:${dir_perturb}/wav.scp ark,t:${dir_perturb}/reco2dur
        fi

        ./process_env/utils/validate_data_dir.sh --no-feats ${dir_perturb} || exit 1
        end_time=`date +%s.%N`
        echo "Recomputing kaldi data time for $perturb:"
        getTiming $start_time $end_time


        perturb_kaldi_data_dirs[idx]=${dir_perturb}
        hdfs_wav_path=${hdfs_wave_files[idx]}
        echo "$wave_name $hdfs_wav_path" >> total_wav_scp_ori
        idx=$((idx + 1))
    done

    start_time=`date +%s.%N`
    combined_dir=combined_kaldi_data_${perturb_suffix}
    # Combining the all kaldi data within this mapper
    ./process_env/utils/combine_data.sh $combined_dir ${perturb_kaldi_data_dirs[*]}
    end_time=`date +%s.%N`
    echo "Combining time for $perturb:"
    getTiming $start_time $end_time


    start_time=`date +%s.%N`
    combined_fbank_feat_dir=combined_fbank_feat_${perturb_suffix}
    combined_fbank_log_dir=combined_fbank_log_${perturb_suffix}
    # Extracting the fbank features from combined data
    ./process_env/steps/make_fbank.sh --nj $nj --cmd $cmd --fbank-config $fbank_config --compress $compress ${combined_dir} ${combined_fbank_log_dir} ${combined_fbank_feat_dir}
    end_time=`date +%s.%N`
    echo "Exrtacting Fbank time for $perturb:"
    getTiming $start_time $end_time


    if [[ ! $perturb =~ "pitch" && ! $perturb =~ "origin" ]]; then
        start_time=`date +%s.%N`
        combined_align_dir=combined_align_${perturb_suffix}
        ./process_env/steps/align.sh                                  \
            --nj                          $nj                         \
            --cmd                         $cmd                        \
            --stage                       $stage                      \
            --beam                        $beam                       \
            --retry-beam                  $retry_beam                 \
            --use-gpu                     $use_gpu                    \
            --width                       $width                      \
            --height                      $height                     \
            --channels                    $channels                   \
            --self-loop-scale             $self_loop_scale            \
            --acoustic-scale              $acoustic_scale             \
            --transition_scale            $transition_scale           \
            --no-softmax                  $no_softmax                 \
            --prior-scale                 $prior_scale                \
            --align-to-lats               $align_to_lats              \
            --lattice-beam                $lattice_beam               \
            --lat-decode-acoustic-scale   $lat_decode_acoustic_scale  \
            --lat-decode-beam             $lat_decode_beam            \
            --lat-graph-transition-scale  $lat_graph_transition_scale \
            --lat-graph-self-loop-scale   $lat_graph_self_loop_scale  \
            --graph-batch-size            $graph_batch_size           \
            ${combined_dir}  $lang  $srcdir  $nnet_json  $nnet_params  $nnet_cmvn  $combined_align_dir
        end_time=`date +%s.%N`
        echo "Computing alignment time for $perturb:"
        getTiming $start_time $end_time
    fi

    # Change back to the original wav.scp for the next script.
    cat total_wav_scp_ori > $combined_dir/wav.scp
    # Replace the current container path with the real HDFS path
    sed -i "s|$current_dir|$hdfs_tgt_combined_fbank_feat_dir|g" $combined_dir/feats.scp

    start_time=`date +%s.%N`
    rm -r $combined_dir/split*
    hdfs dfs -put -f $combined_dir            $hdfs_tgt_combined_kaldi_data_dir &
    hdfs dfs -put -f $combined_fbank_feat_dir $hdfs_tgt_combined_fbank_feat_dir &
    hdfs dfs -put -f $combined_fbank_log_dir  $hdfs_tgt_combined_fbank_log_dir  &
    wait
    # only upload the ali and log files to hdfs
    hdfs dfs -mkdir  $hdfs_tgt_combined_align_dir/$combined_align_dir
    hdfs dfs -put -f $combined_align_dir/ali.1.gz         $hdfs_tgt_combined_align_dir/$combined_align_dir
    hdfs dfs -put -f $combined_align_dir/log/align.1.log  $hdfs_tgt_combined_align_dir/$combined_align_dir
    end_time=`date +%s.%N`
    echo "hdfs put time for $perturb:"
    getTiming $start_time $end_time

    perturb_idx=$((perturb_idx + 1))
done
end_time=`date +%s.%N`
echo "Total perturb time:"
getTiming $start_time $end_time


echo "done!"
