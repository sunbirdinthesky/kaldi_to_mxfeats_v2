#!/bin/bash -x

Reverberate() {
    src_dir=$1
    dest_dir=$2
    key_prefix=$3
    speech_rvb_prob=$4
    rir_file=$5

    ./process_env/steps/data/reverberate_data_dir.py                                \
        --rir-set-parameters                      $rir_file                         \
        --noise-set-parameters                    $noise_file                       \
        --num-replications                        $num_replications                 \
        --foreground-snrs                         $foreground_snrs                  \
        --background-snrs                         $background_snrs                  \
        --prefix                                  $key_prefix                       \
        --speech-rvb-probability                  $speech_rvb_prob                  \
        --pointsource-noise-addition-probability  $pointsource_noise_addition_prob  \
        --isotropic-noise-addition-probability    $isotropic_noise_additon_prob     \
        --rir-smoothing-weight                    $rir_smoothing_weight             \
        --noise-smoothing-weight                  $noise_smoothing_weight           \
        --max-noises-per-minute                   $max_noise_per_minute             \
        --random-seed                             $random_seed                      \
        --shift-output                            true                              \
        --source-sampling-rate                    $source_sampling_rate             \
        --include-original-data                   $include_original_data            \
        $src_dir  $dest_dir
}

export LD_LIBRARY_PATH=./process_env/lib
export PATH=$PATH:./process_env/

hdfs_rir_noise_tar_dir=
output_perturb_path=
output_fbank_log=
output_fbank_dir=
hdfs_dir=

# Begin configuration section.
nj=1
cmd=./process_env/utils/run.pl
fbank_config=./process_env/conf/fbank_40.conf
compress=true

only_add_noise=true
rvb_with_noise=true
use_three_channel=true
use_four_channel=false
snrs="20:19:18:17:16:15:14:13:12:11:10:9:8:7:6:5"
random_seed=777
num_replications=1
foreground_snrs="20:19:18:17:16:15:14:13:12:11:10:9:8:7:6:5"
background_snrs="20:19:18:17:16:15:14:13:12:11:10:9:8:7:6:5"
pointsource_noise_addition_prob=1
isotropic_noise_additon_prob=1
rir_smoothing_weight=0.3
noise_smoothing_weight=0.3
max_noise_per_minute=15
source_sampling_rate=16000
include_original_data=false
spk_info_file=./process_env/data_input/haitian-speaker-info-total.txt
impnoise_dir=./process_env/data_input
noise_file=$impnoise_dir/noise_list
rir_file_1mic=$impnoise_dir/1mic_rir_list
rir_file_3mic=$impnoise_dir/3mic_rir_list
rir_file_4mic=$impnoise_dir/4mic_rir_list

# hisf configurations
hisf_1mic=" ./process_env/bin/hisf_linux_1mic stdin.wav stdout.wav 2 0 0 0 1 300 2 0 a.ini |"
hisf_3mic=" ./process_env/bin/hisf_linux_3mic stdin.wav stdout.wav 2 2 0 0 1 300 2 0 a.ini |"
hisf_4mic=" ./process_env/bin/hisf_linux_4mic stdin.wav stdout.wav 1 2 0 0 2 doa 2 -1 a.ini |" # replace doa with real value later

highpass_cmd="highpass 100"
# End configuration section.

if [ -f path.sh ]; then . ./path.sh; fi
. ./process_env/utils/parse_options.sh || exit 1;
. ./process_env/utils/get_timing.sh || exit 1;


old_rir_dir=`dirname $hdfs_rir_noise_tar_dir`
init_seed=$random_seed
current_dir=$(pwd)
mapper=`printenv mapred_task_id | cut -d "_" -f 5`
random_seed=$(echo "$random_seed + $mapper" | bc)
hdfs_tgt_rvb_kaldi_data_dir=${output_perturb_path}
hdfs_tgt_rvb_fbank_feat_dir=${output_fbank_dir}
hdfs_tgt_rvb_fbank_log_dir=${output_fbank_log}


# TO DO: if you make multiple reverberations, you need to change the prefix to avoid duplications
if [ $only_add_noise = true ]; then
    speech_rvb_prob=0
    prefix=noise
else
    speech_rvb_prob=1
    prefix=rvb
fi

if [ $rvb_with_noise = true ]; then
    pointsource_noise_addition_prob=1
    isotropic_noise_additon_prob=1
else
    pointsource_noise_addition_prob=0
    isotropic_noise_additon_prob=0
fi


start_time=`date +%s.%N`
combined_kaldi_data_dirs=()
kaldi_data_dirs=()
wave_files=()
idx=0
split_idx=0
combined_mapper_id=""
perturb_field=""
while read line
do
    hadoop_key=`echo $line | awk '{print $1}'`
    hdfs_combined_kaldi_data_dir=`echo $line | awk '{print $2}'`
    combined_kaldi_data_dir=`basename $hdfs_combined_kaldi_data_dir`
    perturb_field=`echo $combined_kaldi_data_dir | awk -F '_' '{print $(NF-1)}'`
    combined_mapper_id=`echo $combined_kaldi_data_dir | awk -F '_' '{print $NF}'`
    combined_kaldi_data_dirs[idx]=$combined_kaldi_data_dir
    idx=$((idx + 1))

    hdfs dfs -get $hdfs_combined_kaldi_data_dir
    while read line
    do
        wav_name=`echo $line | cut -d ' ' -f 1`
        echo $wav_name > spk_list
        hdfs_wav_path=`echo $line | cut -d ' ' -f 2`
        hdfs dfs -get $hdfs_wav_path
        ./process_env/utils/subset_data_dir.sh --spk-list spk_list ${combined_kaldi_data_dir} ${wav_name}_kaldi  #make subset kaldi data, one wav per dir
        kaldi_data_dirs[split_idx]=${wav_name}_kaldi
        split_idx=$((split_idx + 1))
    done < ${combined_kaldi_data_dir}/wav.scp
done
end_time=`date +%s.%N`
echo "hdfs get time:"
getTiming $start_time $end_time


if [[ $idx -gt 1 ]]; then
    echo "Should only read one line for each mapper."
    exit 1
fi
perturb_suffix=${perturb_field}_${combined_mapper_id}

if [[ $perturb_field = "origin" ]]; then
    perturb_cmd=""
elif [[ $perturb_field =~ "speed" ]]; then
    value=${perturb_field##speed}
    perturb_cmd="speed $value"
elif [[ $perturb_field =~ "pitch" ]]; then
    value=${perturb_field##pitch}
    perturb_cmd="pitch $value"
else
    echo "Wrong perturb command: $perturb_field"
    exit 1
fi


start_time=`date +%s.%N`
new_3mic_kaldi_data_dirs=()
new_4mic_kaldi_data_dirs=()
new_1mic_kaldi_data_dirs=()
idx=0
new_idx_1mic=0
new_idx_3mic=0
new_idx_4mic=0

for dir in ${kaldi_data_dirs[*]}
do
    wav_name=`cat ${dir}/wav.scp | cut -d ' ' -f 1`
    hdfs_wav_path=`cat ${dir}/wav.scp | cut -d ' ' -f 2`
    wav_path=`basename $hdfs_wav_path`

    # a lot of same code, maybe we can do something
    if [ $only_add_noise = true ]; then
        # Haitian farfield waves have 8 channels
        if [[ $dir =~ "DISTANCE_" ]]; then
            if [ $use_three_channel = true ]; then
                dir_3c=${dir}-3chan
                chan_prefix=3chan_
                mkdir -p ${dir_3c}
                # copy_data_dir.sh has been modified to change the wav.scp and segments at the same time
                ./process_env/utils/copy_data_dir.sh --spk-prefix $chan_prefix --utt-prefix $chan_prefix ${dir} ${dir_3c}
                wav_name=`cat ${dir_3c}/wav.scp | cut -d ' ' -f 1`
                echo "$wav_name sox $wav_path -twav - remix 2 3 1 $highpass_cmd $perturb_cmd |" > ${dir_3c}/wav.scp
                new_3mic_kaldi_data_dirs[new_idx_3mic]=${dir_3c}
                new_idx_3mic=$((new_idx_3mic + 1))
            fi
            if [ $use_four_channel = true ]; then
                dir_4c=${dir}-4chan
                chan_prefix=4chan_
                mkdir -p ${dir_4c}
                ./process_env/utils/copy_data_dir.sh --spk-prefix $chan_prefix --utt-prefix $chan_prefix ${dir} ${dir_4c}
                wav_name=`cat ${dir_4c}/wav.scp | cut -d ' ' -f 1`
                echo "$wav_name sox $wav_path -twav - remix 4 5 6 7 $highpass_cmd  $perturb_cmd |" > ${dir_4c}/wav.scp
                new_4mic_kaldi_data_dirs[new_idx_4mic]=${dir_4c}
                new_idx_4mic=$((new_idx_4mic + 1))
            fi
            if [ ! $use_three_channel = true ] && [ ! $use_four_channel = true ]; then
                dir_1c=${dir}-1chan
                chan_prefix=1chan_
                mkdir -p ${dir_1c}
                ./process_env/utils/copy_data_dir.sh --spk-prefix $chan_prefix --utt-prefix $chan_prefix ${dir} ${dir_1c}
                wav_name=`cat ${dir_1c}/wav.scp | cut -d ' ' -f 1`
                echo "$wav_name sox $wav_path -twav - remix 1 $highpass_cmd  $perturb_cmd |" > ${dir_1c}/wav.scp
                new_1mic_kaldi_data_dirs[new_idx_1mic]=${dir_1c}
                new_idx_1mic=$((new_idx_1mic + 1))
            fi
        else
            # Nearfield to add 1-channel noise
            dir_1c=${dir}-1chan
            chan_prefix=1chan_
            mkdir -p ${dir_1c}
            ./process_env/utils/copy_data_dir.sh --spk-prefix $chan_prefix --utt-prefix $chan_prefix ${dir} ${dir_1c}
            wav_name=`cat ${dir_1c}/wav.scp | cut -d ' ' -f 1`
            echo "$wav_name sox $wav_path -twav - $highpass_cmd $perturb_cmd |" > ${dir_1c}/wav.scp
            new_1mic_kaldi_data_dirs[new_idx_1mic]=${dir_1c}
            new_idx_1mic=$((new_idx_1mic + 1))
        fi
    else
        # Reverberate
        if [[ $dir =~ "DISTANCE_" ]]; then
            # no operation here for farfield data
            continue
        else
            # Reverberate nearield data with different channels
            echo "$wav_name sox $wav_path -twav - $highpass_cmd $perturb_cmd |" > ${dir}/wav.scp

            if [ $use_three_channel = true ]; then
                dir_3c=${dir}-3chan
                chan_prefix=3chan_
                mkdir -p ${dir_3c}
                ./process_env/utils/copy_data_dir.sh --spk-prefix $chan_prefix --utt-prefix $chan_prefix ${dir} ${dir_3c}
                new_3mic_kaldi_data_dirs[new_idx_3mic]=${dir_3c}
                new_idx_3mic=$((new_idx_3mic + 1))
            fi
            if [ $use_four_channel = true ]; then
                dir_4c=${dir}-4chan
                chan_prefix=4chan_
                mkdir -p ${dir_4c}
                ./process_env/utils/copy_data_dir.sh --spk-prefix $chan_prefix --utt-prefix $chan_prefix ${dir} ${dir_4c}
                new_4mic_kaldi_data_dirs[new_idx_4mic]=${dir_4c}
                new_idx_4mic=$((new_idx_4mic + 1))
            fi
            if [ ! $use_three_channel = true ] && [ ! $use_four_channel = true]; then
                dir_1c=${dir}-1chan
                chan_prefix=1chan_
                mkdir -p ${dir_1c}
                ./process_env/utils/copy_data_dir.sh --spk-prefix $chan_prefix --utt-prefix $chan_prefix ${dir} ${dir_1c}
                new_1mic_kaldi_data_dirs[new_idx_1mic]=${dir_1c}
                new_idx_1mic=$((new_idx_1mic + 1))
            fi
        fi
    fi
    idx=$((idx + 1))
done

end_time=`date +%s.%N`
echo "Creating new kaldi data dirs time:"
getTiming $start_time $end_time


start_time=`date +%s.%N`
combine_dirs=()
idx=0
if [ -z "$new_1mic_kaldi_data_dirs" ] && [ -z "$new_3mic_kaldi_data_dirs" ] && [ -z "$new_4mic_kaldi_data_dirs" ]; then
    echo "No operation is needed, please check inputs and arguments!"
    exit 0;
fi
if [ ! -z "$new_1mic_kaldi_data_dirs" ]; then
    ./process_env/utils/combine_data.sh combined_1mic_spk_dir ${new_1mic_kaldi_data_dirs[*]}
    Reverberate combined_1mic_spk_dir perturb_1mic_combine_dir ${prefix}${init_seed}- $speech_rvb_prob $rir_file_1mic
    combine_dirs[idx]=perturb_1mic_combine_dir
    idx=$((idx + 1))
fi
if [ ! -z "$new_3mic_kaldi_data_dirs" ]; then
    ./process_env/utils/combine_data.sh combined_3mic_spk_dir ${new_3mic_kaldi_data_dirs[*]}
    Reverberate combined_3mic_spk_dir perturb_3mic_combine_dir ${prefix}${init_seed}- $speech_rvb_prob $rir_file_3mic
    combine_dirs[idx]=perturb_3mic_combine_dir
    idx=$((idx + 1))
fi
if [ ! -z "$new_4mic_kaldi_data_dirs" ]; then
    ./process_env/utils/combine_data.sh combined_4mic_spk_dir ${new_4mic_kaldi_data_dirs[*]}
    Reverberate combined_4mic_spk_dir perturb_4mic_combine_dir ${prefix}${init_seed}- $speech_rvb_prob $rir_file_4mic
    combine_dirs[idx]=perturb_4mic_combine_dir
    idx=$((idx + 1))
fi
if [ ! -z "$combine_dirs" ]; then
    ./process_env/utils/combine_data.sh combined_total_perturbed_spk_dir ${combine_dirs[*]}
else
    echo "Empty combine perturb data"
    exit 0
fi
end_time=`date +%s.%N`
echo "Combine and reverberate kaldi data time:"
getTiming $start_time $end_time


start_time=`date +%s.%N`
perturbed_kaldi_data_dirs=()
idx=0
perturb_idx=0
for i in ${new_1mic_kaldi_data_dirs[*]} ${new_3mic_kaldi_data_dirs[*]} ${new_4mic_kaldi_data_dirs[*]}
do
    spk_id=`echo $i | awk -F '_' '{print $(NF-2)}'`
    channel=`echo $i | awk -F '_' '{print $NF}'`
    perturb_kaldi_data_dir=${i}_${prefix}${init_seed}
    awk '{print $1}' $i/wav.scp | sed "s/^/${prefix}${init_seed}-1_/" > wav.scp_change_key
    ./process_env/utils/subset_data_dir.sh --spk-list wav.scp_change_key combined_total_perturbed_spk_dir $perturb_kaldi_data_dir
    sed -i "s|$old_rir_dir|./perturb_data/|g" $perturb_kaldi_data_dir/wav.scp
    rm wav.scp_change_key
    if [ $only_add_noise = true ]; then
        if [[ $dir =~ "DISTANCE_" ]]; then
            if [ $use_three_channel = true ] && [ $channel = "kaldi-3chan" ]; then
                sed -i "s| "./process_env/bin/wav-reverberate" | ./process_env/bin/wav-reverberate --multi-channel-output=true |g" ${perturb_kaldi_data_dir}/wav.scp
                cat ${perturb_kaldi_data_dir}/wav.scp | perl -pe 'chop if eof' > wav.scp.tmp    #this cmd will remove \r and \n from per line. and yes, there is only one line
                echo $hisf_3mic >> wav.scp.tmp
                cat wav.scp.tmp > ${perturb_kaldi_data_dir}/wav.scp
                perturbed_kaldi_data_dirs[perturb_idx]=${perturb_kaldi_data_dir}
                perturb_idx=$((perturb_idx + 1))
            fi
            if [ $use_four_channel = true ] && [ $channel = "kaldi-4chan" ]; then
                sed -i "s| "./process_env/bin/wav-reverberate" | ./process_env/bin/wav-reverberate --multi-channel-output=true |g" ${perturb_kaldi_data_dir}/wav.scp
                cat ${perturb_kaldi_data_dir}/wav.scp | perl -pe 'chop if eof' > wav.scp.tmp
                rir_name=$( cat wav.scp.tmp | sed 's:.*./process_env/bin/wav-reverberate --multi-channel-output=true --shift-output=true --impulse-response="\([^"]\+\)".*:\1:' )
                rir_file=`echo $rir_name | cut -d' ' -f 2`
                doa=`./process_env/steps/get_spk_info.py $spk_id $spk_info_file`
                if [ $doa = "-1" ]; then
                    doa=`basename $rir_file | sed 's/.*rir_\([0-9]\+\)_[0-9]\+.wav/\1/'`
                fi
                echo $hisf_4mic | sed "s/ doa / $doa /" >> wav.scp.tmp
                cat wav.scp.tmp > ${perturb_kaldi_data_dir}/wav.scp
                perturbed_kaldi_data_dirs[perturb_idx]=${perturb_kaldi_data_dir}
                perturb_idx=$((perturb_idx + 1))
            fi
            if [ $channel = "kaldi-1chan" ]; then
                cat ${perturb_kaldi_data_dir}/wav.scp | perl -pe 'chop if eof' > wav.scp.tmp
                echo $hisf_1mic >> wav.scp.tmp
                cat wav.scp.tmp > ${perturb_kaldi_data_dir}/wav.scp
                perturbed_kaldi_data_dirs[perturb_idx]=${perturb_kaldi_data_dir}
                perturb_idx=$((perturb_idx + 1))
            fi
        else
            cat ${perturb_kaldi_data_dir}/wav.scp | perl -pe 'chop if eof' > wav.scp.tmp
            echo $hisf_1mic >> wav.scp.tmp
            cat wav.scp.tmp > ${perturb_kaldi_data_dir}/wav.scp
            perturbed_kaldi_data_dirs[perturb_idx]=${perturb_kaldi_data_dir}
            perturb_idx=$((perturb_idx + 1))

        fi
    else
        # Reverbrate nearfield
        if [ $use_three_channel = true ] && [ $channel = "kaldi-3chan" ]; then
            sed -i "s| "./process_env/bin/wav-reverberate" | ./process_env/bin/wav-reverberate --multi-channel-output=true |g" ${perturb_kaldi_data_dir}/wav.scp
            cat $perturb_kaldi_data_dir/wav.scp | perl -pe 'chop if eof' > wav.scp.tmp
            echo $hisf_3mic >> wav.scp.tmp
            cat wav.scp.tmp > ${perturb_kaldi_data_dir}/wav.scp
            perturbed_kaldi_data_dirs[perturb_idx]=${perturb_kaldi_data_dir}
            perturb_idx=$((perturb_idx + 1))
        fi
        if [ $use_four_channel = true ] && [ $channel = "kaldi-4chan" ]; then
            sed -i "s| "./process_env/bin/wav-reverberate" | ./process_env/bin/wav-reverberate --multi-channel-output=true |g" ${perturb_kaldi_data_dir}/wav.scp
            rir_name=$( cat wav.scp.tmp | sed 's:.*./process_env/bin/wav-reverberate --multi-channel-output=true --shift-output=true --impulse-response="\([^"]\+\)".*:\1:' )
            rir_file=`echo $rir_name | cut -d' ' -f 2`
            doa=`./process_env/steps/get_spk_info.py $spk_id $spk_info_file`
            if [ $doa = "-1" ]; then
                doa=`basename $rir_file | sed 's/.*rir_\([0-9]\+\)_[0-9]\+.wav/\1/'`
            fi
            cat $perturb_kaldi_data_dir/wav.scp | perl -pe 'chop if eof' > wav.scp.tmp
            echo $hisf_4mic | sed "s/ doa / $doa /" >> wav.scp.tmp
            cat wav.scp.tmp > ${perturb_kaldi_data_dir}/wav.scp
            perturbed_kaldi_data_dirs[perturb_idx]=${perturb_kaldi_data_dir}
            perturb_idx=$((perturb_idx + 1))
        fi
        if [ $channel = "kaldi-1chan" ]; then
            cat $perturb_kaldi_data_dir/wav.scp | perl -pe 'chop if eof' > wav.scp.tmp
            echo $hisf_1mic >> wav.scp.tmp
            cat wav.scp.tmp > ${perturb_kaldi_data_dir}/wav.scp
            perturbed_kaldi_data_dirs[perturb_idx]=${perturb_kaldi_data_dir}
            perturb_idx=$((perturb_idx + 1))
        fi
    fi
    idx=$((idx + 1))
done

end_time=`date +%s.%N`
echo "Subset combined kaldi data add hisf time:"
getTiming $start_time $end_time


start_time=`date +%s.%N`
suffix=$prefix${init_seed}_$perturb_suffix
total_perturbed_dir=combined_perturbed_kaldi_data_$suffix
# Combining the all perturbed kaldi data within this mapper
./process_env/utils/combine_data.sh $total_perturbed_dir ${perturbed_kaldi_data_dirs[*]}
end_time=`date +%s.%N`
echo "Combining kaldi data time:"
getTiming $start_time $end_time


start_time=`date +%s.%N`
awk -vdir=${total_perturbed_dir} '{
    if ($NF == "|") {
        cmd=$2; for (i=3; i < NF; ++i) cmd = cmd " " $i;
        cmd = cmd " > " dir "/" $1 ".wav";
    } else {
        cmd = "cp " $2 " " dir "/" $1 ".wav";
    }
    system(cmd);
    new_file = dir "/" $1 ".wav";
    print $1,  new_file;
}' ${total_perturbed_dir}/wav.scp > ${total_perturbed_dir}/wav.scp_temp 2>awk_err_$suffix.log
end_time=`date +%s.%N`
echo "Generating reverberated waves time:"
getTiming $start_time $end_time

start_time=`date +%s.%N`
mv ${total_perturbed_dir}/wav.scp_temp ${total_perturbed_dir}/wav.scp
# Extracting the fbank features from perturbed combined data
./process_env/steps/make_fbank.sh \
    --nj            $nj           \
    --cmd           $cmd          \
    --fbank-config  $fbank_config \
    --compress      $compress     \
    ${total_perturbed_dir}  combined_perturbed_fbank_log_$suffix  combined_perturbed_fbank_$suffix
end_time=`date +%s.%N`
echo "Exrtacting Fbank time:"
getTiming $start_time $end_time

sed -i "s|$current_dir|$hdfs_tgt_rvb_fbank_feat_dir|g" ${total_perturbed_dir}/feats.scp

start_time=`date +%s.%N`
find combined_perturbed_kaldi_data_$suffix -name "*.wav" -exec rm {} \;
hdfs dfs -put -f combined_perturbed_kaldi_data_$suffix $hdfs_tgt_rvb_kaldi_data_dir
hdfs dfs -put -f combined_perturbed_fbank_$suffix $hdfs_tgt_rvb_fbank_feat_dir
hdfs dfs -put -f combined_perturbed_fbank_log_$suffix $hdfs_tgt_rvb_fbank_log_dir
hdfs dfs -put -f awk_err_$suffix.log $hdfs_tgt_rvb_fbank_log_dir
end_time=`date +%s.%N`
echo "hdfs put time:"
getTiming $start_time $end_time


echo "done!"
