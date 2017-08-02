#!/bin/bash -x
echo lalalalalla
Reverberate() {
    ./process_env/steps/data/reverberate_data_dir_aec.py --rir-set-parameters $5 \
        --noise-set-parameters $6 \
        --num-replications $num_replications \
        --foreground-snrs $foreground_snrs \
        --background-snrs $background_snrs \
        --prefix $3 \
        --speech-rvb-probability $4 \
        --pointsource-noise-addition-probability $pointsource_noise_addition_prob \
        --isotropic-noise-addition-probability $isotropic_noise_additon_prob \
        --rir-smoothing-weight $rir_smoothing_weight \
        --noise-smoothing-weight $noise_smoothing_weight \
        --max-noises-per-minute $max_noise_per_minute \
        --random-seed $random_seed \
        --shift-output true \
        --source-sampling-rate $source_sampling_rate \
        --include-original-data $include_original_data \
        $1 $2
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
fbank_config=./process_env/conf/fbank.conf
compress=true

use_three_channel=true
snrs="0"
random_seed=777
num_replications=1
foreground_snrs=$snrs
background_snrs=$snrs
pointsource_noise_addition_prob=1
isotropic_noise_additon_prob=1
rir_smoothing_weight=0.3
noise_smoothing_weight=0.3
max_noise_per_minute=15
source_sampling_rate=16000
include_original_data=false
spk_info_file=./process_env/data_input/haitian-speaker-info-total.txt
impnoise_dir=./process_env/data_input
noise_file=$impnoise_dir/music_list
rir_file_3mic=$impnoise_dir/3mic_rir_list

# hisf configurations
hisf_aec_4mic=" ./hisf_linux_aec_4mic stdin.wav stdout.wav 0 0 0 hisf_config.ini |"
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


prefix=music${snrs}db
speech_rvb_prob=1

start_time=`date +%s.%N`
combined_kaldi_data_dirs=()
kaldi_data_dirs=()
wave_files=()
idx=0
split_idx=0
combined_mapper_id=""
perturb_field=""
wav_dur=300
while read line
do
    hadoop_key=`echo $line | awk '{print $1}'`
    hdfs_combined_kaldi_data_dir=`echo $line | awk '{print $2}'`
    combined_kaldi_data_dir=`basename $hdfs_combined_kaldi_data_dir`
    combined_mapper_id=`echo $combined_kaldi_data_dir | awk -F '_' '{print $NF}'`
    perturb_field=`echo $combined_kaldi_data_dir | awk -F '_' '{print $(NF-1)}'`
    combined_kaldi_data_dirs[idx]=$combined_kaldi_data_dir
    hdfs dfs -get $hdfs_combined_kaldi_data_dir
    while read line
    do
        wav_name=`echo $line | cut -d ' ' -f 1`
        echo $wav_name > spk_list
        hdfs_wav_path=`echo $line | cut -d ' ' -f 2`
        hdfs dfs -get $hdfs_wav_path
        local_wav_path=`basename $hdfs_wav_path`
        if [ -e $local_wav_path ]; then
            echo "$wav_name $local_wav_path" > tmp_wav.scp
            ./process_env/bin/wav-to-duration scp:tmp_wav.scp ark,t:tmp_wav.duration
            wav_dur=`cat tmp_wav.duration | cut -d ' ' -f 2`
            ./process_env/utils/subset_data_dir.sh --spk-list spk_list ${combined_kaldi_data_dir} ${wav_name}_kaldi
            # ./process_env/utils/validate_data_dir.sh --no-feats ${wav_name}_kaldi || exit 1
            kaldi_data_dirs[split_idx]=${wav_name}_kaldi
            split_idx=$((split_idx + 1))
        else
            echo "$hdfs_wav_path not exits!"
        fi
    done < ${combined_kaldi_data_dir}/wav.scp
    idx=$((idx + 1))
done
end_time=`date +%s.%N`
echo "hdfs get time:"
getTiming $start_time $end_time


if [[ $idx -gt 1 ]]; then
    echo "Should only read one line for each mapper."
    exit 1
fi

# Pick up one music from 5000 music audios using the given random seed
echo "import random;
random.seed($random_seed);
for i in range($split_idx):
    print random.randint(1,5000);
" > gen_random_int_${random_seed}.py
random_ints=`python gen_random_int_${random_seed}.py`
rm gen_random_int_${random_seed}.py

perturb_suffix=${perturb_field}_${combined_mapper_id}

if [[ $perturb_field = "origin" ]]; then
    perturb_cmd=""
    new_wav_dur=$wav_dur
elif [[ $perturb_field =~ "speed" ]]; then
    value=${perturb_field##speed}
    perturb_cmd="speed $value"
    ###adjust the wav_dur
    new_wav_dur=$(echo "$wav_dur / $value" | bc)
    ###
elif [[ $perturb_field =~ "pitch" ]]; then
    value=${perturb_field##pitch}
    perturb_cmd="pitch $value"
    new_wav_dur=$wav_dur
else
    echo "Wrong perturb command: $perturb_field"
    exit 1
fi


cp ./process_env/hisf_linux_aec_4mic .
cp ./process_env/hisf_config.ini .
cp ./process_env/hisf.so .
chmod 755 hisf_linux_aec_4mic


start_time=`date +%s.%N`
idx=0
perturbed_kaldi_data_dirs=()
for dir in ${kaldi_data_dirs[*]}
do
    wav_name=`cat ${dir}/wav.scp | cut -d ' ' -f 1`
    hdfs_wav_path=`cat ${dir}/wav.scp | cut -d ' ' -f 2`
    wav_path=`basename $hdfs_wav_path`

    # Haitian farfield waves have 8 channels
    if [[ $dir =~ "DISTANCE_" ]]; then
        wav_name=`cat ${dir}/wav.scp | cut -d ' ' -f 1`
        echo "$wav_name sox $wav_path -twav - remix 2 3 1 $highpass_cmd $perturb_cmd |" > ${dir}/wav.scp
    else
        # Nearfield
        wav_name=`cat ${dir}/wav.scp | cut -d ' ' -f 1`
        echo "$wav_name sox $wav_path -twav - $highpass_cmd $perturb_cmd |" > ${dir}/wav.scp
    fi

    random_int=`echo $random_ints | awk -v idx=$idx '{print $(idx+1)}'`
    one_music=`sed -n "${random_int}p" $noise_file`
    echo $one_music > music_list


    perturbed_kaldi_data_dir=${dir}_${prefix}${init_seed}
    perturbed_kaldi_data_dirs[idx]=perturbed_kaldi_data_dir

    Reverberate $dir $perturbed_kaldi_data_dir ${prefix}${init_seed}- $speech_rvb_prob $rir_file_3mic music_list
    sed -i "s|$old_rir_dir|./perturb_data/|g" $perturbed_kaldi_data_dir/wav.scp
    music_name=`echo $one_music | awk '{print $NF}'`

    cat ${perturbed_kaldi_data_dir}/wav.scp | perl -pe 'chop if eof' > wav.scp.tmp
    echo "sox -M - $music_name  -twav - trim 0 $new_wav_dur | $hisf_aec_4mic" >> wav.scp.tmp
    cat wav.scp.tmp > ${perturbed_kaldi_data_dir}/wav.scp

    perturbed_kaldi_data_dirs[idx]=${perturbed_kaldi_data_dir}
    idx=$((idx + 1))
done

end_time=`date +%s.%N`
echo "Generating AEC reverberated wav.scp time:"
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
./process_env/steps/make_fbank.sh --nj $nj --cmd $cmd --fbank-config $fbank_config --compress $compress ${total_perturbed_dir} combined_perturbed_fbank_log_$suffix combined_perturbed_fbank_$suffix
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
