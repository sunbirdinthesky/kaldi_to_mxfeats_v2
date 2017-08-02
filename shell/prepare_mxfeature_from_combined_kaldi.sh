#!/bin/bash -x

export LD_LIBRARY_PATH=./process_env/lib/
export PATH=$PATH:./process_env/
chmod +x ./process_env/bin/* ./process_env/utils/* ./process_env/steps/* ./process_env/data_input/* ./process_env/steps/mxnet/* ./process_env/prepare_mxfeature_from_combined_kaldi.sh

sentences_per_block=5000
hdfs_mxfeature_dir=
hdfs_cmvn_dir=
hdfs_prior_dir=
ali_log_dir=
feat_out_prefix=
need_convert_alignment=false
mdl_2831=./resource_for_alignment/relu_dnn_reverb_haitian_and_accent_with_cmd_more_noise/final.mdl
mdl_6037=./resource_for_alignment/6037_mdl/final.mdl
tree_6037=./resource_for_alignment/6037_mdl/tree

if [ -f path.sh ]; then . ./path.sh; fi
. ./process_env/utils/parse_options.sh || exit 1;
. ./process_env/utils/get_timing.sh || exit 1;

# Get unique mapper id
mapper=`printenv mapred_task_id | cut -d "_" -f 5`
# Create log dir
cur_ali_log_dir=$ali_log_dir/${mapper}
hdfs dfs -rm -r $cur_ali_log_dir
hdfs dfs -mkdir -p $cur_ali_log_dir


alignment_dirs=()
fbank_scp_files=()
fbank_ark_files=()
start_time=`date +%s.%N`
idx=0
while read line
do
    hadoop_key=`echo $line | awk '{print $1}'`
    hdfs_fbank_scp_file=`echo $line | awk '{print $2}'`
    hdfs_ali_gz_file=`echo $line | awk '{print $3}'`

    # Get ali.1.gz
    ali_dir=`echo $hdfs_ali_gz_file | awk -F '/' '{info_field=NF-1; print $info_field}'`
    if [ -f $ali_dir ]; then
        echo "$ali_dir already exists"
    else
        mkdir $ali_dir
        hdfs dfs -get $hdfs_ali_gz_file $ali_dir
    fi
    ali_file=`basename $hdfs_ali_gz_file`
    alignment_dirs[idx]=$ali_dir/$ali_file

    # Get feats.scp
    scp_dir=`echo $hdfs_fbank_scp_file | awk -F '/' '{info_field=NF-1; print $info_field}'`
    mkdir $scp_dir
    hdfs dfs -get $hdfs_fbank_scp_file $scp_dir
    fbank_scp_files[idx]=$scp_dir/feats.scp

    # Get fbank ark
    old_hdfs_fbank_ark_file=`head -1 $scp_dir/feats.scp | cut -d ' ' -f 2 | cut -d ':' -f 1-3`
    hdfs_fbank_ark_file=$old_hdfs_fbank_ark_file
    ark_name=`basename $hdfs_fbank_ark_file`
    fbank_ark_files[idx]=$ark_name
    if [ -f $ark_name ]; then
        echo "$ark_name already exists."
    else
        hdfs dfs -get $hdfs_fbank_ark_file
    fi

    idx=$((idx + 1))
done
end_time=`date +%s.%N`
echo "hdfs get time:"
getTiming $start_time $end_time


idx=0
for scp_file in ${fbank_scp_files[*]}
do
    scp_key=`head -1 $scp_file | cut -d ' ' -f 1`
    scp_ark_path=`head -1 $scp_file | cut -d' ' -f 2`
    ark_old_dir=`dirname $scp_ark_path`
    sed -i "s|${ark_old_dir}/||g" $scp_file

    gunzip -c ${alignment_dirs[idx]} > tmp.ali

    # Map the pitch feats.scp with origin ali file
    if [[ $scp_key =~ "pitch50" ]]; then
        sed -i "s/origin/pitch50/g" tmp.ali
    elif [[ $scp_key =~ "pitch100" ]]; then
        sed -i "s/origin/pitch100/g" tmp.ali
    fi

    noise_key_check=`echo $scp_key | grep "noise[0-9]*"`
    rvb_key_check=`echo $scp_key | grep "rvb[0-9]*"`

    # Add prefix for noise and rvb and combined all ali files within this mapper
    if [[ $noise_key_check -eq 0 && rvb_key_check -eq 0 ]]; then
        cat tmp.ali >> total.ali
    else
        prefix=`echo $scp_key | cut -d '_' -f 1-2`
        sed "s/^/"${prefix}_"/g" tmp.ali >> total.ali
    fi

    # Combine all feats.scp within this mapper
    cat $scp_file >> total.scp
    idx=$((idx + 1))
done

num_utt_feats=`wc -l total.scp | cut -d' ' -f 1`
num_utt_ali=`wc -l total.ali | cut -d' ' -f 1`
echo "Total num of utterances from original feats: $num_utt_feats" >> utt_stats.log
echo "Total num of utterances from original ali: $num_utt_ali" >> utt_stats.log

if [[ $num_utt_feats -eq 0 || $num_utt_ali -eq 0 ]]; then
    # Use err to display the information on web logs
    err="Error input: num_utt_feats is ${num_utt_feats} and num_utt_ali is ${num_utt_ali}"
    echo $err >> utt_stats.log
    hdfs dfs -put utt_stats.log $cur_ali_log_dir
    exit 1
fi

sort -u -k1 total.ali > sorted_ali
sort -u -k1 total.scp > sorted_scp

num_utt_feats=`wc -l sorted_scp | cut -d' ' -f 1`
num_utt_ali=`wc -l sorted_ali | cut -d' ' -f 1`
echo "Total num of utterances from sorted and unique feats: $num_utt_feats" >> utt_stats.log
echo "Total num of utterances from sorted and unique ali: $num_utt_ali" >> utt_stats.log


# Map feats with ali
awk 'NR==FNR {
    content = $2
    for(i=3;i<=NF;++i)
        content = content " " $i
    map[$1] = content; next}
    {
    print $1, $2, map[$1]
}' sorted_ali  sorted_scp > combined.txt
num_utt=`wc -l combined.txt  | cut -d' ' -f 1`
echo "Total num of mapped utterances from feat and ali: $num_utt" >> utt_stats.log


# Remove utterances with empty ali
awk '{
    if ( $3 > 0 )
        print $0
}' combined.txt > removed_empty_ali.txt
awk '{
    print $1, $2
}' removed_empty_ali.txt > total_mapped.scp
awk '{
    content = $1
    for (i=3 ;i<=NF;i++)
        content = content " " $i
    print content
}' removed_empty_ali.txt > total_mapped.ali
num_utt=`wc -l removed_empty_ali.txt | cut -d' ' -f 1`
echo "Total num of mapped utterances after removing empty ali: $num_utt" >> utt_stats.log


# Shuffle on utterance-level
shuf total_mapped.scp > shuf.scp
awk 'BEGIN{i=0;} {printf "%06d ", i ; i++;} $0' shuf.scp > tmp.scp
cut  -d ' ' -f 1,3 tmp.scp > total_shuffled.scp
awk 'BEGIN {while (getline < "tmp.scp") {map[$2] = $1}}
{
    key = map[$1];
    str = key;
    for(i=2;i<=NF;++i)
        str = str " " $i;
    print str;
}' total_mapped.ali | sort -k1 > total_shuffled.ali
num_utt_feats=`wc -l total_shuffled.scp | cut -d' ' -f 1`
num_utt_ali=`wc -l total_shuffled.ali | cut -d' ' -f 1`
echo "Total num of utterances from the final shuffled feats: $num_utt_feats" >> utt_stats.log
echo "Total num of utterances from the final shuffled ali: $num_utt_ali" >> utt_stats.log


mdl=${mdl_2831}
cat total_shuffled.ali > total_shuffled_2831.ali
# Convert the old alignments to new alignments according to the new model and tree
if [[ $need_convert_alignment = true ]]; then
    new_mdl=${mdl_6037}
    new_tree=${tree_6037}
    ./process_env/bin/convert-ali $mdl $new_mdl $new_tree ark:total_shuffled_2831.ali ark:total_shuffled_6037.ali
    cat total_shuffled_6037.ali > total_shuffled.ali
    mdl=$new_mdl
fi

# Convert trans-id to pdf
./process_env/bin/ali-to-pdf $mdl ark,o:total_shuffled.ali ark,t:total_shuffled.ali_pdf
tar -czf ali_log.tgz total.ali total.scp combined.txt total_mapped.ali total_mapped.scp total_shuffled.ali total_shuffled.scp total_shuffled.ali_pdf
hdfs dfs -put -f ali_log.tgz $cur_ali_log_dir
hdfs dfs -put -f utt_stats.log $cur_ali_log_dir

# Calculate the pdf-prior information each mapper
./process_env/bin/analyze-counts ark:total_shuffled.ali_pdf pdf_prior_counts_${mapper}
hdfs dfs -put -f pdf_prior_counts_${mapper} ${hdfs_prior_dir} || exit 1

# Convert kaldi feats and ali to mxnet feature
./process_env/bin/hr-combine-ali-feats ark,o:total_shuffled.ali_pdf scp,o:total_shuffled.scp ark,t:- ark,t:- 2>stderr.log | \
./process_env/steps/prepare_mxfeature_from_kaldi_key.py - $sentences_per_block ${feat_out_prefix}_${mapper}_
hdfs dfs -put -f stderr.log $cur_ali_log_dir
hdfs dfs -put -f ${feat_out_prefix}_${mapper}_* ${hdfs_mxfeature_dir} || exit 1

# Compute CMVN stats each mapper
./process_env/bin/compute-cmvn-stats --binary=false "ark:./process_env/bin/add-deltas scp:total_shuffled.scp ark:- |" delta2_cmvn_${mapper}
hdfs dfs -put -f delta2_cmvn_${mapper} ${hdfs_cmvn_dir} || exit 1

echo "done!"
