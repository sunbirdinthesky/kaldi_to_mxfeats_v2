#!/bin/bash
# Copyright 2012-2015 Brno University of Technology (author: Karel Vesely)
# Apache 2.0

# Aligns 'data' to sequences of transition-ids using Neural Network based acoustic model.
# Optionally produces alignment in lattice format, this is handy to get word alignment.

# Begin configuration section.
nj=1
cmd=./process_env/utils/run.pl
stage=0
# Begin configuration.

# scale_opts
self_loop_scale=0.1
acoustic_scale=0.1
transition_scale=1.0
beam=10
retry_beam=40
# scale_opts="--transition-scale=1.0 --acoustic-scale=0.1 --self-loop-scale=0.1"

# nnet_forward_opts
no_softmax=false
prior_scale=1.0

# input speech feature shape
width=
height=
channels=

# nnet_forward_opts="--no-softmax=true --prior-scale=1.0"
ivector=            # rx-specifier with i-vectors (ark-with-vectors),

align_to_lats=false # optionally produce alignment in lattice format
# lat_decode_opts
# lat_decode_opts="--acoustic-scale=0.1 --beam=20 --lattice_beam=10"
lat_decode_acoustic_scale=0.1
lat_decode_beam=20
lattice_beam=10
# lat_graph_scale="--transition-scale=1.0 --self-loop-scale=0.1"
lat_graph_transition_scale=1.0
lat_graph_self_loop_scale=0.1
graph_batch_size=200
use_gpu="no" # yes|no|optionaly
# End configuration options.

[ $# -gt 0 ] && echo "$0 $@"  # Print the command line for logging

[ -f path.sh ] && . ./path.sh # source the path.
. ./process_env/utils/parse_options.sh || exit 1;

set -euo pipefail

if [ $# != 7 ]; then
   echo "usage: $0 <data-dir> <lang-dir> <src-dir> <nnet_json> <nnet_params> <feature_transform> <align-dir>"
   echo "e.g.:  $0 data/train data/lang exp/tri1 exp/tri1_ali"
   echo "main options (for others, see top of script file)"
   echo "  --config <config-file>                           # config containing options"
   echo "  --nj <nj>                                        # number of parallel jobs"
   echo "  --cmd (utils/run.pl|utils/queue.pl <queue opts>) # how to run jobs."
   exit 1;
fi

data=$1
lang=$2
srcdir=$3
nnet_json=$4
nnet_params=$5
feature_transform=$6
dir=$7

mkdir -p $dir/log
echo $nj > $dir/num_jobs
sdata=$data/split$nj
[[ -d $sdata && $data/feats.scp -ot $sdata ]] || ./process_env/utils/split_data.sh $data $nj || exit 1;

cp $srcdir/{tree,final.mdl} $dir || exit 1;

# Select default locations to model files
nnet=$srcdir/final.nnet;
class_frame_counts=$srcdir/ali_train_pdf.counts
# feature_transform=$srcdir/final.feature_transform
model=$dir/final.mdl

# Check that files exist
for f in $sdata/1/feats.scp $sdata/1/text $lang/L.fst $nnet $model $feature_transform $class_frame_counts; do
  [ ! -f $f ] && echo "$0: missing file $f" && exit 1;
done


# PREPARE FEATURE EXTRACTION PIPELINE
# import config,
cmvn_opts=
delta_opts=
D=$srcdir
[ -e $D/norm_vars ] && cmvn_opts="--norm-means=true --norm-vars=$(cat $D/norm_vars)" # Bwd-compatibility,
[ -e $D/cmvn_opts ] && cmvn_opts=$(cat $D/cmvn_opts)
[ -e $D/delta_order ] && delta_opts="--delta-order=$(cat $D/delta_order)" # Bwd-compatibility,
[ -e $D/delta_opts ] && delta_opts=$(cat $D/delta_opts)
#
# Create the feature stream,
feats="ark,s,cs:./process_env/bin/copy-feats scp:$sdata/JOB/feats.scp ark:- |"
# apply-cmvn (optional),
[ ! -z "$cmvn_opts" -a ! -f $sdata/1/cmvn.scp ] && echo "$0: Missing $sdata/1/cmvn.scp" && exit 1
[ ! -z "$cmvn_opts" ] && feats="$feats ./process_env/bin/apply-cmvn $cmvn_opts --utt2spk=ark:$sdata/JOB/utt2spk scp:$sdata/JOB/cmvn.scp ark:- ark:- |"
# add-deltas (optional),
[ ! -z "$delta_opts" ] && feats="$feats ./process_env/bin/add-deltas $delta_opts ark:- ark:- |"
# add-pytel transform (optional),
[ -e $D/pytel_transform.py ] && feats="$feats /bin/env python $D/pytel_transform.py |"

# add-ivector (optional),
if [ -e $D/ivector_dim ]; then
  ivector_dim=$(cat $D/ivector_dim)
  [ -z $ivector ] && echo "Missing --ivector, they were used in training! (dim $ivector_dim)" && exit 1
  ivector_dim2=$(./process_env/bin/copy-vector --print-args=false "$ivector" ark,t:- | head -n1 | awk '{ print NF-3 }') || true
  [ $ivector_dim != $ivector_dim2 ] && "Error, i-vector dimensionality mismatch! (expected $ivector_dim, got $ivector_dim2 in $ivector)" && exit 1
  # Append to feats
  feats="$feats append-vector-to-feats ark:- '$ivector' ark:- |"
fi

# nnet-forward-mxnet,
feats="$feats ./process_env/bin/nnet-forward-mxnet --no-softmax=$no_softmax --apply-log=true --prior-scale=$prior_scale --feature-transform=$feature_transform --class-frame-counts=$class_frame_counts --use-gpu=$use_gpu --width=$width --height=$height --channels=$channels $nnet_json $nnet_params ark:- ark:- |"
#

echo "$0: aligning data '$data' using nnet/model '$srcdir', putting alignments in '$dir'"

# Map oovs in reference transcription,
oov=`cat $lang/oov.int` || exit 1;
tra="ark:./process_env/utils/sym2int.pl --map-oov $oov -f 2- $lang/words.txt $sdata/JOB/text|";
# We could just use align-mapped in the next line, but it's less efficient as it compiles the
# training graphs one by one.
if [ $stage -le 0 ]; then
  train_graphs="ark:./bin/compile-train-graphs $dir/tree $dir/final.mdl $lang/L.fst '$tra' ark:- |"
  $cmd JOB=1:$nj $dir/log/align.JOB.log \
    ./process_env/bin/compile-train-graphs --batch-size=$graph_batch_size $dir/tree $dir/final.mdl $lang/L.fst "$tra" ark:- \| \
    ./process_env/bin/align-compiled-mapped --transition-scale=$transition_scale --acoustic-scale=$acoustic_scale --self-loop-scale=$self_loop_scale \
      --beam=$beam --retry-beam=$retry_beam $dir/final.mdl ark:- \
      "$feats" "ark,t:|gzip -c >$dir/ali.JOB.gz" || exit 1;
fi

# Optionally align to lattice format (handy to get word alignment)
if [ "$align_to_lats" == "true" ]; then
  echo "$0: aligning also to lattices '$dir/lat.*.gz'"
  $cmd JOB=1:$nj $dir/log/align_lat.JOB.log \
    ./process_env/bin/compile-train-graphs --transition-scale=$lat_graph_transition_scale --self-loop-scale=$lat_graph_self_loop_scale \
      $dir/tree $dir/final.mdl  $lang/L.fst "$tra" ark:- \| \
    ./process_env/bin/latgen-faster-mapped --acoustic-scale=$lat_decode_acoustic_scale --beam=$lat_decode_beam --lattice_beam=$lattice_beam \
      --word-symbol-table=$lang/words.txt $dir/final.mdl ark:- \
      "$feats" "ark:|gzip -c >$dir/lat.JOB.gz" || exit 1;
fi

echo "$0: done aligning data."
