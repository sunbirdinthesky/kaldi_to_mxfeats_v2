#export KALDI_ROOT=/data-sdb/speech/lixin/kaldi-20160629/jianwei-niu/src
#export KALDI_ROOT=/data-sdb/speech/lixin/kaldi
export KALDI_ROOT=/home/users/chunqi.wang/kaldi
[ -f $KALDI_ROOT/tools/env.sh ] && . $KALDI_ROOT/tools/env.sh 
export PATH=$PWD/utils/:$KALDI_ROOT/src/bin:$KALDI_ROOT/tools/openfst/bin:$KALDI_ROOT/src/fstbin/:$KALDI_ROOT/src/gmmbin/:$KALDI_ROOT/src/featbin/:$KALDI_ROOT/src/lm/:$KALDI_ROOT/src/sgmmbin/:$KALDI_ROOT/src/sgmm2bin/:$KALDI_ROOT/src/fgmmbin/:$KALDI_ROOT/src/latbin/:$KALDI_ROOT/src/nnetbin:$KALDI_ROOT/src/nnet2bin/:$KALDI_ROOT/src/kwsbin:$KALDI_ROOT/src/online2bin/:$KALDI_ROOT/src/ivectorbin/:$KALDI_ROOT/src/lmbin/:$KALDI_ROOT/src/nnet3bin/:$PWD:$PATH
export LC_ALL=C
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/cuda-7.0/lib64
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/hdp/current/hadoop-yarn-client:/usr/hdp/current/hadoop-mapreduce-client

