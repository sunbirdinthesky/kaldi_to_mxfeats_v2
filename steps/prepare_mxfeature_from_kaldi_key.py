#!/bin/env python
import numpy
import StringIO
import mxnet as mx
import sys
import struct
import os
import subprocess
import time
import random

g_index      = 1
g_bad_items  = 0
hdfs_path    = ""

# def upload_to_hdfs (local_path, remote_path): #upload to hdfs system and remove local temp file
#     child = subprocess.Popen(
#             "hdfs dfs -put -f " + local_path +
#             " " + remote_path   + local_path.split('/')[-1] +
#             " && /bin/rm "      + local_path +
#             " && echo \'file "  + local_path.split('/')[-1] + " uploaded\'",
#             shell  = True)


def save_record(labels, feats, outfile):
    """
    save as mxnet format
    """
    global g_index
    record_io = mx.recordio.MXRecordIO(outfile, 'w')
    for feat_key_val, label_key_val in zip(feats, labels):
        record_io.write(label_key_val[1].tostring() + feat_key_val[1].tostring()
                        + struct.pack('Q', g_index))

        g_index += 1

if __name__ == '__main__':
    lowerbound = 99999999 #lowerbound for random int generator
    upperbound = 99999999 #upperbound for random int generator

    if len(sys.argv) != 4:
      if len(sys.argv) != 6:
        print 'this script is used to convert labels and feats from kaldi ver. to mxnet ver.'
        print 'Usage: prepare_mxfeature_from_kaldi.py file_in num_sentence_per_block out_prefix [lowerbound upperbound]'
        print 'Note : 1.if out_prefix is a hdfs path, the feats will be uploaded to hdfs instead of saving it at local'
        print '       2.lowerbound default is 99999999, upperbound default is 99999999, these values is use to generate random int'
        print '       3.set lowerbound and upperbound to a very large value(eg:99999999) to disable file split'
        sys.exit(1)
      else:
        lowerbound = int(sys.argv[4])
        upperbound = int(sys.argv[5])

    #check hdfs dirs
    if 'hdfs:' in sys.argv[3]:
        #analize out_prefix path
        try:
            global hdfs_path
            index = 0
            if sys.argv[3][len(sys.argv[3])-1] == '/':
                hdfs_path = sys.argv[3]
                sys.argv[3] = './MXfeats_'
            else:
                index = sys.argv[3].rindex('/')
                hdfs_path = sys.argv[3][:index+1]
                sys.argv[3] = './' + sys.argv[3][index:]

        except:
            sys.stderr.write("error: bad hdfs path\n")
            sys.exit(1)

        #check if hdfs dir exist or not
        child = subprocess.Popen("hdfs dfs -ls " + hdfs_path + " > /dev/null", shell = True)
        if child.wait() != 0:
            sys.stderr.write("error: hdfs dest dir not found\n")
            sys.exit(1)
    print "hdfs path =", hdfs_path
    print "local path =", sys.argv[3]

    num_sentence_per_block = int(sys.argv[2])
    file_key = sys.argv[3]

    pair_count = 0
    file_id = 1
    labels_block, feats_block = [], []
    input = None
    if sys.argv[1] == '-':
        input = sys.stdin
    else:
        input = open(sys.argv[1], 'r')

    while True:
        label = input.readline().split()
        if len(label) == 0:
            break
        label_key, tgt_labels = label[0], [ int(x) for x in label[1:] ]
        feat_line = input.readline().rstrip()
        #input label data struct: label label_of_first_frame label_of_second_frame label_of_third_frame ......
        #input feats data struct: label [
        #       fbank of frame 1, eg: 1.111, 1.222, 1.333, 1.444  .... 1.nnn
        #       fbank of frame 1, eg: 2.111, 2.222, 2.333, 2.444  .... 2.nnn
        #       fbank of frame 1, eg: 3.111, 3.222, 3.333, 3.444  .... 3.nnn
        #       fbank of frame 1, eg: 4.111, 4.222, 4.333, 4.444  .... 4.nnn]
        assert (feat_line.endswith('[')), ("data " + label_key + "error, feats.scp " +
                "line number, " + str(pair_count + 1))
        assert (label_key == feat_line.split()[0]), ("key " +str(label_key) + " error")
        feat_mat = []
        while True:
            feat_line = input.readline().rstrip()
            if feat_line.endswith(']'):
                feat_mat.append(feat_line[:-1])
                break
            feat_mat.append(feat_line)
        #one record finish reading, now: label_key = filename(or other names, whatever, is a label), data type:str
        #    tgt_label = [label_of_first_frame, label_of_second_frame, label_of_third_frame ....] data type:int
        #    feat_mat  = [fbank_of_frame_1, fbank_of_frame_2, fbank_of_frame_3 .... fbank_of_Frame_n] data type:str

        label_array = numpy.array(tgt_labels, dtype=numpy.int32)
        feature_array = numpy.loadtxt(StringIO.StringIO('\n'.join(feat_mat)),
                                      dtype=numpy.float32)

        #check if item length valid or not
        if len(feature_array.tostring())/feature_array.shape[1] != len(label_array.tostring()):
            sys.stderr.write ("bad data, name: " + label_key + "\n"
                + "label(" + str(len(label_array.tostring())) + " bits) "
                + "!= featsi/dim ("
                + "feats: " + str(len(feature_array.tostring())) + " bits, "
                + "dim: " + str(feature_array.shape[1]) + ", "
                + "feats/dim: " + str(len(feature_array.tostring())/feature_array.shape[1]) + "), pass this item\n")
            g_bad_items += 1
            continue

        #cut files
        limit = random.randint(lowerbound, upperbound)
        len_feats = feature_array.shape[0]
        ptr = 0
        title = label_key
        while len_feats > limit:
          title = label_key + "_ptr" + str(ptr)
          labels_block.append((title, label_array[ptr:ptr+limit]))
          feats_block.append((title, feature_array[ptr:ptr+limit]))
          len_feats -= limit
          pair_count += 1
          ptr += limit
          limit = random.randint(lowerbound, upperbound)

        title = label_key + "_ptr_" + str(ptr)
        labels_block.append((title, label_array[ptr:]))
        feats_block.append((title, feature_array[ptr:]))
        pair_count += 1

        #data struct:
        #labels_block = [(title_1, [label_of_frame1, label_of_frame2 ....]),
        #                 (title_2, [label_of_frame1, label_of_frame2 ....])]
        #feats_block = [(title_1, feature_array_1), (title_2, feature_array_2)]
        if pair_count > num_sentence_per_block:
            outfile = file_key + str(file_id)
            save_record(labels_block, feats_block, outfile)
            labels_block, feats_block = [], []
            pair_count = 0
            file_id += 1

    if pair_count > 0:
        outfile = file_key + str(pair_count/num_sentence_per_block)
        save_record(labels_block, feats_block, outfile)
        labels_block, feats_block = [], []
    # if hdfs_path != "":
    #     time.sleep(20) #make sure the final hdfs upload operation is finished
    # sys.stderr.write("total " + str(g_index-1) + " items written\n")
    # sys.stderr.write("total " + str(g_bad_items) + " items broken\n")
