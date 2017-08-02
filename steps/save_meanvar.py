import sys
import numpy as np
import StringIO

import mxnet as mx


def conver_mapper_cmvn_to_meanvar(in_file, outfile):
    with open(in_file) as fin:
        lines = fin.readlines()
        mean_matrix = []
        var_matrix = []
        for idx, line in enumerate(lines):
            nums = line.split()
            if (idx % 2 == 0):
                mean_matrix.append(nums)
            else:
                var_matrix.append(nums)
        mean_arr = np.array(mean_matrix, dtype=float).sum(axis=0)
        var_arr = np.array(var_matrix, dtype=float).sum(axis=0)

        num_frames = mean_arr[-1]
        negtive_mean = [ - i / num_frames for i in mean_arr[:-1] ]
        mean = [ i / num_frames for i in mean_arr[:-1] ]
        var = [ (1.0 / num_frames * var_i - m_i * m_i)**0.5 for m_i, var_i in zip(mean, var_arr[:-1])]
        inv_stdvar = [ 1 / i for i in var ]
        neg_mean_arr = np.array(negtive_mean, dtype=float)
        inv_std_arr = np.array(inv_stdvar, dtype=float)

        dim = len(neg_mean_arr) / 3
        neg_mean_arr = neg_mean_arr.reshape(3, dim)
        inv_std_arr = inv_std_arr.reshape(3, dim)

        mean = mx.nd.array(neg_mean_arr)
        var = mx.nd.array(inv_std_arr)

        mx.nd.save(outfile, {"mean":mean, "var":var})


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print 'Usage: save_meanvar.py total_mapper_cmvn_without_brackets mean_var_mx_format'
        print '(x + mean)*var'
        sys.exit(1)

    meanvar_array = conver_mapper_cmvn_to_meanvar(sys.argv[1], sys.argv[2])

    #check
    nds = mx.nd.load(sys.argv[2])
    for k, v in nds.iteritems():
        print "dict[%s] =" % k, v.shape, v.asnumpy()

    print "ndarray file %s saved" % sys.argv[2]
