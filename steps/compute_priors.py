import sys
import numpy as np


def compute_priors(in_file, num_pdf, outfile):
    with open(in_file) as fin:
        with open(outfile, "w") as fout:
            lines = fin.readlines()
            matrix = []
            for line in lines:
                line = line.split(' ')[1:-1]
                # remove 1 added by analyse-counts
                for idx, i in enumerate(line):
                    if i == "1":
                        line[idx] = "0"
                # append 0 upto num_pdf
                if len(line) != int(num_pdf):
                    diff = int(num_pdf) - len(line)
                    for i in range(diff):
                        line.append("0")
                line = np.asarray(line, dtype=int)
                matrix.append(line)
            matrix = np.asarray(matrix)
            prior = matrix.sum(axis=0)
            sum = prior.sum()
            prior_prob = [ i / float(sum) for i in prior ]
            fout.write(str(prior_prob))

if __name__ == '__main__':
    if len(sys.argv) != 4:
        print 'Usage: save_meanvar.py total_mapper_priors num_pdf final_pdf_prior_prob'
        sys.exit(1)

    prior_array = compute_priors(sys.argv[1], sys.argv[2], sys.argv[3])
    print "ndarray file %s saved" % sys.argv[3]
