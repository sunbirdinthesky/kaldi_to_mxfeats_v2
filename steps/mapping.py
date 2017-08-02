import sys
if __name__ == '__main__':

    if len(sys.argv) != 4:
        print 'This script is used to generate the input for converting kaldi feats to mxnet format, which maps the feats.scp files from the all perturbed combined kaldi data with corresponding ali.1.gz files computed from those original and perturbed with speed kaldi data.'
        print 'Usage: mapping.py file_out all_feats_scp_list clean_ali_gz_list'
        sys.exit(1)

    output_file = sys.argv[1]
    feats_scp_list = sys.argv[2]
    ali_gz_list = sys.argv[3]
    with open(output_file, "w") as fout:
        with open(feats_scp_list) as scp_f:
            with open(ali_gz_list) as ali_f:
                scp_lines = scp_f.readlines()
                ali_lines = ali_f.readlines()
                for scp_line in scp_lines:
                    # examples of scp keys:
                    # clean_000000, pitch50_000050, speed0.9_000002
                    # noise777_clean_000036, noise777_pitch50_000014, rvb777_clean_000027, rvb777_speed1.1_000001
                    key = scp_line.split('/')[-2]
                    key = key.split('_')
                    if "pitch" in key[-2]:
                        key[-2] = "clean"
                    key = "_".join(key[-2:])
                    for ali_line in ali_lines:
                        # examples of ali keys:
                        # clean_000051, speed0.9_000000, speed1.1_000051
                        if key in ali_line:
                            fout.write(scp_line[:-1] + " " + ali_line[:-1] + "\n")