#!/bin/env python

import sys
import os.path
if __name__ == '__main__':
    assert(len(sys.argv) == 3)
    spk_id = sys.argv[1]
    input = None
    if sys.argv[2] == '-':
        input = sys.stdin
    else:
        input = open(sys.argv[2])

    flag = False
    lines = input.readlines()
    for line in lines:
        if spk_id in line:
            print line.split(' ')[-1][:-1]
            flag = True
    if flag == False:
        print "-1"
