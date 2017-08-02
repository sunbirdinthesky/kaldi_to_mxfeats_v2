#!/usr/bin/env python
# -*- coding:utf-8 -*-

import struct
from curses import ascii

def Read(filename):
    input = ''
    if filename == '-':
        input = sys.stdin
    else:
        input = open(filename)
    key_values = []
    for line in input.read().splitlines():
        if len(line.split()) == 2:
            key, chars = line.split()
        else:
            key, chars = line.split()[0], ' '.join(line.split()[1:])
        chars_unicode = chars.decode('UTF-8')
        key_values.append([key, chars_unicode])
    if filename != '-':
        input.close()
    return key_values

def ReadMultiStr(filename):
    input = ''
    if filename == '-':
        input = sys.stdin
    else:
        input = open(filename)
    key_values = []
    for line in input.read().splitlines():
        key, chars = line.split()[0], ' '.join(line.split()[1:])
        chars_unicode = chars.decode('UTF-8')
        key_values.append([key, chars_unicode])
    if filename != '-':
        input.close()
    return key_values

def Write():
    pass
    
def IsChineseChar(code):
    if code >= 0xB0A1 and code <= 0xF7FE:
        return True
    elif code >= 0x8140 and code <= 0xA0FE:
        return True
    elif code >= 0xAA40 and code <= 0xFEA0:
        return True
    else:
        return False

def IsGBKPunctuation(code):
    if code >= 0xA1A1 and code <= 0xA9FE \
            and (not IsAlphanum(code)):
        return True
    else:
        return False

def IsGBKDigital(code):
    code = code - 0xA380
    if code >= ord('0') and code <= ord('9'): #code >= '0' and code <= '9'
        return True
    else:
        return False

def IsGBKAlphabeta(code):
    code = code - 0xA380
    if (code >= ord('A') and code <= ord('Z')) or (code >= ord('a') and code <= ord('z')):
        return True
    else:
        return False

def IsGBKAlphanum(code):
    return IsGBKAlphabeta(code) or IsGBKDigital(code)

def IsASCIIPunctuation(code):
    return ascii.ispunct(code)

def IsASCIIAlphanum(code):
    return ascii.isalnum(code)

def IsPunctuation(code):
    return IsGBKPunctuation(code) or ascii.ispunct(code)

def IsAlphanum(code):
    return IsGBKAlphanum(code) or ascii.isalnum(code)

def IsDigital(code):
    return IsGBKDigital(code) or ascii.isdigit(code)

def IsASCII(code):
    if code <= int(0x7F):
        return True
    else:
        return False

def ShowAscii(char, code):
    if ascii.isalpha(code):
        print char, 'is an ascii alphabeta'
    elif ascii.isdigit(code):
        print char, 'is an ascii digital'
    elif ascii.ispunct(code):
        print char, 'is an ascii punctuation'
    else:
        print char, 'is an ascii code(not alphabeta, number or punctuation)'


def ShowString(gbk_str):
    i = 0
    while i < len(gbk_str):
        cur_char = gbk_str[i:i+1]
        one_byte_code = struct.unpack('B', gbk_str[i:i+1])[0]
        if IsASCII(one_byte_code):
            #process ascii code: digital alphebeta, punctuation
            ShowAscii(cur_char, one_byte_code)
            i +=1
            continue

        cur_char = gbk_str[i:i+2]
        gbk_code = struct.unpack('>H', gbk_str[i:i+2])[0]
        if IsChineseChar(gbk_code):
            print cur_char.decode('GBK'), 'is chinese charactor'
        elif IsGBKPunctuation(gbk_code):
            print cur_char.decode('GBK'), 'is chinese punctuation'
        elif IsGBKAlphabeta(gbk_code):
            print cur_char.decode('GBK'), 'is chinese alphabeta'
        elif IsGBKDigital(gbk_code):
            print cur_char.decode('GBK'), 'is chinese digital'
        else:
            sys.stderr.write(cur_char.decode('GBK') + 'unkown GBK code')
        i += 2

def RemovePunctuation(gbk_str):
    new_str = []
    i = 0
    while i < len(gbk_str):
        cur_char = gbk_str[i:i+1]
        one_byte_code = struct.unpack('B', cur_char)[0]
        if IsASCII(one_byte_code):
            if not IsASCIIPunctuation(one_byte_code):
                new_str.append(cur_char)
            i +=1
            continue

        cur_char = gbk_str[i:i+2]
        gbk_code = struct.unpack('>H', cur_char)[0]
        if not IsGBKPunctuation(gbk_code):
            new_str.append(cur_char)
        i += 2
    return ''.join(new_str)

def SplitChar(gbk_str):
    new_str = []
    i = 0
    while i < len(gbk_str):
        cur_char = gbk_str[i:i+1]
        one_byte_code = struct.unpack('B', cur_char)[0]
        if IsASCII(one_byte_code):
            new_str.append(cur_char)
            i +=1
            continue

        cur_char = gbk_str[i:i+2]
        gbk_code = struct.unpack('>H', cur_char)[0]
        new_str.append(' ' + cur_char + ' ')
        i += 2
    return ''.join(new_str)


def ConvertAlphanumToGBK(gbk_str):
    new_str = []
    i = 0
    while i < len(gbk_str):
        cur_char = gbk_str[i:i+1]
        one_byte_code = struct.unpack('B', cur_char)[0]
        if IsASCII(one_byte_code):
            if IsASCIIAlphanum(one_byte_code):
                gbk_char = struct.pack('>H', one_byte_code + 0xA380)
                new_str.append(gbk_char)
            else:
                new_str.append(cur_char)
            i +=1
            continue

        cur_char = gbk_str[i:i+2]
        gbk_code = struct.unpack('>H', cur_char)[0]
        if not IsGBKPunctuation(gbk_code):
            new_str.append(cur_char)
        i += 2
    return ''.join(new_str)

def ProcessRemovePunctuation(key_values):
    for key, str_unicode in key_values:
        #print  key, str_unicode.encode('UTF-8')
        gbk_str = str_unicode.encode('GBK')
        #print gbk_str.decode('GBK'), 'in gbk has length of', len(gbk_str)
        new_str = RemovePunctuation(gbk_str)
        new_str = ConvertAlphanumToGBK(new_str)
        #ShowString(gbk_str)
        print key, new_str.decode('GBK').encode('UTF-8')

def ProcessSplitChar(key_values):
    for key, str_unicode in key_values:
        #print  key, str_unicode.encode('UTF-8')
        gbk_str = str_unicode.encode('GBK')
        #print gbk_str.decode('GBK'), 'in gbk has length of', len(gbk_str)
        new_str = SplitChar(gbk_str)
        new_str = ConvertAlphanumToGBK(new_str)
        #ShowString(gbk_str)
        print key, new_str.decode('GBK').encode('UTF-8')

from optparse import OptionParser
import sys
if __name__ == '__main__':
    usage = 'Usage: remove-chinese-punct.py file'
    parser = OptionParser(usage=usage)
    parser.add_option('--remove-punct', dest="remove_punct", default=False,
                     help="remove punctuation in text")
    parser.add_option('--multi-str', dest="multi_str", default=False,
                     help="multiple string in one line")
    parser.add_option('--split-char', dest="split_char", default=False,
                     help="split text into charactor")
    options, args = parser.parse_args()
    if len(args) != 1:
        parser.print_help()
        sys.exit(1)

    if options.multi_str:
        key_values = ReadMultiStr(args[0])
    else:
        key_values = Read(args[0])
    if options.remove_punct:
        ProcessRemovePunctuation(key_values)
    if options.split_char:
        ProcessSplitChar(key_values)
