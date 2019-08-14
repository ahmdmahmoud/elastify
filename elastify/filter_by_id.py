#!/usr/bin/env python3
# -*- coding=utf8 -*-
"""
Retrieves a set of document identifiers from
filenames in a given directory and filters
a file by these identifiers
"""
from __future__ import print_function
import os, argparse, sys

def find_ids_in_dir(path):
    """ Retrieves IDs from filenames in a dir """
    #identifiers = dict()
    #for _, _, filenames in os.walk(path):
        #for filename in filenames:
            #identifiers[os.path.splitext(filename)[0]] = True

    # efficient generator solution
    identifiers = {int(os.path.splitext(filename)[0]) : True\
            for _, _, filenames in os.walk(path)\
            for filename in filenames}
    return identifiers

def main():
    raise DeprecationWarning
    """ Uses <path> to find ids and filters input by these ids """
    parser = argparse.ArgumentParser()
    parser.add_argument("path", help="Use this path to find filenames (without extensions) for ids")
    parser.add_argument("-s", "--seperator", default=None,\
            help="Use this column seperator in input")
    args = parser.parse_args()
    # load all ids into main memory
    identifiers = find_ids_in_dir(args.path)
    n_lines, n_lines_filtered = 0, 0
    print("[+] Found %d identifiers." % len(identifiers), file=sys.stderr)
    for line in sys.stdin.readlines():
        identifier = int(line.split(sep=args.seperator, maxsplit=1)[0])
        n_lines += 1
        if identifier in identifiers:
            print(line, file=sys.stdout, end="")
            n_lines_filtered += 1
    print("[+] Filtered %d -> %d lines [%.2f%%]" % (n_lines, n_lines_filtered, (100*n_lines_filtered / n_lines)), file=sys.stderr)


if __name__ == '__main__':
    main()
