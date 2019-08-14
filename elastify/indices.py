#!/usr/bin/env python3
# -*- coding=utf8 -*-
"""
Indices -- manage your elasticsearch indices
invoking without arguments lists all arguments,
indices can be created and deleted with -c and -d commands.

In the future, there will be a possibility to put our analyzers and mappings in
the indices
"""
from __future__ import print_function
import argparse
import time
import os
import pprint
import json
import yaml
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError

ES = Elasticsearch(timeout=60)

DEFAULT_INDEX_BODY = {"settings": {"number_of_shards": 1,
                                   "number_of_replicas": 0}}


def main():
    """ Gives information about indices and allows to manipulate them """
    parser = argparse.ArgumentParser()
    parser.add_argument("command", type=str, help="The command to perform",
                        choices=['list', 'info', 'create', 'delete',
                                 'settings', 'mappings', 'monitor', 'analyze',
                                 'open', 'close'])
    parser.add_argument("index", nargs="?", type=str,
                        help="The index to operate on")
    parser.add_argument("filename", nargs="?", type=str,
                        help="Several commands require an additional argument\
                        such as the filename of the settings to put in command\
                        'set'")
    args = parser.parse_args()

    # assert connection
    # assert ES.ping()

    cmd = args.command

    if cmd == "list":
        print(ES.cat.indices(index=args.index, v=True), end="")

    elif cmd == "create":
        if args.filename:
            fname, ext = os.path.splitext(args.filename)
            with open(args.filename, 'r') as settings_file:
                if ext.lower() == ".yaml":
                    index_settings = yaml.load(settings_file)
                elif ext.lower() == ".json":
                    index_settings = json.load(settings_file)
                    print(index_settings)
                else:
                    print("Unrecognized extension of settings file. Abort.")
                    exit(1)
        else:
            print("Warning: Created index {} without any\
                  mappings!".format(args.index))
            index_settings = DEFAULT_INDEX_BODY

        print(ES.indices.create(index=args.index, body=index_settings))

    elif cmd == "delete":
        try:
            if input("Please confirm to delete index {} [y/N]."
                     .format(args.index)) == 'y':
                print(ES.indices.delete(args.index))
            else:
                print("Aborted.")
        except NotFoundError:
            pass

    elif cmd == "info":
        result = ES.indices.get(args.index, human=True, allow_no_indices=True)
        pprint.pprint(result)
    elif cmd == "settings":
        result = ES.indices.get_settings(args.index, human=True,
                                         allow_no_indices=True)
        pprint.pprint(result)
    elif cmd == "mappings":
        result = ES.indices.get_mapping(args.index, allow_no_indices=True)
        pprint.pprint(result)

    elif cmd == "monitor":
        os.system('clear')
        print(ES.cat.indices(index=args.index, v=True), end="")
        try:
            while True:
                time.sleep(2)
                os.system('clear')
                print(ES.cat.indices(index=args.index, v=True), end="")
        except KeyboardInterrupt:
            pass

    elif cmd=="open":
        print(ES.indices.open(index=args.index))

    elif cmd=="close":
        print(ES.indices.close(index=args.index))

    elif cmd == "set":
        ES.indices.close(index=args.index)
        with open(args.filename, 'r') as settings_file:
            ES.indices.put_settings(index=args.index,
                                    body=json.load(settings_file))
        ES.indices.open(index=args.index)

if __name__ == '__main__':
    main()
