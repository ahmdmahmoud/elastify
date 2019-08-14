#!/usr/bin/env python3
# -*- coding=utf8 -*-
# header {{{
"""
Convenient script for adding either plain files to an elasticsearch index
or adding lines from a single file to the elasticsearch index.
"""
from __future__ import print_function
from elasticsearch import Elasticsearch, helpers
from timeit import default_timer

import argparse
import json
import os
ES = Elasticsearch([{'host': 'localhost'}], timeout=3600)


def name2id(path):
    """ Returns basename extension (identifier) and extension
    /data/../data/0001234.txt => 0001234
    Useful for retrieving an appropriate id from filenames
    """
    basename = os.path.basename(path)
    identifier, ext = os.path.splitext(basename)
    return identifier, ext


def generate_actions(path, index, doc_type, op_type='index',
                     fieldname='fulltext', force_update=False, extract=None):
    """ Generate action items to use with elasticsearchs bulk API """
    def process_file(filehandle):
        """ Process one file handle. Returns a dict for the action"""
        _source_or_doc = {'index': '_source', 'update': 'doc'}[op_type]
        identifier, extension = name2id(filehandle.name)
        action = {'_op_type': op_type,
                  '_index': index,
                  '_type': doc_type,
                  '_id': identifier}
        if op_type is 'update':
            action['doc_as_upsert'] = force_update
        if extension == '.txt':
            action[_source_or_doc] = {str(fieldname): filehandle.read()}
        elif extension == '.json':
            # TODO: we could restrict json input to certain fields here
            _full_dict = dict(json.load(filehandle))
            if extract:
                action[_source_or_doc] = {key : value for key,value in
                                          _full_dict.items() if key in extract}
            else:
                action[_source_or_doc] = _full_dict

        return action

    if os.path.isdir(path):
        for dirpath, _, filenames in os.walk(path):
            for filename in filenames:
                with open(os.path.join(dirpath, filename), 'r') as filehandle:
                    yield process_file(filehandle)
    elif os.path.isfile(path):
        with open(path, 'r') as filehandle:
            yield process_file(filehandle)
    else:
        raise ValueError


def main():
    """ Parses command line arguments and either performs indexing or
    partial doc update operations
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("index", help="Elasticsearch index [economics, ...]")
    parser.add_argument(
        "doc_type",
        help="Elasticsearch document type, [publication]")
    parser.add_argument(
        "path",
        type=str,
        help="Path to data directory containing either .txt or .json files")
    parser.add_argument("-u", "--update", action="store_true", default=False,
                        help="Performs update instead of index operations")
    parser.add_argument("-f", "--field", type=str, default="fulltext",
                        help="specify the field for txt files defaults to 'fulltext'")
    parser.add_argument("-e",
                        "--extract",
                        type=str,
                        dest='extract',
                        nargs='+',
                        default=None,
                        help="Extract these fields from json files")
    parser.add_argument(
        "-j",
        "--jobs",
        type=int,
        default=1,
        help="Number of jobs for parallel execution")
    parser.add_argument(
        "-v",
        dest="verbose",
        action="count",
        default=0,
        help="Verbosity")

    try:
        assert ES.ping()
    except AssertionError:
        print("[elastify] Could not connect to elasticsearch instance")
        exit(1)

    args = parser.parse_args()
    if not ES.indices.exists(args.index):
        print("[elastify] Index not found. Please create an index first.")
        exit(1)

    op_type = "update" if args.update else "index"
    print("[elastify] Bulking %s as %s in %s with '%s' using %d jobs..."
          % (args.path, args.doc_type, args.index, op_type, args.jobs))

    # set refresh time to -1
    ES.indices.put_settings(index=args.index, body={"refresh_interval": "-1"})
    actions = generate_actions(args.path, args.index, args.doc_type,
                               op_type=op_type, fieldname=args.field,
                               extract=args.extract)
    bulk = helpers.parallel_bulk(ES, actions,
                                 thread_count=args.jobs,
                                 chunk_size=1000,
                                 raise_on_error=False)
    n_success, n_fails, fails = 0, 0, []
    start = default_timer()
    for success, result in bulk:
        if success:
            n_success += 1
        else:
            n_fails += 1
            if args.verbose:
                fails.append(result[op_type]['error'])
        print("\r[elastify] %7d succeeded, %7d failed."
              % (n_success, n_fails), flush=True, end='')

    # set refresh time to 1s
    ES.indices.put_settings(index=args.index, body={"refresh_interval": "1s"})
    # assert merging
    ES.indices.forcemerge(index=args.index)
    print()
    if args.verbose:
        print(*fails)
    elapsed = default_timer() - start
    minutes, seconds = divmod(elapsed, 60)
    hours, minutes = divmod(minutes, 60)
    print("[elastify] Finished after %d hours,\
            %d minutes and %.0f seconds."
          % (hours, minutes, seconds))

if __name__ == '__main__':
    main()
