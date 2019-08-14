#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import gzip
import os
import argparse
import re
import sys
from langdetect import detect
from urllib.parse import unquote
from os.path import splitext, isdir, isfile
from joblib import Parallel, delayed

line_regex = re.compile('[(\d\.)]+ - .+? \[(.*?)\] "(.*?)" [\d-]+ [\d-]+ ".*?" "(.*?)"')
# /Search/Results?lookfor=econstor&type=AllFields&submit=Search
url_regex = re.compile('/Search/Results\?lookfor="([^"]*)"&type=AllFields.*')
bot_regex = re.compile('(?i)(bot)|(?i)(spider)|(?i)(slurp)')
clean_regex = re.compile('[\+]')


def process_file(filepath, lang=None, ext='.gz', clean=False):
    """
    Example Line to process in file: 34.245.92.16 - - [24/Feb/2016:04:02:12
    +0100] "GET /Search/Results?lookfor=subject_exact%3A%22Developing+
    »countries%22&type=AllFields&limit=50&filter%5B%5D=isPartOf%3A%22Applied+
    »economics%22&filter%5B%5D=subject%3A%22Entwicklungshilfe%22&filter%5B%5D=subject%3A%22Children%22
    HTTP/1.1" 200 39429 "-" "Mozilla/5.0 (compatible; Applebot/0.3;
    +http://www.apple.com/go/applebot)"
    """
    queries = []
    if splitext(filepath)[1] == ext:
        with gzip.open(filepath, 'rt') as filehandle:
            for line in filehandle.readlines():
                try:
                    groups = line_regex.match(line).groups()
                except AttributeError:
                    print("Not Processed:", line)
                    continue
                # [ip, timestamp, request, retcode, something,something, agent]
                timestamp, request, agent = groups
                url = unquote(request.split(' ')[1])
                # if not "bot" in agent and not "spider" in agent:
                bot = bot_regex.search(agent)
                if not bot:
                    m = url_regex.match(url)
                    if m:
                        query = m.groups()[0]
                        if clean:
                            query = "\"".join([clean_regex.sub(' ', item) for
                                               item in query.split("\"")])
                        try:
                            if lang and detect(query) == lang:
                                queries.append("[{}] {}".format(timestamp,
                                                                query))
                        except Exception:
                            print("[log2query] Warning: Could not detect\
                                lang:", query)

    return queries


def process_paths(paths, ext='.gz', n_jobs=1, verbose=0, lang=None, clean=False):
    """Process an iterable of paths, call process_file for each .gz file found

    :paths: TODO
    :returns: TODO

    """
    filepaths = list()
    for path in paths:
        if isfile(path):
            filepaths.append(path)
        elif isdir(path):
            for dirpath, dirnames, filenames in os.walk(path):
                fps = [os.path.join(dirpath, fn) for fn in filenames]
                filepaths.extend(fps)

    results = Parallel(n_jobs=n_jobs,
                       verbose=verbose)(delayed(process_file)(fp, lang=lang,
                                                              ext=ext,
                                                              clean=clean) for
                                        fp in filepaths)
    queries = [item for sublist in results for item in sublist]
    return queries


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('paths', nargs='+', help="paths for logs\
    to parse")
    parser.add_argument('-o', '--output', type=argparse.FileType('a'),
                        default=sys.stdout,
                        help="output file")
    parser.add_argument('-j', '--jobs', default=-1, type=int,
                        help="Number of jobs")
    parser.add_argument('-v', '--verbose', default=0, type=int,
                        help="Verbosity level")
    parser.add_argument('-l', '--language', default="en", type=str,
                        help="Language filter.\
                        Use '_all' to disable filtering. Default: 'en'")
    parser.add_argument('--raw', action='store_false', dest='clean',
                        default=True,
                        help="Dont clean up the queries (removing '+' signs etc)")
    args = parser.parse_args()
    paths = args.paths
    out = args.output
    lang = None if args.language == '_all' else args.language
    queries = process_paths(paths, n_jobs=args.jobs, verbose=args.verbose,
                            lang=lang, clean=args.clean)
    print(*queries, sep='\n', file=out)
    print("[log2query] Extracted {} queries.".format(len(queries)))


if __name__ == "__main__":
    main()
