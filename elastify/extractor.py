#!/usr/bin/env python3
# -*- coding:utf8 -*-
""" The extractor extracts the synsets and preflabels from a thesaurus .nt or
.json file.  It may also be used to retain the length of the longest
{pref,alt}label"""

from __future__ import print_function
import argparse
import yaml
import os
import sys
import json
from elasticsearch import Elasticsearch
from collections import deque, defaultdict
try:
    from elastify.thesaurus_reader import ThesaurusReader
except ImportError:
    from thesaurus_reader import ThesaurusReader


ES = Elasticsearch(timeout=3600)

ES_PREPROCESSOR_INDEX = "tmpthesauruspreprocessor"


SETTINGS_PATH = os.path.join(os.path.split(__file__)[0], "thes_prep.yaml")
print(SETTINGS_PATH)
with open(SETTINGS_PATH, 'r') as thes_prep_file:
    ES_PREPROCESSOR_INDEX_SETTINGS = yaml.load(thes_prep_file)


def analyze(label, analyzer="ThesaurusPreprocessor"):
    """ Analyzes words with elasticsearch
    Arguments:
    words: [str]"""
    if label == "":
        # nothing to do here *flies away
        return ""
    results = ES.indices.analyze(index=ES_PREPROCESSOR_INDEX,
                                 analyzer=analyzer, body=label)
    analyzed_label = " ".join([result['token']
                               for result in results['tokens']])
    # print(label, "=>", analyzed_label)
    return analyzed_label


def spread(reader, concept, direction='broader'):
    """Perform Spreading Activation up to the root(s).

    :reader: Thesaurus reader instance
    :concept: The concept to perform spreading activation on
    :returns: List of activated concepts

    """
    queue = deque(concept[direction])
    activated = list()
    while queue:
        current = queue.popleft()
        activated.append(current)
        for parent in reader.thesaurus[current][direction]:
            queue.append(parent)
    return set(activated)


def store_hierarchy(reader, filehandle, down=False, surround="{}",
                    onehop=False):
    """TODO: Docstring for store_hierarchy.

    :reader: TODO
    :filehandle: TODO
    :analyzer: TODO
    :returns: TODO

    """
    d = "narrower" if down else "broader"
    for desc_id, concept in reader.thesaurus.items():
        lhs = surround.format(desc_id)
        if onehop:
            rhs = [surround.format(desc_id) for desc_id in set(concept[d])]
        else:
            rhs = [surround.format(desc_id) for desc_id in spread(reader,
                                                                  concept,
                                                                  direction=d)]
        if rhs:
            print(lhs, '=>', ', '.join(rhs), file=filehandle)
        else:
            print("Warning: No righthandside for", lhs, file=sys.stderr)


def levels_by_concept(reader):
    """levels_by_concept

    :param reader:
    """
    """TODO: Docstring for store_levels.

    :reader: TODO
    :filehandle: TODO
    :returns: TODO

    """
    # use json or yaml? -> JSON
    import networkx as nx

    levels = {desc_id: nx.shortest_path_length(reader.nx_graph,
                                               reader.nx_root,
                                               reader.nodename_index[desc_id])
              for desc_id in
              reader.thesaurus}

    return levels


def level_counts(levels):
    """level_counts

    :param levels: iterable of hierachy levels.
    """
    counts = defaultdict(int)
    for level in levels:
        counts[level] += 1

    return counts


def store_preflabel2descid(reader, filehandle, analyzer=None):
    """TODO: Stores a file of "<preflabel> => <desc_id>" lines.

    :reader: TODO
    :filehandle: TODO
    :analyzer: TODO
    :returns: TODO

    """
    for desc_id, concept in reader.thesaurus.items():
        print (concept)
        
        try:
            pref_label = concept['prefLabel'][0]
        except IndexError:
            pref_label = ""
        if pref_label == "":
            print("[extractor] Warning: empty or out of range prefLabel", file=sys.stderr)
        if analyzer:
            pref_label = analyze(pref_label, analyzer=analyzer)
        print("{} => {}".format(pref_label, desc_id), file=filehandle)


def concept_extraction(reader, filehandle, analyzer=None, surround="{}"):
    """uses both, pref and alt labels to map to descriptor ID

    :reader: TODO
    :filehandle: TODO
    :returns: TODO

    """
    for desc_id, concept in reader.thesaurus.items():
        # use both pref labels and alt labels for lefthand side
        labels = concept['prefLabel'] + concept['altLabel']
        labels = list(filter(None, labels))  # drop empty string values
        if analyzer:
            # analyze with elasticsearch if desired
            labels = [analyze(label, analyzer=analyzer) for label in labels]
        if labels:  # dont write empty left hand side
            line = ", ".join(labels) + " => " + surround.format(desc_id)
        print(line, file=filehandle)


def store_descriptor_ids(reader, filehandle, surround="{}"):
    for desc_id, _ in reader.thesaurus.items():
        print(surround.format(desc_id), file=filehandle)


def store_labels(reader, filehandle, synsets=False, analyzer=None):
    """store_labels

    :param reader: Thesaurus reader
    :param filehandle: Filehandle to write labels to.
    :param synsets: If true, compute synsets
    :param analyzer: If given, analyze using the analyzer
    """
    """ Uses the thesaurus reader to create a file of preflabels\ to use in
    keep token filter of elasticsearch or if synsets=True, generates a file
    from the altlabel -> preflabel mappings """
    
    
    
    for _, concept in reader.thesaurus.items():
        try:
            pref_label = concept['prefLabel'][0]
            if analyzer:
                pref_label = analyze(pref_label, analyzer=analyzer)
            if synsets:
                alt_labels = [alt_label for alt_label in concept['altLabel']
                              if len(alt_label) > 0]
                if len(alt_labels) == 0:
                    continue
                if analyzer:
                    alt_labels = [analyze(alt_label, analyzer=analyzer)
                                  for alt_label in alt_labels]
                synset_string = ", ".join(alt_labels) + " => " + pref_label
                print(synset_string, file=filehandle)
            else:
                print(pref_label, file=filehandle)

        except IndexError:
            print ("Skipping Warning: IndexError list index out of range")
        

def retrieve_max_length(reader):
    """ Computes the maximum number of words given in an alt or pref label"""
    max_length = 0
    for _, concept in reader.thesaurus.items():
        labels = [altlabel for altlabel
                  in concept['altLabel']] + [concept['prefLabel'][0]]
        for label in labels:
            words = label.split()
            length = len(words)
            if length > max_length:
                max_length = length
    return max_length


def belllog_discount(reader, filehandle, delimiter="|", surround="{}"):
    import math
    levels = levels_by_concept(reader)
    counts = level_counts(levels.values())
    print(counts)

    for desc_id, _ in reader.thesaurus.items():
        # prevent zero division
        sdesc = surround.format(desc_id)
        discount = 1.0 / (math.log10(counts[levels[desc_id]] + 1))
        print(sdesc, "=>", "{}|{}".format(sdesc, discount),
              file=filehandle)


def main():
    """ Main Function """
    parser = argparse.ArgumentParser()
    parser.add_argument("filename")
    parser.add_argument("-l", "--max-length", action='store_true',
                        default=False,
                        help="Computes the maximum count of words\
                        occuring in a {alt,pref}label")
    parser.add_argument("-a", "--analyzer", default=None,
                        help="Analyze the labels using an elasticsearch\
                        analyzer")

    parser.add_argument("-c", "--concepts", default=None,
                        type=argparse.FileType('w'),
                        help="Extracts stemmed altLabel,prefLabel => desc_id\
                        mapping")

    parser.add_argument("-d", "--descs", default=None,
                        type=argparse.FileType('w'),
                        help="Extracts desc_ids mapping")

    parser.add_argument("-s",
                        "--synsets",
                        default=None,
                        type=argparse.FileType('w'),
                        help="Extract synsets and store to file")

    parser.add_argument("-p",
                        "--preflabels",
                        default=None,
                        type=argparse.FileType('w'),
                        help="Extract preflabels and store to file")

    parser.add_argument("-S", "--spreading-activation",
                        default=None,
                        type=argparse.FileType('w'),
                        help="For each concept, store the whole hierarchy to\
                        the root(s).")

    parser.add_argument("-o", "--one-hop",
                        default=None,
                        type=argparse.FileType('w'),
                        help="For each concept, activate one parent level")

    parser.add_argument("-D", "--down-spreading",
                        default=None,
                        type=argparse.FileType('w'),
                        help="For each concept, store the whole hierarchy to\
                        the bottom.")

    parser.add_argument("-L", "--levels",
                        default=None,
                        type=argparse.FileType('w'),
                        help="For each desc_id, store the bellog discount\
                        factor according to levels.")

    parser.add_argument("-C", "--counts",
                        default=None,
                        type=argparse.FileType('w'),
                        help="For each level in the thesaurus' hierarchy,\
                        store the count of concepts on this level.")

    parser.add_argument("-P", "--preflabel2descid",
                        default=None,
                        type=argparse.FileType('w'),
                        help="Store a preflabel to desc_id mapping")

    parser.add_argument("-b", "--belllog",
                        default=None,
                        type=argparse.FileType('w'),
                        help="Store Belllog discounts")

    parser.add_argument("-F", "--surround", type=str,
                        default='<{}>',
                        help="Specify a format string to use a surrounding for\
                        descriptor ids [default: '{}']")

    args = parser.parse_args()
    thes_reader = ThesaurusReader(args.filename, normalize=False)
    if args.analyzer:
        print("Setting up temporary index to use for analyzis")
        if ES.indices.exists(ES_PREPROCESSOR_INDEX):
            ES.indices.delete(ES_PREPROCESSOR_INDEX)
        # FIXME wtf prints 404 here?
        print(ES.indices.create(index=ES_PREPROCESSOR_INDEX,
                                body=ES_PREPROCESSOR_INDEX_SETTINGS))
        # ES.cluster.health(wait_for_status='green')
        print("Done.")

    if args.concepts:
        concept_extraction(thes_reader, args.concepts, analyzer=args.analyzer,
                           surround=args.surround)

    if args.descs:
        store_descriptor_ids(thes_reader, args.descs, surround=args.surround)

    if args.synsets:
        store_labels(thes_reader, args.synsets, synsets=True,
                     analyzer=args.analyzer)

    if args.preflabels:
        store_labels(thes_reader, args.preflabels, synsets=False,
                     analyzer=args.analyzer)

    if args.max_length:
        print("Maximum word count in an {alt,pref}label: ",
              retrieve_max_length(thes_reader))

    if args.spreading_activation:
        store_hierarchy(thes_reader,
                        args.spreading_activation,
                        surround=args.surround)

    if args.down_spreading:
        store_hierarchy(thes_reader,
                        args.down_spreading,
                        surround=args.surround,
                        down=True)

    if args.one_hop:
        store_hierarchy(thes_reader,
                        args.one_hop,
                        surround=args.surround,
                        onehop=True)

    if args.preflabel2descid:
        store_preflabel2descid(thes_reader, args.preflabel2descid)

    if args.levels or args.counts:
        levels = levels_by_concept(thes_reader)
        if args.levels:
            json.dump(levels, args.levels)
        if args.counts:
            counts = level_counts(levels.values())
            json.dump(counts, args.counts)

    if args.belllog:
        belllog_discount(thes_reader, args.belllog, surround=args.surround)

    # Clean up
    if args.analyzer and ES.indices.exists(ES_PREPROCESSOR_INDEX):
        ES.indices.delete(ES_PREPROCESSOR_INDEX)
    exit(0)

if __name__ == '__main__':
    main()
