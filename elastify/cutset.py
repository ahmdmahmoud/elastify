#!/usr/bin/env python3
# -*- coding=utf8 -*-
""" Executable """
from __future__ import print_function
from timeit import default_timer
import argparse
import os
import shutil


def collect_elements(pathspecs, recursive=False):
    """ Collects all elements without extension in paths
    and returns a list of sets with the elements found in the respective path
    """
    sets = list()
    for path, ext in pathspecs:
        path = os.path.normpath(path)
        assert os.path.isdir(path)
        # i use walk here because of the filenames _generator_
        for _, _, filenames in os.walk(path):
            elements = set(identifier
                           for identifier, suffix
                           in (os.path.splitext(filename)
                               for filename in filenames)
                           if suffix == ext)
            sets.append(elements)
            print("%s: %d" % (path, len(elements)))

            if not recursive:
                break
    return sets


def cutset_copy(cutset, pathspecs, parent_dir):
    """
    Takes an iterable of document identifiers (without extension), an iterable
    for the paths to search for these identifiers with extension exts, such
    that exts[i] correspponds to paths[i].  The prefix is used to create the
    names of the new directories!
    """
    parent_dir = os.path.normpath(parent_dir)
    for i, (src_path, ext) in enumerate(pathspecs):
        tail = os.path.basename(os.path.normpath(src_path))
        dst_path = os.path.join(parent_dir, tail)
        try:
            os.makedirs(dst_path)
        except OSError:
            print(dst_path, "already exists. Abort.")
            exit(1)
        for j, element in enumerate(cutset):
            src = os.path.join(src_path, element) + ext
            dst = os.path.join(dst_path, element) + ext
            shutil.copyfile(src, dst)
            print("\r[%3.0f%%]" %
                  (100 *
                   (i *
                    len(cutset) +
                       j +
                       1) /
                      (len(pathspecs) *
                       len(cutset))), flush=True, end="")


def check_supersets(names, sets):
    """
    Checks if there are any supersets contained in sets If any two sets a,b
    fulfill the superset property a>=b, we return the tuple of a_name, b_name,
    a-b
    """
    supersets = [(a_name, b_name, a >= b, len(a - b)) for (a_name, a), (b_name, b) in zip(zip(names, sets), zip(names, sets)) if a_name != b_name]
    return supersets


def main():
    """ Main Function """
    start = default_timer()
    parser = argparse.ArgumentParser()
    parser.add_argument("pathspecs", nargs="+",
                        help="Pathspecs to the directories to intersect with the following syntax\
              path%%ext, as in example/dir/%%.json")
    parser.add_argument(
        "-c",
        "--copy",
        action='store_true',
        help="Copy intersected filenames to prefix:dirname")
    parser.add_argument("-p",
                        "--prepend",
                        type=str,
                        default="cs",
                        help="parent directory name to use for copying (-c). This should NOT exist before")
    parser.add_argument(
        "-s",
        "--supersets",
        action='store_true',
        help="Check for supersets and find their differences")
    args = parser.parse_args()
    parent_dir = os.path.normpath(args.prepend)
    assert args.prepend != ""

    pathspecs = [(os.path.normpath(path), ext) for path, ext
                 in (pathspec.rsplit("%", maxsplit=1)
                     for pathspec in args.pathspecs)]

    print("Decomposed pathspecs into:", *pathspecs)

    print("Collecting elements...")
    sets = collect_elements(pathspecs)

    union = set.union(*sets)
    print("Collected a total of %d elements in %d sets."
          % (len(union), len(sets)))

    print("Intersecting...")
    cutset = set.intersection(*sets)

    print("Cutset retained %d elements. [%3.2f%% with respect to their union]"
          % (len(cutset), 100 * len(cutset) / len(union)))

    if args.supersets:
        supersets = check_supersets([pathspec[0]
                                     for pathspec in pathspecs], sets)
        print("Supersets:", *supersets, sep="\n")

    if args.copy:
        print("Copying cutset into subdirectories of %s..." % parent_dir)
        cutset_copy(cutset, pathspecs, parent_dir)

    minutes, seconds = divmod(default_timer() - start, 60)
    hours, minutes = divmod(minutes, 60)
    print(" Finished after %d hours, %d minutes and %.0f seconds."
          % (hours, minutes, seconds))
    exit(0)

if __name__ == '__main__':
    main()
