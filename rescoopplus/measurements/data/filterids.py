#!/usr/bin/env python


import sys


def readids(filestream):
    return set(int(line) for line in filestream if line.strip())

sys.stdin.readline() # filter header
ids = readids(sys.stdin)

with open(sys.argv[1]) as filteredfile:
    filteredids = readids(filteredfile)

output_ids = ids - filteredids

for out_id in sorted(output_ids):
    print out_id






