"""Parses a VisualVM Forward Call Tree CSV file and outputs a flamegraph.
In VisualVM CPU Snapshot, select "Export", and choose "Forward Call Tree" and save
as CSV. The input will look something like:
    "org.apache.spark.util.collection.ExternalSorter.writePartitionedFile(ExternalSorter.java:388)", "1,252 ms (100.0%)"
    "  Self time", "1 ms (0.01%)"
    "  org.apache.spark.util.collection.ExternalSorter.someOtherFunction(ExternalSorter.java:388)", "1,150 ms (95.0%)"
    "    Self time", "75 ms (5.0%)"
       (Other calls from of someOtherFunction)
    "org.apache.spark.util.collection.ExternalSorter.doAThing(ExternalSorter.java:388)", "1,150 ms (95.0%)"
The output is a flattened callstack with the time spent in each function consumable by the Brendan Gregg's flamegraph.pl script.
org.apache.spark.util.collection.ExternalSorter.writePartitionedFile(ExternalSorter.java:388)   1252
org.apache.spark.util.collection.ExternalSorter.writePartitionedFile(ExternalSorter.java:388);org.apache.spark.util.collection.ExternalSorter.someOtherFunction(ExternalSorter.java:388)   1150
and so on
Usage:
    python parse_stack.py <path to csv file> > stacks.txt
The output can be used with flamegraph.pl to generate a flamegraph as follows:
    ./flamegraph.pl --title="My Flamegraph" stacks.txt > flamegraph.svg
Open the file in a browser to view the flamegraph.
"""

import csv
import sys


def read_csv(filename):
    """Reads a CSV file and returns a list of lists."""
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        # Skip headers
        next(reader)
        return list(reader)


def flatten_callstack(table):
    """Flattens a callstack table."""
    stacks = []
    for idx, row in enumerate(table):
        call = row[0]
        my_time = float(row[1].replace(',', '').split()[0])
        my_hits = int(row[3].replace(',', ''))
        call_strip = call.strip()
        if call_strip == 'Self time':
            continue
        spaces_before = len(call) - len(call.lstrip())
        my_stack = [call_strip]
        # Read backwards to get calls indented less than my call
        for prev_row in reversed(table[:idx]):
            prev_call = prev_row[0]
            prev_call_strip = prev_call.strip()
            if prev_call_strip == 'Self time':
                continue
            prev_spaces_before = len(prev_call) - len(prev_call.lstrip())
            if prev_spaces_before < spaces_before:
                # This is a parent call,  prepend it, remember current indentation level
                # so we don't get siblings
                my_stack.insert(0, prev_call_strip)
                spaces_before = prev_spaces_before
        stacks.append((my_stack, my_time, my_hits))
    return stacks


def to_flamegraph_format(stacks):
    """Converts a list of stacks to flamegraph format."""
    for stack, _, hits in stacks:
        print(';'.join(stack), hits)


def main(jmx_path):
    data = read_csv(jmx_path)
    flattened = flatten_callstack(data)
    to_flamegraph_format(flattened)


if __name__ == '__main__':
    main(sys.argv[1])
