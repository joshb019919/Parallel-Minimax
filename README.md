# Parallel Minimax

Parallelize the Minimax algorithm using PySpark RDDs.

## Comes with a graph and a tree generator, for convenience.

Generates all 5478 valid game board states and and 16167 edges between them.  It's designed to act like a directed graph, but display like a tree for easy viewing.

Usage examples for the new dataset generators:

Generate a graph with 120000 nodes (default):

```
python3 generate_graph.py --nodes 120000 --max-out 3 --out graph_120k.json
```

Generate a tree with 130000 nodes (default branching 3):

```
python3 generate_tree.py --nodes 130000 --max-children 3 --out tree_130k.json
```

Both generators accept `--seed` to make results deterministic.

To write compact binary files (pickle) for later re-loading by the main program use `--binary-out`.
Use `--compress` to gzip-compress the binary output.

Example with binary output and gzip:

```
python3 generate_graph.py -n 120000 -b graph_120k.bin --compress
python3 generate_tree.py -n 130000 -b tree_130k.bin --compress
```

To run the main file demonstrating correct choice:

```
python3 parallel_minimax.py [<dataset-file-name>]
```

## Requirements

If no dataset is specified, the simple tree created in the class is used.  If the dataset is specified, this program expects it to be in the format of:

\[{integer, string, array or list, integer}, ...\]

as JSON to represent each node's ID (serialized from 0 to `len(nodes)`), type (one of `max`, `min`, or `leaf`), children nodes by ID, and value, if leaf.

# WARNING!!!!

This current factoring is preliminary, proof-of-concept.  It is _super_ slow!  At least for me on my personal machine, it's been using 100% CPU for over an hour and is still going.

Also, it has used approximately 8GB of memory, so your mileage may vary.
