#!/usr/bin/env python3
"""Generate a directed graph dataset for testing attractor algorithms.

Output format (JSON):
{
  "node_count": n,
  "nodes": [
    {"id": 0, "owner": 0, "priority": 3, "edges": [1,5,...]},
    ...
  ]
}
"""
import argparse
import json
import random
import sys
import pickle
import gzip
from typing import List, Set


def generate_graph(n: int, max_out: int, seed: int = None):
    if seed is not None:
        random.seed(seed)

    nodes = []
    for i in range(n):
        owner = random.randint(0, 1)
        priority = random.randint(0, 15)

        # choose out-degree 1..max_out
        out_deg = random.randint(1, max_out)
        targets: Set[int] = set()
        while len(targets) < out_deg:
            t = random.randrange(n)
            if t != i:
                targets.add(t)

        nodes.append({
            "id": i,
            "owner": owner,
            "priority": priority,
            "edges": sorted(targets),
        })

    return {"node_count": n, "nodes": nodes}


def main(argv: List[str]):
    p = argparse.ArgumentParser(description="Generate a directed graph dataset.")
    p.add_argument("--nodes", "-n", type=int, default=120000,
                   help="Number of nodes to generate (recommended 100000-150000)")
    p.add_argument("--max-out", type=int, default=3,
                   help="Maximum out-degree per node")
    p.add_argument("--seed", type=int, default=None, help="Random seed")
    p.add_argument("--out", "-o", type=str, default="graph_dataset.json",
                   help="Output JSON file path")
    p.add_argument("--binary-out", "-b", type=str, default="graph_dataset.bin",
                   help="Output binary file path (pickle).")
    p.add_argument("--compress", action="store_true",
                   help="Gzip-compress the binary output file")
    args = p.parse_args(argv)

    if args.nodes < 1:
        print("--nodes must be >=1", file=sys.stderr)
        sys.exit(2)

    data = generate_graph(args.nodes, args.max_out, args.seed)
    with open(args.out, "w") as fh:
        json.dump(data, fh, separators=(',', ':'), ensure_ascii=False)
    # write binary (pickle); optionally gzip-compress
    if args.compress:
        with gzip.open(args.binary_out, "wb") as bf:
            pickle.dump(data, bf, protocol=pickle.HIGHEST_PROTOCOL)
    else:
        with open(args.binary_out, "wb") as bf:
            pickle.dump(data, bf, protocol=pickle.HIGHEST_PROTOCOL)

    print(f"Wrote graph with {args.nodes} nodes to {args.out} and binary to {args.binary_out}")


if __name__ == "__main__":
    main(None)
