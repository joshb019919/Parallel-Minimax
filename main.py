from pyspark import RDD
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
import json
import sys
import os

# Set string has determinism to fix [PYTHON_HASH_SEED_NOT_SET] error
os.environ["PYTHONHASHSEED"] = "0"

class ParallelMinimax:
    def __init__(self):
        # Add string hash seed to Spark for good measure
        self.conf = SparkConf()
        self.conf.set("spark.executorEnv.PYTHONHASHSEED", "0")
        self.file = self.get_filename()
        
        # Create Spark
        spark = self.create_spark()
        self.session: SparkSession = spark["session"]
        self.context: SparkContext = spark["context"]
        
        # Create tree
        tree = self.get_nodes()
        self.root_id = tree["root"]
        self.nodes = tree["nodes"]

        # Quick test tree to get things working
        self.test_tree = [
            # id,  type,  children,  value
            ["A", "max", ["B", "C"], None],  # Root
            ["B", "min", ["D", "E"], None],
            ["C", "min", ["F", "G"], None],
            ["D", "leaf", [], 5],
            ["E", "leaf", [], 4],
            ["F", "leaf", [], 7],
            ["G", "leaf", [], -2]
        ]

    def get_filename(self):
        """If filename supplied, return filename, else empty string."""
        return sys.argv[1] if len(sys.argv) > 1 else ""

    def create_spark(self):
        # Build session and context to use RDDs
        spark = SparkSession    \
            .builder            \
            .master("local[*]") \
            .appName("MLSpark") \
            .getOrCreate()

        sc = spark.sparkContext
        return {"session": spark, "context": sc}

    def get_nodes(self):
        try:
            with open(self.file, "r") as f:
                tree = json.load(f)
            root_id = tree["root"]
            nodes = self.session.createDataFrame(tree["nodes"])
        except (FileNotFoundError):
            root_id = self.test_tree[0][0]
            nodes = self.session.createDataFrame(self.test_tree)

        return {"root": root_id, "nodes": nodes}

    def minimax(self):
        parent_types = self.nodes             \
            .filter(lambda x: x[1] != "leaf") \
            .map(lambda x: (x[0], x[1]))
        ptype_map = dict(parent_types.collect())
        b_ptype = self.context.broadcast(ptype_map)

        self.values = self.nodes.filter(lambda x: x[1] == "leaf") \
                    .map(lambda x: (x[0], x[3]))   # (node_id, value)

        # Repeat until root is computed:
        #     1. Identify nodes whose children all have known values.
        #     2. Combine child values using min or max.
        #     3. Update parent values.
        while True:
            # Join each parent with current child values
            joined: RDD = self.nodes.filter(lambda x: x[1] != "leaf")         \
                        .flatMap(lambda x: [(child, x[0]) for child in x[2]]) \
                        .join(self.values)                                    \
                        .map(lambda x: (x[1][0], x[1][1]))    # (parent, child_value)

            # Group child values under each parent
            grouped = joined.groupByKey()

            # Apply min or max based on node type
            computed = grouped.join(self.nodes.map(lambda x: (x[0], x[1]))) \
                            .map(lambda x: (
                                x[0],
                                max(x[1][0]) if x[1][1] == "min" else min(x[1][0])
                            ))

            # Merge new values with previous values
            self.values = self.values.union(computed).reduceByKey(lambda a, b: a)

            # Check if the root is done
            if self.values.lookup(self.root_id):
                break

    def print_value(self):
        print("MiniMax value:", self.values.lookup(self.root_id)[0])


def main():
    pm = ParallelMinimax()
    pm.minimax()


if __name__ == "__main__":
    main()
