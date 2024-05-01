from pyspark import SparkContext
import sys
import numpy as np


# Check if the correct number of command-line arguments is provided
if len(sys.argv) != 2:
    print("Usage: python script.py <path_to_file>")
    sys.exit(1)

path_to_file = sys.argv[1]

# Initialize SparkContext
sc = SparkContext("local", "Adjacency List")
text_rdd = sc.textFile(path_to_file)

# Split each line of the RDD to extract source and destination vertices
edges = text_rdd.map(lambda line: line.split())

# Collect the edges RDD to the driver node
edge_list = edges.collect()

# Extract all unique nodes from the edge list
nodes_set = set()
for edge in edge_list:
    nodes_set.add(edge[0])
    nodes_set.add(edge[1])

# Convert the set of nodes to an array
nodes_array = np.array(sorted([int(node) for node in nodes_set]))
print(nodes_array)

# Create adjacency list
adjacency_list = {}
for edge in edge_list:
    source = int(edge[0])
    destination = int(edge[1])
    if source not in adjacency_list:
        adjacency_list[source] = []
    if destination not in adjacency_list:
        adjacency_list[destination] = []
    adjacency_list[source].append(destination)
    adjacency_list[destination].append(source)

# Print the adjacency list
print("Adjacency List:")
for key, value in adjacency_list.items():
    print(key, "->", value)

# Count the number of edges
num_edges = edges.count()
print("Number of edges:", num_edges)

# Calculate the threshold for heavy hitter nodes
threshold_count = num_edges ** 0.5
print("Threshold count:", threshold_count)

# Calculate the number of heavy hitter nodes
heavy_hitter_nodes = sum(1 for node, neighbors in adjacency_list.items() if len(neighbors) >= threshold_count)
print("Number of heavy hitter nodes:", heavy_hitter_nodes)

# Stop SparkContext
sc.stop()