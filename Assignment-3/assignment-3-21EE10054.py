from pyspark import SparkContext
import sys
import numpy as np
import time

# Start the timer
start_time = time.time()

########################################
### Number of Heavy hitter Triangles ###
########################################

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

# Count the number of edges
num_edges = edges.count()

# Calculate the threshold for heavy hitter nodes
threshold_count = num_edges ** 0.5

# Calculate the number of heavy hitter nodes
heavy_hitter_nodes = sum(1 for node, neighbors in adjacency_list.items() if len(neighbors) >= threshold_count)
print("No of heavy hitter nodes:",heavy_hitter_nodes)

###########################
### Number of Triangles ###
###########################
 
def count_triangles(adjacency_list):
    triangle_count = 0
    
    # Iterate through each node
    for node, neighbors in adjacency_list.items():
        # Iterate through each pair of neighbors
        for i in range(len(neighbors)-1):
            for j in range(i + 1, len(neighbors)):
                neighbor1 = neighbors[i]
                neighbor2 = neighbors[j]
                
                # Check if there is an edge between the pair of neighbors
                if neighbor2 in adjacency_list[neighbor1]:
                    # Triangle found
                    triangle_count += 1
    
    # Each triangle is counted 3 times (once from each node), so divide by 3
    triangle_count //= 3
    return triangle_count

print("No of triangles:", count_triangles(adjacency_list))

# Stop SparkContext
sc.stop()

# Calculate the execution time
execution_time = time.time() - start_time
print("Execution time:", execution_time, "seconds")
