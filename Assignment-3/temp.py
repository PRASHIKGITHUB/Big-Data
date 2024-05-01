from pyspark import SparkContext

import sys
# Stop existing SparkContext if it exists
# if 'sc' in locals():
#     sc.stop()

# Initialize new SparkContext
sc = SparkContext("local", "GraphAnalysis")
path=sys.argv[1]
# Rest of the code follows...


# Step 2: Load graph data from file into RDD
graph_file_path = path
graph_data = sc.textFile(graph_file_path)

# Step 3: Split each line into individual nodes
edges = graph_data.map(lambda line: tuple(map(int, line.split())))

# Step 4: Count the degree of each node
node_degrees = edges.flatMap(lambda edge: [(edge[0], 1), (edge[1], 1)]) \
                    .reduceByKey(lambda x, y: x + y)

# Step 5: Find heavy hitter nodes (nodes with high degree)
threshold = (edges.count())**0.5
heavy_hitters = node_degrees.filter(lambda x: x[1] >= threshold)

# Step 6: Count the number of heavy hitter nodes
num_heavy_hitters = heavy_hitters.count()

# Step 7: Find triangles in the graph
# Create a list of neighbor nodes for each node
neighbor_list = edges.flatMap(lambda edge: [(edge[0], edge[1]), (edge[1], edge[0])]) \
                     .groupByKey() \
                     .mapValues(lambda neighbors: sorted(neighbors))

# Broadcast the list of all edges to all worker nodes
edge_list = edges.collect()
neighbor_edges = sc.broadcast(set(edge_list))

# Function to find all triangles given a pair of nodes and their neighbors
def find_triangles(pair):
    v, neighbors = pair[0], pair[1]
    triangles = []
    for neighbor1 in neighbors:
        for neighbor2 in neighbors:
            if neighbor1 < neighbor2 and (neighbor1, neighbor2) in neighbor_edges.value:
                triangles.append((v, neighbor1, neighbor2))
    return triangles

# Generate all possible triangles by checking combinations of connected nodes
triangles = neighbor_list.flatMap(find_triangles)

# Count the total number of triangles
num_triangles = triangles.count() // 3  # Each triangle is counted 3 times

# Step 8: Print results
print("No of heavy hitter nodes:", num_heavy_hitters)
print("No of triangles:", num_triangles)
print("Path to data file:", graph_file_path)

# Step 9: Stop SparkContext
sc.stop()