from pyspark import SparkContext, SparkConf
import sys
import math

# Initialize Spark context
path = sys.argv[1]
conf = SparkConf().setMaster("local").setAppName("Graph Analytics")
sc = SparkContext.getOrCreate(conf=conf)
# Add path
file = sc.textFile(path)
edges = file.map(lambda l: l.split())

# Determine threshold for heavy hitter nodes
threshold = math.ceil(math.sqrt(edges.count()))

# Function to count heavy hitter nodes
def count_heavy_hitters(ds, threshold):
    # Calculate the degree of each node
    degrees = ds.flatMap(lambda e: [(e[0], 1), (e[1], 1)])
    node_degrees = degrees.reduceByKey(lambda x, y: x + y)
    
    # Filter out nodes whose degree is greater than or equal to the threshold
    heavy_hitters = node_degrees.filter(lambda x: x[1] >= threshold).count()
    
    return heavy_hitters

# Count heavy hitter nodes
heavy_hitters_count = count_heavy_hitters(edges, threshold)
print(f'No of heavy hitter nodes: {heavy_hitters_count}')

# Function to count triangles in the graph
def count_triangles(ds):
    # Create adjacency list
    adj_list = ds.flatMap(lambda x: [(x[0], x[1]), (x[1], x[0])]) \
                .groupByKey() \
                .mapValues(set) \
                .collectAsMap()

    # Function to count triangles for a given node
    def count_triangles_for_node(node):
        neighbors = adj_list[node]
        triangle_count = 0
        for neighbor in neighbors:
            common_neighbors = adj_list.get(neighbor, set()) & neighbors
            triangle_count += len(common_neighbors)
        return triangle_count // 2  # Divide by 2 to avoid double-counting

    # Sum up triangle counts for all nodes
    total_triangles = sum(count_triangles_for_node(node) for node in adj_list)
    
    return total_triangles // 3  # Divide by 3 to avoid triple-counting

# Count triangles in the graph
triangle_count = count_triangles(edges)
print(f"No of triangles: {triangle_count}")
sc.stop()