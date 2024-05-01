## Importing the libraries
from pyspark import SparkContext
import sys
import math

## Function to count the number of triangles
def find_triangles(vertex, neighbors):
    triangles = 0
    for neighbor1 in neighbors:
        for neighbor2 in neighbors:
            if neighbor1 != neighbor2:
                if neighbor1 in adjacency_list[neighbor2]:
                    triangles += 1
    return triangles // 2

## Main function
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Please enter proper arguments")
        sys.exit(1)

    filedir = sys.argv[1]

    # Initialize SparkContext
    sc = SparkContext("local","ans")

    # Read the text file as an RDD
    lines = sc.textFile(filedir)

    # Split each line by tab character and create key-value pairs where node1 is the key and node2 is the value
    pairs = lines.map(lambda line: line.split())

    listx=pairs.collect()

    adjacency_list = {}
    for edge in listx:
        source = int(edge[0])
        destination = int(edge[1])
        if source not in adjacency_list:
            adjacency_list[source] = []
        if destination not in adjacency_list:
            adjacency_list[destination] = []
        adjacency_list[source].append(destination)
        adjacency_list[destination].append(source)

    total_triangles = 0
    for vertex, neighbors in adjacency_list.items():
        triangles = find_triangles(vertex, neighbors)
        total_triangles += triangles
    total_triangles = int(total_triangles / 3)

    # Calculate the total number of edges
    total_edges = sum(len(neighbors) for neighbors in adjacency_list.values()) // 2

    # Calculate the square root of the total number of edges
    root_m = math.sqrt(total_edges)

    # Find heavy hitter nodes (nodes with degree greater than sqrt(m))
    heavy_hitters = sum(1 for node, neighbors in adjacency_list.items() if len(neighbors) > root_m)

    print("No of heavy hitter nodes:", heavy_hitters)
    print("No of triangles:", total_triangles)

    # Stop SparkContext
    sc.stop()

                        ###### Use arguments as spark-submit <your-code> <path to file> to run the file ######
