from pyspark import SparkContext
import sys
import numpy as np
import math

file_path = sys.argv[1]

sc = SparkContext("local", "Graph Analytics")
text_rdd = sc.textFile(file_path)

edges = text_rdd.map(lambda line: line.split())

threshold = math.ceil(math.sqrt(edges.count()))

def chh(graph_rdd, threshold):
    edges = graph_rdd.flatMap(lambda edge: [(edge[0], 1), (edge[1], 1)])
    node_degrees = edges.reduceByKey(lambda x, y: x + y)
    count = node_degrees.filter(lambda x: x[1] >= threshold).count()
    return count

heavyhitters = chh(edges, threshold)
print("No of heavy hitter nodes:", heavyhitters)

edge_list = edges.collect()
adjacency_list = {}

for edge in edge_list:
    source, destination = int(edge[0]), int(edge[1])
    if source not in adjacency_list:
        adjacency_list[source] = []
    adjacency_list[source].append(destination)
    if destination not in adjacency_list:
        adjacency_list[destination] = []
    adjacency_list[destination].append(source)

triangle_count = 0
for node, neighbors in adjacency_list.items():
    for i in range(len(neighbors) - 1):
        for j in range(i + 1, len(neighbors)):
            neighbor1, neighbor2 = neighbors[i], neighbors[j]
            if neighbor2 in adjacency_list[neighbor1]:
                triangle_count += 1
                
triangle_count = triangle_count // 3

print("No of triangles:", triangle_count)
sc.stop()