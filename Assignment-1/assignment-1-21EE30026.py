# Importing the libraries
from threading import Thread
import numpy as np
import sys
from queue import PriorityQueue
from sklearn.metrics.pairwise import cosine_similarity

## Defining necessary functions
# Function for calculation of cosine similarity
def cosineSimilarity(part,query_vector,results):
    for item_id,item_vector in part.items():
        similarity=cosine_similarity([item_vector],[query_vector])[0][0]
        results.put((-similarity,item_id))

# Function to read the data file containing the dataset
def read_item_vectors(data_file):
    item_vectors={}
    with open(data_file,'r') as f:
        for line in f:
            parts=line.strip().split()
            item_id=parts[0]
            vector=np.array([float(x) for x in parts[1:]])
            item_vectors[item_id]=vector
    return item_vectors

# Function to read the query file containing the query vector
def read_query_vector(query_file):
    with open(query_file,'r') as f:
        query_vector=np.array([float(x) for x in f.readline().strip().split()])
    return query_vector

# Defining a seperate function to perform multithreaded programming
def multiThreading(item_vectors,query_vector,num_threads,k):

    # Creating priority queue for storing results
    results=PriorityQueue()

    # Thread Creation
    threads=[None]

    # Partition of the dataset
    size=len(item_vectors)//num_threads
    chunks = [list(item_vectors.items())[i:i+size] for i in range(0,len(item_vectors),size)]

    # Assigning the tasks to the threads and initiating them
    for i, chunk in enumerate(chunks):
        thread=Thread(target=cosineSimilarity,args=(i,dict(chunk),query_vector,results))
        thread.start()
        threads.append(thread)
    
    # Joining the threads to synchronise the process
    for thread in threads:
        thread.join()

    top_k_vectors=[]
    while not results.empty():
        top_k_vectors.append(results.get())

    output_vector=[]
    for similarity,item_id in top_k_vectors[:k]:
        output_vector.append((item_id,-similarity))

    return output_vector[:k]

# Defining the main function
if __name__=="__main__":
    if len(sys.argv)!=6:
        print("Usage: python <your-code.py> <data file> <query item file> <# threads> <value of k>")
        sys.exit(1)

    data_file=sys.argv[1]
    query_file=sys.argv[2]
    num_threads=int(sys.argv[3])
    k=int(sys.argv[4])

    item_vector=read_item_vectors(data_file)
    query_vector=read_query_vector(query_file)
    top_k_vectors=multiThreading(item_vector,query_vector,num_threads,k)

    # Printing the output 
    for item_id, similarity in top_k_vectors:
        print(item_id, similarity)
