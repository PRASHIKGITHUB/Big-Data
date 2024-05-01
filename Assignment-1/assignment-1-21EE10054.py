import threading
import queue
from threading import Thread
from scipy.spatial.distance import cosine
import sys
# for reading the arguments from command line
if len(sys.argv) != 5:
    print("Usage: python your_code.py <data_file> <query_item_file> <num_threads> <k>")
    sys.exit(1)

data_file = sys.argv[1]
query_item_file = sys.argv[2]
num_threads = int(sys.argv[3])
k = int(sys.argv[4]) 

query_vector=[] #contains query vector
vectors = [] #this contains all the data given in data_file

#taking out the query data from file and storing it into vectors
with open(query_item_file,'r') as file:
    for line in file:
        vector=list(map(float,line.split()))
        query_vector.append(vector)

#taking out the data from queryfile and storing it into query_vector
with open(data_file, 'r') as file:
    for line in file:
        vector = list(map(float, line.split()))
        vectors.append(vector)

threads=[None]*num_threads
result = [queue.PriorityQueue() for _ in range(num_threads)] #creates quque vector for storing results from multiple thread
size=int(len(vectors)/len(threads))

def compare(v1,v2):
    v1_excluded=v1[1:]
    similarity=1-cosine(v1_excluded,v2)
    return similarity

def top_k(data, result, id, start, end):
    for i in range(start, end):
        computation=compare(vectors[i],query_vector[0])
        result[id].put((computation,int(vectors[i][0])))
        if(result[id].qsize()>k):
            result[id].get()

for i in range(len(threads)):
    start = i*size
    end = (i+1)*size
    threads[i] = Thread(target=top_k, args=(vectors, result, i, start, end))
    threads[i].start()

for i in range(len(threads)):
    threads[i].join()
#waits until all threads completes their work

final_result=queue.PriorityQueue()#it will contain top k elements coming from all the threads
for i in range(len(threads)):
    while not result[i].empty():
        item=result[i].get()
        final_result.put(item)
        if(final_result.qsize()>k):
            final_result.get()

max_heap=queue.PriorityQueue()#to reverse the order of final_result
while not final_result.empty():
    item=final_result.get()
    max_heap.put((-item[0],item[1]))

while not max_heap.empty():
    item=max_heap.get()
    print(item[1],-item[0])
    
# python assignment-1-21EE10054.py data.txt Test.txt 8 5
