import sys
import threading
import numpy as np
from queue import PriorityQueue

def read_data(file_path):
    """Read item vectors from the file, assuming space-delimited format."""
    with open(file_path, 'r') as file:
        data = [line.strip().split(' ') for line in file]
    # Convert to dict with item id as key and vector as value
    items = {row[0]: np.array(row[1:], dtype=float) for row in data}
    return items

def read_query(file_path):
    """Read the query vector from the file, assuming space-delimited format."""
    with open(file_path, 'r') as file:
        query = np.array(file.readline().strip().split(' '), dtype=float)
    return query

def cosine_similarity(vec1, vec2):
    """Compute the cosine similarity between two vectors."""
    dot_product = np.dot(vec1, vec2)
    norm_vec1 = np.linalg.norm(vec1)
    norm_vec2 = np.linalg.norm(vec2)
    similarity = dot_product / (norm_vec1 * norm_vec2)
    return similarity

def process_chunk(item_ids, items, query_vec, top_k_queue):
    """Process a chunk of items to compute similarities and update the top-k queue."""
    for item_id in item_ids:
        sim = cosine_similarity(query_vec, items[item_id])
        # Negative similarity for reverse sorting in PriorityQueue
        top_k_queue.put((-sim, item_id))

def main(data_file, query_file, num_threads, k):
    items = read_data(data_file)
    query_vec = read_query(query_file)
    top_k_queue = PriorityQueue()

    # Split item ids for threading
    item_ids = list(items.keys())
    chunk_size = len(item_ids) // num_threads
    threads = []

    for i in range(num_threads):
        start = i * chunk_size
        end = start + chunk_size if i < num_threads - 1 else len(item_ids)
        thread = threading.Thread(target=process_chunk, args=(item_ids[start:end], items, query_vec, top_k_queue))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    # Collect and print top-k results
    results = []
    while not top_k_queue.empty() and len(results) < k:
        sim, item_id = top_k_queue.get()
        results.append((item_id, -sim))

    # Sort results based on similarity
    results.sort(key=lambda x: x[1], reverse=True)
    for item_id, sim in results:
        print(f"{item_id} {sim}")

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: python <script.py> <data file> <query file> <# threads> <value of k>")
        sys.exit(1)
    data_file, query_file, num_threads, k = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
    main(data_file, query_file, num_threads, k)
