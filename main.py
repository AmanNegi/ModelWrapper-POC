import concurrent.futures
import time
from pymongo import MongoClient  
from random import sample

# MongoDB connection
client = MongoClient('mongodb://localhost:27017/')
db = client['test']
tasks_collection = db['tasks']
workers_collection = db['workers']

# Function to process a task
def process_task(task, idle_worker):
    # Simulating task processing
    time.sleep(3)
    # Update task status to completed
    tasks_collection.update_one({'_id': task['_id']}, {
                                '$set': {'status': 'completed'}})
    # Update worker status to idle
    workers_collection.update_one({'worker_id': idle_worker["worker_id"]}, {
                                  '$set': {'status': 'idle'}})
    print("Task completed by worker", idle_worker["worker_id"])

# Worker function
def create_worker(worker_id):
    print("Worker created")
    print(worker_id)

    # Check if a worker with the given worker_id already exists
    existing_worker = workers_collection.find_one({'_id': worker_id})

    # If no such worker exists, insert a new one
    if existing_worker is None:
        workers_collection.insert_one(
            {'_id': worker_id, 'worker_id': worker_id, 'status': 'idle'})


# Limit/Increase the number of workers from here
for i in range(4):
    worker_id = f'worker_{i}'
    create_worker(worker_id)

# Create worker thread pool
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
    while True:
        new_tasks = tasks_collection.find(
            {'status': 'queued', 'locked': False})
        idle_workers = list(workers_collection.find(
            {'status': 'idle'}, {'_id': 1, 'worker_id': 1}))
        if idle_workers:
            for task in new_tasks:
                if not idle_workers:
                    break
                idle_worker = sample(idle_workers, 1)[0]
                idle_workers.remove(idle_worker)
                # Lock the task
                tasks_collection.update_one({'_id': task['_id']}, {'$set': {
                                            'locked': True, 'status': 'working', 'assigned_to': idle_worker['worker_id']}})
                # Update worker status to working
                workers_collection.update_one({'worker_id': idle_worker['worker_id']}, {
                                              '$set': {'status': 'working'}})
                print("Task assigned to worker", idle_worker['worker_id'])
                # Process the task using ThreadPoolExecutor
                executor.submit(process_task, task, idle_worker)
        else:
            print("No idle workers available.")
        # Currently using polling, will be replaced using mongodb change streams
        time.sleep(3)
