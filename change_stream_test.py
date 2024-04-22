import threading
import time
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017")
db = client["test"]
collection = db["changetest"]
task_collection = db["tasks"]

# Worker class
class Worker(threading.Thread):
    def __init__(self, worker_id):
        threading.Thread.__init__(self)
        self.worker_id = worker_id
        self.status = "idle"

    def run(self):
        while True:
            if self.status == "idle":
                task = get_available_task()
                if task:
                    self.status = "working"
                    process_task(task)
                    self.status = "completed"
                    task_collection.update_one({"_id": task["_id"]}, {"$set": {"status": "completed"}})
                    print(f"Worker {self.worker_id} completed task {task['_id']}")
            else:
                print(f"Worker {self.worker_id} is busy")
                time.sleep(1)

def get_available_task():
    task = task_collection.find_one({"status": "queued"})
    if task:
        task_collection.update_one({"_id": task["_id"]}, {"$set": {"status": "working"}})
    return task

def process_task(task):
    # Your task processing logic here
    print(f"Processing task {task['_id']}")

# Start workers
workers = [Worker(i) for i in range(5)]
for worker in workers:
    worker.start()

# MongoDB change stream listener
change_stream = collection.watch()
for change in change_stream:
    if change["operationType"] == "insert":
        print(f"New document inserted: {change['fullDocument']}")
        assign_task_to_worker(change['fullDocument'])

# Function to assign tasks to available workers
def assign_task_to_worker(task):
    for worker in workers:
        if worker.status == "idle":
            worker.status = "queued"
            task_collection.insert_one({"_id": task["_id"], "status": "queued"})
            break