from pymongo import MongoClient
import time

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["test"]
tasks_collection = db["tasks"]


# Insert a queued entry
for j in range(5):
    for i in range(3):
        task_data = {
            "task_name": f"Sample Task {j*3 + i+1}",
            "status": "queued",
            "locked": False
        }

        tasks_collection.insert_one(task_data)
        print(f"Queued entry {j*3 + i+1} added successfully.")

    # Wait for 2 seconds before the next batch insertion
    time.sleep(2)
