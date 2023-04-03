############################ Import Dependencies ############################
from pymongo import MongoClient 
import json
############################################################################

################################## Class ##################################
class Database:
    def __init__(self, database_name):
        self.client = MongoClient('localhost', 71072)
        self.db = self.client[database_name]
    
    def create_collection(self, collection_name):
        new_collection = self.db[collection_name]
        new_collection.delete_many({})
        return new_collection

    def fill_collection(self, collection, json_filename):
        with open(json_filename, 'r') as f:
            data = json.load(f)
            f.close()
        collection.insert_many(data)
###########################################################################

################################## Main ##################################
if __name__ == "__main__":
    A4dbNorm = Database("A4dbNorm")
    songwriters = A4dbNorm.create_collection("songwriters")
    A4dbNorm.fill_collection(songwriters, "songwriters.json")
    recordings = A4dbNorm.create_collection("recordings")
    A4dbNorm.fill_collection(recordings, "recordings.json")
###########################################################################