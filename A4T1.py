############################ Import Dependencies ############################
from pymongo import MongoClient 
import json
import sys
from bson import json_util
############################################################################

################################## Class ##################################
class Database:
    def __init__(self, database_name, port_number):
        self.client = MongoClient('localhost', port_number)
        self.db = self.client[database_name]
    
    def create_collection(self, collection_name):
        new_collection = self.db[collection_name]
        new_collection.delete_many({})
        return new_collection

    def fill_collection(self, collection, json_filename):
        with open(json_filename, 'r') as f:
            data = json.load(f)
            f.close()
        for item in data:
            item['_id'] = json_util.loads(json_util.dumps(item['_id']))
            if item.get('issue_date'):
                item['issue_date'] = json_util.loads(json_util.dumps(item['issue_date']))
            collection.insert_one(item)
    
    def close(self):
        self.client.close()
###########################################################################

################################## Main ##################################
if __name__ == "__main__":
    port_number = int(sys.argv[1])
    A4dbNorm = Database("A4dbNorm", port_number)
    songwriters = A4dbNorm.create_collection("songwriters")
    A4dbNorm.fill_collection(songwriters, "songwriters.json")
    recordings = A4dbNorm.create_collection("recordings")
    A4dbNorm.fill_collection(recordings, "recordings.json")
    A4dbNorm.close()
###########################################################################