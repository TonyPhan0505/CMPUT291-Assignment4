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

    def get_data(self, json_filename):
        with open(json_filename, 'r') as f:
            data = json.load(f)
            f.close()
        return data

    def find_record(self, data, PK, id):
        for item in data:
            if item[PK] == id:
                item['issue_date'] = json_util.loads(json_util.dumps(item['issue_date']))
                return item
        return None

    def create_merged_data(self, parent_json_filename, child_json_filename, child_PK, child_FK):
        parent_data = self.get_data(parent_json_filename)
        child_data = self.get_data(child_json_filename)
        for parent in parent_data:
            parent[child_FK] = list(
                map(
                    lambda child_id: self.find_record(child_data, child_PK, child_id), 
                    parent[child_FK]
                )
            )
        return parent_data

    def fill_collection(self, collection, data):
        for item in data:
            item['_id'] = json_util.loads(json_util.dumps(item['_id']))
            collection.insert_one(item)
    
    def close(self):
        self.client.close()
###########################################################################

################################## Main ##################################
if __name__ == "__main__":
    port_number = int(sys.argv[1])
    A4dbEmbed = Database("A4dbEmbed", port_number)
    merged_data = A4dbEmbed.create_merged_data(
        'songwriters.json', 
        'recordings.json', 
        'recording_id',
        'recordings'
    )
    SongwritersRecordings = A4dbEmbed.create_collection('SongwritersRecordings')
    A4dbEmbed.fill_collection(SongwritersRecordings, merged_data)
    A4dbEmbed.close()
###########################################################################