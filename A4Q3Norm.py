############################ Import Dependencies ############################
from pymongo import MongoClient
import sys
#############################################################################

############################ Class ############################
class SongDatabase:
    def __init__(self, database_name, port_number):
        self.client = MongoClient('localhost', port_number)
        self.db = self.client[database_name]
    
    def connect_collection(self, collection_name):
        return self.db[collection_name]

    def sum_of_recording_lengths(self, collection):
        pass
###############################################################

############################ Styles ##########################
if __name__ == "__main__":
    pass
##############################################################