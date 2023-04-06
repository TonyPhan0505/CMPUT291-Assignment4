############################ Import Dependencies ############################
from pymongo import MongoClient
import sys
#############################################################################

############################ Class ############################
class SongDatabase:
    def __init__(self, database_name, port_number):
        self.client = MongoClient('localhost', port_number)
        self.db = self.client[database_name]
    
    def close(self):
        self.client.close()
    
    def solve_A4Q3Embed(self):
        '''
            Just like in A4Q3Norm.py except we don't have to join the documents from 2 tables anymore.
        '''
        pipeline = [
            {
                "$unwind": "$recordings"
            },

            {
                "$group": {
                    "_id": {
                        "_id": "$_id",
                        "songwriter_id": "$songwriter_id"
                    },
                    "total_length": {
                        "$sum": "$recordings.length"
                    }
                }
            },

            {
                "$project": {
                    "_id": "$_id._id",
                    "total_length": 1,
                    "songwriter_id": "$_id.songwriter_id"
                }
            }
        ]
        result = self.db.SongwritersRecordings.aggregate(pipeline)
        for songwriter in result:
            print(songwriter)
###############################################################

############################ Styles ##########################
if __name__ == "__main__":
    port_number = int(sys.argv[1])
    A4dbEmbed = SongDatabase("A4dbEmbed", port_number)
    A4dbEmbed.solve_A4Q3Embed()
    A4dbEmbed.close()
##############################################################