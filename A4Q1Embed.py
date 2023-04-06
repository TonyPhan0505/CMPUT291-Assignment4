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
    
    def solve_A4Q1Embed(self):
        
        pipeline = [
           
            
            {
                '$unwind': '$recordings'
            },

            {
                '$match': {
                    'matched_recordings': {
                        '$ne': []
                    }
                }
            },
            
            {
                '$group': {
                    '_id': {
                        '_id': '$_id',
                        'songwriter_id': '$songwriter_id',
                        'name': '$name'
                    },
                    'num_recordings': {
                        '$count': {}
                    }
                }
            },

            {
                '$project': {
                    '_id': '$_id._id',
                    'songwriter_id': '$_id.songwriter_id',
                    'name': '$_id.name',
                    'num_recordings': 1
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
    A4dbEmbed.solve_A4Q1Embed()
    A4dbEmbed.close()
##############################################################