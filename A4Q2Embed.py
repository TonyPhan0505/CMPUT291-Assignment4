############################ Import Dependencies ############################
from pymongo import MongoClient
import sys
import datetime
#############################################################################

############################ Class ############################
class SongDatabase:
    def __init__(self, database_name, port_number):
        self.client = MongoClient('localhost', port_number)
        self.db = self.client[database_name]
    
    def close(self):
        self.client.close()
    
    def solve_A4Q2Embed(self):

        pipeline = [
            {
                '$match': {
                    '_id': "/^70/"
                }
            },

            {
                '$unwind': "$songwriters"
            },

            {
                '$group': {
                    '_id': "$_id",
                    'avg_rhythmicality': {
                        '$avg': "$rhythmicality"
                    }
                }
            },

            {
                '$project': {
                    '_id': 1,
                    'avg_rhythmicality': 1
                }
            }
        ]
        result = self.db.SongwritersRecordings.aggregate(pipeline)
        for recording in result:
            print(recording)
###############################################################

############################ Styles ##########################
if __name__ == "__main__":
    port_number = int(sys.argv[1])
    A4dbEmbed = SongDatabase("A4dbEmbed", port_number)
    A4dbEmbed.solve_A4Q2Embed()
    A4dbEmbed.close()
##############################################################