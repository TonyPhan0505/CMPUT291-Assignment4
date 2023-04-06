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
    
    def solve_A4Q2Embed(self):

        pipeline = [
            {
                '$unwind': "$recordings"
            },

            {
                '$match': {
                    'recordings.recording_id': {'$regex': '^70'}
                }
            },

            {
                '$group': {
                    '_id': None, 
                    'avg_rhythmicality': {'$avg': '$recordings.rhythmicality'}
                    }
            },

            {
                '$project': {
                    '_id': '',
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