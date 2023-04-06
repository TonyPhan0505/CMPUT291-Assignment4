############################ Import Dependencies ############################
from pymongo import MongoClient
import sys
#############################################################################

############################ Class ############################
class SongDatabase:
    def __init__(self, database_name, port_number):
        self.client = MongoClient('localhost', port_number)
        self.db = self.client[database_name]

    def solve_A4Q2Norm(self):

        pipeline = [
            {
                '$match': {
                    'recording_id': {'$regex': '^70'}
                }
            },

            {
                '$group': {
                    '_id': None, 
                    'avg_rhythmicality': {'$avg': '$rhythmicality'}
                    }
            },

            {
                '$project': {
                    '_id': '',
                    'avg_rhythmicality': 1
                }
            }
        ]
        result = self.db.recordings.aggregate(pipeline)
        for recording in result:
            print(recording)

    def close(self):
        self.client.close()
###############################################################

############################ Styles ##########################
if __name__ == "__main__":
    port_number = int(sys.argv[1])
    A4dbNorm = SongDatabase("A4dbNorm", port_number)
    A4dbNorm.solve_A4Q2Norm()
    A4dbNorm.close()
##############################################################