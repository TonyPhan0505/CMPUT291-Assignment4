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
        '''
            Just like in A4Q3Norm.py except we don't have to join the documents from 2 tables anymore.
        '''
        pipeline = [
            {
                '$unwind': "$songwriters"
            },

            {
                '$match': {
                    'songwriters': {
                        '$ne': []
                    }
                }
            },

            {
                '$project': {
                    "recording_id": '$_id',
                    "num_recordings": {
                        '$map': {
                            'input': '$num_recordings',
                            'in': {
                                'toInt': '$$this'
                            }
                        }
                    }
                }
            },
            
            {
                '$unwind': '$num_recordings'
            },

            {
                '$group': {
                    '_id': {
                        'songwriter_id': '$songwriters._id',
                    },
                    'name': {
                        '$first': '$songwriters.name'
                    },
                    'num_recordings': {
                        '$sum': "$num_recordings"
                    }
                }
            },

            {
                '$project': {
                    '_id': 0,
                    'songwriter_id': '$_id.songwriter_id',
                    'name': 1,
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