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
    
    def solve_A4Q1Norm(self):
        '''
            SQL:
                SELECT recordings._id AS _id, songwriters._id AS songwriter_id, songwriters.name AS name, count(songwriters.recordings) as num_recordings
                FROM songwriters, recordings
                WHERE 
                    (recordings.recording_id IN songwriters.recordings) 
                        AND 
                    (num_recordings > 0)
                GROUP BY (
                    recordings._id, 
                    songwriters._id, 
                    songwriters.name
                );
        '''
        '''Join with songwriters collection
        Filter out recordings without any associated songwriters
        Project the recording_id and the transformed num_recordings array
        Unwind the num_recordings array
        Group by songwriter_id and count the number of recordings
        Project the output fields and rename the _id as songwriter_id'''

        pipeline = [
            
            {
                '$lookup': {
                    'from': 'songwriters',
                    'localField': 'recording_id',
                    'foreignField': 'recordings',
                    'as': "songwriters"
                }
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

        result = self.db.songwriters.aggregate(pipeline)
        for r in result:
            print(r)

    

###############################################################

############################ Styles ##########################
if __name__ == "__main__":
    port_number = int(sys.argv[1])
    A4dbNorm = SongDatabase("A1dbNorm", port_number)
    A4dbNorm.solve_A4Q1Norm()
    A4dbNorm.close()
##############################################################