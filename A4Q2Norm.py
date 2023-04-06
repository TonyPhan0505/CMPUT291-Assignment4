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

    def solve_A4Q2Norm(self):
        '''Write a query to get the average rhythmicality for all recordings that have a recording_id beginning with “70”'''
        '''
            SQL:
                SELECT recordings._id AS _id, AVERAGE(recordings.rhythmicality) AS avg_rhythmicality
                FROM recordings, songwriters
                WHERE 
                    (recordings.recording_id IN songwriters.recordings) 
                        AND 
                    (recordings.recording_id LIKE "70%");
        '''
        '''Match recordings with a recording_id beginning with “70”'''
        '''Look up songwriters collection to get associated recordings'''
        '''Unwind the songwriters array'''
        '''Group by recording_id and calculate the average rhythmicality'''
        '''Project output fields'''
        pipeline = [
            {
                '$match': {
                    '_id': "/^70/"
                }
            },

            {
                '$lookup': {
                    'from': 'songwriters',
                    'localField': 'recording_id',
                    'foreignField': 'recordings',
                    'as': "songwriters"
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