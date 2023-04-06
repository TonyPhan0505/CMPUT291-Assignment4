############################ Import Dependencies ############################
from pymongo import MongoClient
import sys
#############################################################################

############################ Class ############################
class SongDatabase:
    def __init__(self, database_name, port_number):
        self.client = MongoClient('localhost', port_number)
        self.db = self.client[database_name]
    
    def solve_A4Q3Norm(self):
        '''
            1. Join the songwriters and recordings tables WHERE songwriters.songwriter_id IN recordings.songwriter_ids
            2. Destructure each songwriter document based on the array field "matched_recordings". Now, each songwriter document has a field "matched_recording" whose value is a recording that belongs to that songwriter.
            3. Group the resultant songwriter documents by songwriters._id and songwriters.songwriter_id
            4. Sum the lengths of recordings in each group up and name the sum "total_length". "total_length" is attached to each group.
            5. SELECT _id, total_length, songwriters.songwriter_id
        '''
        pipeline = [
            {
                "$lookup": {
                    "from": "recordings",
                    "localField": "songwriter_id",
                    "foreignField": "songwriter_ids",
                    "as": "matched_recordings"
                }
            },

            {
                "$unwind": "$matched_recordings"
            },

            {
                "$group": {
                    "_id": {
                        "_id": "$_id",
                        "songwriter_id": "$songwriter_id"
                    },
                    "total_length": {
                        "$sum": "$matched_recordings.length"
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
        result = self.db.songwriters.aggregate(pipeline)
        for songwriter in result:
            print(songwriter)
    
    def close(self):
        self.client.close()
###############################################################

############################ Styles ##########################
if __name__ == "__main__":
    port_number = int(sys.argv[1])
    A4dbNorm = SongDatabase("A4dbNorm", port_number)
    A4dbNorm.solve_A4Q3Norm()
    A4dbNorm.close()
##############################################################