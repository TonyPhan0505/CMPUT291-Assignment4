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
    
    def solve_A4Q4Embed(self):
        '''
            1. Unwind the recordings array field so that 1 songwriter document has only 1 recording document attached to it.
            2. Filter the songwriter documents so that only those with a recording after datetime.datetime(1950, 1, 1) can go through to the next stage.
            3. Group by recordings._id, recordings.name, SongwritersRecordings.name, recordings.issue_date
            4. Project recordings._id AS _id, recordings.name AS r_name, songwriters.name AS name, recordings.issue_date AS r_issue_date
        '''

        pipeline = [
            {
                "$unwind": "$recordings"
            },

            {
                "$match": {
                    "recordings.issue_date": {"$gt": datetime.datetime(1950, 1, 1)}
                }
            },

            {
                "$group": {
                    "_id": {
                        "_id": "$recordings._id",
                        "r_name": "$recordings.name",
                        "name": "$name",
                        "r_issue_date": "$recordings.issue_date"
                    }
                }
            },

            {
                "$project": {
                    "_id": "$_id._id",
                    "r_name": "$_id.r_name",
                    "name": "$_id.name",
                    "r_issue_date": "$_id.r_issue_date",
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
    A4dbEmbed.solve_A4Q4Embed()
    A4dbEmbed.close()
##############################################################