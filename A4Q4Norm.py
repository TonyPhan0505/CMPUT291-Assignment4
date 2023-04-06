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

    def solve_A4Q4Norm(self):
        '''
            SQL:
                SELECT recordings._id AS _id, recordings.name AS r_name, songwriters.name AS name, recordings.issue_date AS r_issue_date
                FROM recordings, songwriters
                WHERE 
                    (recordings.recording_id IN songwriters.recordings) 
                        AND 
                    (recordings.issue_date > datetime.datetime(1950,1,1))
                GROUP BY (
                    recordings._id, 
                    recordings.name, 
                    songwriters.name, 
                    recordings.issue_date
                );
        '''
        pipeline = [
            {
                "$lookup": {
                    "from": "songwriters",
                    "localField": "recording_id",
                    "foreignField": "recordings",
                    "as": "matched_songwriters"
                }
            },

            {
                "$unwind": "$matched_songwriters"
            },

            {
                "$match": {
                    "issue_date": {"$gt": datetime.datetime(1950, 1, 1)}
                }
            },

            {
                "$group": {
                    "_id": {
                        "_id": "$_id",
                        "r_name": "$name",
                        "name": "$matched_songwriters.name",
                        "r_issue_date": "$issue_date"
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
    A4dbNorm.solve_A4Q4Norm()
    A4dbNorm.close()
##############################################################