import pymongo
import json
import sys
import settings
import tempfile
from mapreduce_item_item import SimilaritiesJob
import os

mongo = pymongo.MongoClient()
mongo_yelp = mongo.yelp
mongo_users = mongo_yelp.users
mongo_businesses = mongo_yelp.businesses

TARGET = open(sys.argv[1],"r")

for line in TARGET:
    settings.init()
    target = json.loads(line)
    target_user_ratings = dict()

    settings.target_user_id = target["user_id"]
    settings.target_business_id = target["business_id"]

    user_record = mongo_users.find({"user_id":target["user_id"]})[0]
    TEMPFILE = tempfile.NamedTemporaryFile()

    for item in user_record["ratings"] + [{"business_id":settings.target_business_id}]:
        
        if "stars" in item:
            target_user_ratings[item["business_id"]] = item["stars"] 
        business_record = mongo_businesses.find({"business_id":item["business_id"]})[0]
        settings.sum_of_squares[business_record["business_id"]] = business_record["sum_of_squares"]
        for user in business_record["ratings"]:
            TEMPFILE.write(json.dumps({"user_id":user["user_id"],"business_id":item["business_id"],"stars":user["stars"]})+"\n")
            
    TEMPFILE.flush()
    os.fsync(TEMPFILE)

    mr_job = SimilaritiesJob(args=[TEMPFILE.name])
    with mr_job.make_runner() as runner:
        runner.run()
        sum_of_similarities = 0
        weighted_sum_of_ratings = 0
        for line in runner.stream_output():
            columns = line.split('\t')
            sum_of_similarities += float(columns[1])
            weighted_sum_of_ratings += target_user_ratings[json.loads(columns[0])[1]]*float(columns[1])
        
        if sum_of_similarities > 0:
            print weighted_sum_of_ratings/sum_of_similarities,target["stars"],abs(weighted_sum_of_ratings/sum_of_similarities-target["stars"])
        else:
            for item in target_user_ratings:
                sum_of_similarities += 1
                weighted_sum_of_ratings += target_user_ratings[item]
            
            print weighted_sum_of_ratings/sum_of_similarities,target["stars"],abs(weighted_sum_of_ratings/sum_of_similarities-target["stars"])


    TEMPFILE.close()
