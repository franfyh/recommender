import pymongo
import json
import sys
import settings
import tempfile
from mapreduce_user_user import SimilaritiesJob
import os

mongo = pymongo.MongoClient()
mongo_yelp = mongo.yelp
mongo_users = mongo_yelp.users
mongo_businesses = mongo_yelp.businesses

TARGET = open(sys.argv[1],"r")

for line in TARGET:
    settings.init()
    target = json.loads(line)
    target_business_ratings = dict()

    settings.target_user_id = target["user_id"]
    settings.target_business_id = target["business_id"]

    business_record = mongo_businesses.find({"business_id":target["business_id"]})[0]
    TEMPFILE = tempfile.NamedTemporaryFile()
#    TEMPFILE = open("./temp","w")
    for item in business_record["ratings"] + [{"user_id":settings.target_user_id}]:
        if "stars" in item:
            target_business_ratings[item["user_id"]] = item["stars"]
            
        user_record = mongo_users.find({"user_id":item["user_id"]})[0]
        settings.sum_of_squares[user_record["user_id"]] = user_record["sum_of_squares"]
        for business in user_record["ratings"]:
            TEMPFILE.write(json.dumps({"business_id":business["business_id"],"user_id":item["user_id"],"stars":business["stars"]})+"\n")
            
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
            weighted_sum_of_ratings += target_business_ratings[json.loads(columns[0])[1]]*float(columns[1])
        
        if sum_of_similarities > 0:
            print weighted_sum_of_ratings/sum_of_similarities,target["stars"],abs(weighted_sum_of_ratings/sum_of_similarities-target["stars"])
        else:
            for item in target_business_ratings:
                sum_of_similarities += 1
                weighted_sum_of_ratings += target_business_ratings[item]
            
            print weighted_sum_of_ratings/sum_of_similarities,target["stars"],abs(weighted_sum_of_ratings/sum_of_similarities-target["stars"])



    TEMPFILE.close()

