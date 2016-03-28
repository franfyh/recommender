import pymongo
import json

INSIGHT = "/scratch/yfu7/Projects/Insight/"

mongo = pymongo.MongoClient()
mongo_yelp = mongo.yelp
mongo_users = mongo_yelp.users
mongo_businesses = mongo_yelp.businesses

REVIEW = open(INSIGHT+"Data/yelp_las_vegas_training.json")

mongo_users.create_index("user_id",unique=True)
mongo_businesses.create_index("business_id",unique=True)

mongo_users.create_index("number_of_ratings")
mongo_businesses.create_index("number_of_ratings")


for line in REVIEW:
    review = json.loads(line)
    mongo_users.update({"user_id":review["user_id"]},{"$push":{"ratings":{"business_id":review["business_id"],"stars":review["stars"]}}},upsert=True)
    mongo_businesses.update({"business_id":review["business_id"]},{"$push":{"ratings":{"user_id":review["user_id"],"stars":review["stars"]}}},upsert=True)
    mongo_users.update({"user_id":review["user_id"]},{"$inc":{"sum_of_squares":review["stars"]**2}})
    mongo_businesses.update({"business_id":review["business_id"]},{"$inc":{"sum_of_squares":review["stars"]**2}})
    mongo_users.update({"user_id":review["user_id"]},{"$inc":{"number_of_ratings":1}})
    mongo_businesses.update({"business_id":review["business_id"]},{"$inc":{"number_of_ratings":1}})


REVIEW.close()

print mongo_users.find_one()
print mongo_businesses.find_one()

