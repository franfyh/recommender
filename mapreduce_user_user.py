from mrjob.job import MRJob
import json
from math import sqrt
from mrjob.step import MRStep
import settings

class SimilaritiesJob(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper=self.group_by_business,
                   reducer=self.assemble_business_ratings),
            MRStep(mapper=self.pairwise_users,
                   reducer=self.calculate_similarity)]

    def group_by_business(self, key, line):
        temp_input = json.loads(line)

        yield temp_input["business_id"],(temp_input["user_id"],temp_input["stars"])


    def assemble_business_ratings(self, business_id, values):
        final = []

        for user_id, rating in values:
            final.append((user_id, rating))

        yield business_id, final

    def pairwise_users(self, business_id, values):
        temp_target_rating = 0
        found_target_user = False
                            
        for item in values:
            if item[0]==settings.target_user_id:

                found_target_user = True
                temp_target_rating = item[1]
                break
                
        if found_target_user:
            for item in values:
                if not item[0]==settings.target_user_id:
                    yield (settings.target_user_id, item[0]),(temp_target_rating,item[1])

    def calculate_similarity(self, pair_key, values):
        nominator = 0
        
        for ratings in values:
            nominator += ratings[0]*ratings[1]

        yield pair_key, nominator/sqrt(settings.sum_of_squares[pair_key[0]]*settings.sum_of_squares[pair_key[1]])
        
    
