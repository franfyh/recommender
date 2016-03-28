from mrjob.job import MRJob
import json
from math import sqrt
from mrjob.step import MRStep
import settings

class SimilaritiesJob(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper=self.group_by_user,
                   reducer=self.assemble_user_ratings),
            MRStep(mapper=self.pairwise_businesses,
                   reducer=self.calculate_similarity)]

    def group_by_user(self, key, line):
        temp_input = json.loads(line)

        yield temp_input["user_id"],(temp_input["business_id"],temp_input["stars"])


    def assemble_user_ratings(self, user_id, values):
        final = []

        for business_id, rating in values:
            final.append((business_id, rating))

        yield user_id, final

    def pairwise_businesses(self, user_id, values):
        temp_target_rating = 0
        found_target_business = False
                            
        for item in values:
            if item[0]==settings.target_business_id:

                found_target_business = True
                temp_target_rating = item[1]
                break
                
        if found_target_business:
            for item in values:
                if not item[0]==settings.target_business_id:
                    yield (settings.target_business_id, item[0]),(temp_target_rating,item[1])

    def calculate_similarity(self, pair_key, values):
        nominator = 0
        
        for ratings in values:
            nominator += ratings[0]*ratings[1]

        yield pair_key, nominator/sqrt(settings.sum_of_squares[pair_key[0]]*settings.sum_of_squares[pair_key[1]])
        
    
