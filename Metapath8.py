## D ---> PR ---> PR Review ---> Developer

from pymongo import MongoClient
from HRanking import HRank
from Requirements import smartshark

class metapath_4():
    
    def __init__(self):
        
        self.project = "giraph"
        
        self.client = MongoClient("mongodb://localhost:27017/")
        self.db = self.client["smartshark"]       
        
        target_repo = "https://github.com/apache/" + self.project  
        
        self.pull_requests = self.db["pull_request"]
        self.pull_requests_list = list(self.pull_requests.find({"creator_id": {"$exists": True}}))
        self.pull_request_reviews = self.db["pull_request_review"]
        self.reviews_list = list(self.pull_request_reviews.find({"creator_id": {"$exists": True}}))
                
    def metapath_4_req_1(self, mean_time):
        
        prs_list = STQ.find_less_than_rejected_prs(mean_time)
                
        metapaths = []
        
        for review in self.reviews_list:
            
            reviewer_id = str(review["creator_id"])
            review_id = str(review["_id"])
                        
            for pr in prs_list:
                if(str(review["pull_request_id"]) == pr):
                                        
                    for pr_info in self.pull_requests_list:
                        
                        if(str(pr_info["_id"])==pr):
                            
                            metapaths.append(
                                {
                                    "developer": str(pr_info["creator_id"]),
                                    "pull_request": pr,
                                    "pull_request_review": review_id,
                                    "reviewer": reviewer_id
                                }
                            )
        
        return metapaths

    def metapath_4_req_2(self, mean_time):
        
        prs_list = STQ.find_more_than_rejected_prs(mean_time)
                
        metapaths = []
        
        for review in self.reviews_list:
            
            reviewer_id = str(review["creator_id"])
            review_id = str(review["_id"])
                        
            for pr in prs_list:
                if(str(review["pull_request_id"]) == pr):
                                        
                    for pr_info in self.pull_requests_list:
                        
                        if(str(pr_info["_id"])==pr):
                            
                            metapaths.append(
                                {
                                    "developer": str(pr_info["creator_id"]),
                                    "pull_request": pr,
                                    "pull_request_review": review_id,
                                    "reviewer": reviewer_id
                                }
                            )
        
        return metapaths
    
    def metapath_4_req_3(self, mean_time):
        
        prs_list = STQ.find_less_than_all_prs(mean_time)
                
        metapaths = []
        
        for review in self.reviews_list:
            
            reviewer_id = str(review["creator_id"])
            review_id = str(review["_id"])
                        
            for pr in prs_list:
                if(str(review["pull_request_id"]) == pr):
                                        
                    for pr_info in self.pull_requests_list:
                        
                        if(str(pr_info["_id"])==pr):
                            
                            metapaths.append(
                                {
                                    "reviewer": reviewer_id,
                                    "pull_request_review": review_id,
                                    "pull_request": pr,
                                    "developer": str(pr_info["creator_id"]),
                                }
                            )
        
        return metapaths
    
    def metapath_4_req_4(self, mean_time):
        
        prs_list = STQ.find_more_than_all_prs(mean_time)
                
        metapaths = []
        
        for review in self.reviews_list:
            
            reviewer_id = str(review["creator_id"])
            review_id = str(review["_id"])
                        
            for pr in prs_list:
                if(str(review["pull_request_id"]) == pr):
                                        
                    for pr_info in self.pull_requests_list:
                        
                        if(str(pr_info["_id"])==pr):
                            
                            metapaths.append(
                                {
                                    "reviewer": reviewer_id,
                                    "pull_request_review": review_id,
                                    "pull_request": pr,
                                    "developer": str(pr_info["creator_id"]),
                                }
                            )
        
        return metapaths
    
    def metapath_4_req_5(self, mean_time):
        
        prs_list = STQ.find_less_than_accepted_prs(mean_time)
                
        metapaths = []
        
        for review in self.reviews_list:
            
            reviewer_id = str(review["creator_id"])
            review_id = str(review["_id"])
                        
            for pr in prs_list:
                if(str(review["pull_request_id"]) == pr):
                                        
                    for pr_info in self.pull_requests_list:
                        
                        if(str(pr_info["_id"])==pr):
                            
                            metapaths.append(
                                {
                                    "reviewer": reviewer_id,
                                    "pull_request_review": review_id,
                                    "pull_request": pr,
                                    "developer": str(pr_info["creator_id"]),
                                }
                            )
        
        return metapaths
    
    def metapath_4_req_6(self, mean_time):
        
        prs_list = STQ.find_more_than_accepted_prs(mean_time)
                
        metapaths = []
        
        for review in self.reviews_list:
            
            reviewer_id = str(review["creator_id"])
            review_id = str(review["_id"])
                        
            for pr in prs_list:
                if(str(review["pull_request_id"]) == pr):
                                        
                    for pr_info in self.pull_requests_list:
                        
                        if(str(pr_info["_id"])==pr):
                            
                            metapaths.append(
                                {
                                    "reviewer": reviewer_id,
                                    "pull_request_review": review_id,
                                    "pull_request": pr,
                                    "developer": str(pr_info["creator_id"]),
                                }
                            )
        
        return metapaths
    
    def metapath_4_req_7(self, mean_time):
        
        prs_list = STQ.find_less_than_rejected_prs(mean_time)
                
        metapaths = []
        
        for review in self.reviews_list:
            
            reviewer_id = str(review["creator_id"])
            review_id = str(review["_id"])
                        
            for pr in prs_list:
                if(str(review["pull_request_id"]) == pr):
                                        
                    for pr_info in self.pull_requests_list:
                        
                        if(str(pr_info["_id"])==pr):
                            
                            metapaths.append(
                                {
                                    "reviewer": reviewer_id,
                                    "pull_request_review": review_id,
                                    "pull_request": pr,
                                    "developer": str(pr_info["creator_id"]),
                                }
                            )
        
        return metapaths

    def metapath_4_req_8(self, mean_time):
        
        prs_list = STQ.find_more_than_rejected_prs(mean_time)
                
        metapaths = []
        
        for review in self.reviews_list:
            
            reviewer_id = str(review["creator_id"])
            review_id = str(review["_id"])
                        
            for pr in prs_list:
                if(str(review["pull_request_id"]) == pr):
                                        
                    for pr_info in self.pull_requests_list:
                        
                        if(str(pr_info["_id"])==pr):
                            
                            metapaths.append(
                                {
                                    "reviewer": reviewer_id,
                                    "pull_request_review": review_id,
                                    "pull_request": pr,
                                    "developer": str(pr_info["creator_id"]),
                                }
                            )
        
        return metapaths
                                    
if __name__ == "__main__":
    
    STQ = smartshark()
    query = metapath_4()
    mean_time = STQ.find_mean_rejected_prs()
    # print(mean_time)
    
    # RQ1
    print("Requirement 1 for Metapath 4:\n")
    req1 = query.metapath_4_req_1(mean_time)  
    # Calling HRanking Function
    HRank.perform_asymmetric(req1)
    print()
    
    # RQ2
    print("Requirement 2 for Metapath 4:\n")    
    req2 = query.metapath_4_req_2(mean_time)
    # Calling HRanking Function
    HRank.perform_asymmetric(req2)
    print()

    # RQ3
    print("Requirement 3 for Metapath 4:\n")    
    req3 = query.metapath_4_req_3(mean_time)
    # Calling HRanking Function
    HRank.perform_asymmetric(req3)
    print()
    
    # RQ4
    print("Requirement 4 for Metapath 4:\n")    
    req4 = query.metapath_4_req_4(mean_time)
    # Calling HRanking Function
    HRank.perform_asymmetric(req4)
    print()

    # RQ5
    print("Requirement 5 for Metapath 4:\n")    
    req5 = query.metapath_4_req_5(mean_time)
    # Calling HRanking Function
    HRank.perform_asymmetric(req5)
    print()
    
    # RQ6
    print("Requirement 6 for Metapath 4:\n")    
    req6 = query.metapath_4_req_6(mean_time)
    # Calling HRanking Function
    HRank.perform_asymmetric(req6)
    print()

    # RQ7
    print("Requirement 7 for Metapath 4:\n")
    req7 = query.metapath_4_req_7(mean_time)  
    # Calling HRanking Function
    HRank.perform_asymmetric(req7)
    print()
    
    # RQ8
    print("Requirement 8 for Metapath 4:\n")    
    req8 = query.metapath_4_req_8(mean_time)
    # Calling HRanking Function
    HRank.perform_asymmetric(req8)
    print()
    


        
 