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
        self.pull_requests_files = self.db["pull_request_file"]
        self.files_list = list(self.pull_requests_files({"pull_request_id": {"$exists": True}}))
                
    def metapath_4_req_1(self, mean_time):
        
        prs_list = STQ.find_less_than_rejected_prs(mean_time)
                
        metapaths = []
        
        for pr in prs_list:
            
            pr_files = prs_list[pr]
            
            for file_1 in self.files_list:
                
                file_1_info = self.pull_requests_files.find_one({"_id": file_1})
                
                # print(file_1_info)
                
                for file_2 in self.files_list:
                    
                    file_2_info = self.pull_requests_files.find_one({"_id": file_2})
                    
                    if(file_1_info["pull_request_id"] == file_2_info["pull_request_id"] and file_1_info["pull_request_id"] == pr):
                        metapaths.append(
                                {
                                    "developer_1": str(pr["creator_id"]),
                                    "file_1": file_1,
                                    "pull_request": pr,
                                    "file_2": file_2,
                                    "developer_2": str(pr["creator_id"]),
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
    print(req1)
    # Calling HRanking Function
    # HRank.perform_asymmetric(req1)
    print()
    
    # # RQ2
    # print("Requirement 2 for Metapath 4:\n")    
    # req2 = query.metapath_4_req_2(mean_time)
    # # Calling HRanking Function
    # HRank.perform_asymmetric(req2)
    # print()

    # # RQ3
    # print("Requirement 3 for Metapath 4:\n")    
    # req3 = query.metapath_4_req_3(mean_time)
    # # Calling HRanking Function
    # HRank.perform_asymmetric(req3)
    # print()
    
    # # RQ4
    # print("Requirement 4 for Metapath 4:\n")    
    # req4 = query.metapath_4_req_4(mean_time)
    # # Calling HRanking Function
    # HRank.perform_asymmetric(req4)
    # print()

    # # RQ5
    # print("Requirement 5 for Metapath 4:\n")    
    # req5 = query.metapath_4_req_5(mean_time)
    # # Calling HRanking Function
    # HRank.perform_asymmetric(req5)
    # print()
    
    # # RQ6
    # print("Requirement 6 for Metapath 4:\n")    
    # req6 = query.metapath_4_req_6(mean_time)
    # # Calling HRanking Function
    # HRank.perform_asymmetric(req6)
    # print()

    # # RQ7
    # print("Requirement 7 for Metapath 4:\n")
    # req7 = query.metapath_4_req_7(mean_time)  
    # # Calling HRanking Function
    # HRank.perform_asymmetric(req7)
    # print()
    
    # # RQ8
    # print("Requirement 8 for Metapath 4:\n")    
    # req8 = query.metapath_4_req_8(mean_time)
    # # Calling HRanking Function
    # HRank.perform_asymmetric(req8)
    # print()
    


        
 