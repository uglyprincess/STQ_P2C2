## D ---> PR ---> File ---> PR ---> Developer

from pymongo import MongoClient
from HRanking import HRank
from Requirements import smartshark
from Build_reverse_identity_dictionary import Build_reverse_identity_dictionary
from bson import ObjectId

class metapath_8():
    
    def __init__(self):
        
        self.project = "giraph"
        
        self.client = MongoClient("mongodb://localhost:27017/")
        self.db = self.client["smartshark"]       
        
        target_repo = "https://github.com/apache/" + self.project  
        
        self.pull_requests = self.db["pull_request"]
        self.pull_request_files = self.db["pull_request_file"]
        self.pull_request_reviews = self.db["pull_request_review"]
        
        self.BRID = Build_reverse_identity_dictionary()
        self.BRID.reading_identity_and_people_and_building_reverse_identity_dictionary()
        
    def metapath_8_req_1(self, mean_time):
        
        prs_list = STQ.find_less_than_rejected_prs(mean_time)
        
        metapaths = []
        
        for pr_1 in prs_list:
            
            files_list_1 = prs_list[pr_1]
            
            for pr_2 in prs_list:
                
                if(pr_1 is not pr_2):
                    
                    files_list_2 = prs_list[pr_2]
                    
                    for file_1 in files_list_1:
                        
                        file_1_path = self.pull_request_files.find_one({"_id": ObjectId(file_1)})["path"]
                        # print(file_1_path)
                        
                        for file_2 in files_list_2:
                            
                            file_2_path = self.pull_request_files.find_one({"_id": ObjectId(file_2)})["path"]
                            # print(file_2_path)
                            
                            if(file_1_path is file_2_path):
                                
                                print("Files matched!")
                                
                                developer = self.pull_requests.find_one({"_id": ObjectId(pr_1)})["creator_id"]
                                
                                actual_developer = str(self.BRID.reverse_identity_dict[developer])
                                
                                pr_2_review = self.pull_request_reviews.find_one({"pull_request_id": ObjectId(pr_2)})
                                
                                review_id = str(pr_2_review["_id"])
                                reviewer_id = pr_2_review["creator_id"]
                                
                                actual_reviewer = str(self.BRID.reverse_identity_dict[reviewer_id])
                            
                                metapaths.append(
                                    {
                                        "developer_1": actual_developer,
                                        "pr_1": pr_1,
                                        "file": file_1_path,
                                        "pr_2": pr_2,
                                        "pr_2_review": review_id,
                                        "reviewer": actual_reviewer
                                    }
                                )
        
        return metapaths
        
    def metapath_8_req_2(self, mean_time):
        
        prs_list = STQ.find_less_than_rejected_prs(mean_time)
        
        metapaths = []
        
        for pr_1 in prs_list:
            
            files_list_1 = prs_list[pr_1]
            
            for pr_2 in prs_list:
                
                if(pr_1 is not pr_2):
                    
                    files_list_2 = prs_list[pr_2]
                    
                    for file_1 in files_list_1:
                        
                        file_1_path = self.pull_request_files.find_one({"_id": ObjectId(file_1)})["path"]
                        # print(file_1_path)
                        
                        for file_2 in files_list_2:
                            
                            file_2_path = self.pull_request_files.find_one({"_id": ObjectId(file_2)})["path"]
                            # print(file_2_path)
                            
                            if(file_1_path is file_2_path):
                                
                                print("Files matched!")
                                
                                developer = self.pull_requests.find_one({"_id": ObjectId(pr_1)})["creator_id"]
                                
                                actual_developer = str(self.BRID.reverse_identity_dict[developer])
                                
                                pr_2_review = self.pull_request_reviews.find_one({"pull_request_id": ObjectId(pr_2)})
                                
                                review_id = str(pr_2_review["_id"])
                                reviewer_id = pr_2_review["creator_id"]
                                
                                actual_reviewer = str(self.BRID.reverse_identity_dict[reviewer_id])
                            
                                metapaths.append(
                                    {
                                        "developer_1": actual_developer,
                                        "pr_1": pr_1,
                                        "file": file_1_path,
                                        "pr_2": pr_2,
                                        "pr_2_review": review_id,
                                        "reviewer": actual_reviewer
                                    }
                                )
        
        return metapaths
        
    def metapath_8_req_3(self, mean_time):
        
        prs_list = STQ.find_less_than_all_prs(mean_time)
        
        metapaths = []
        
        for pr_1 in prs_list:
            
            files_list_1 = prs_list[pr_1]
            
            for pr_2 in prs_list:
                
                if(pr_1 is not pr_2):
                    
                    files_list_2 = prs_list[pr_2]
                    
                    for file_1 in files_list_1:
                        
                        file_1_path = self.pull_request_files.find_one({"_id": ObjectId(file_1)})["path"]
                        # print(file_1_path)
                        
                        for file_2 in files_list_2:
                            
                            file_2_path = self.pull_request_files.find_one({"_id": ObjectId(file_2)})["path"]
                            # print(file_2_path)
                            
                            if(file_1_path is file_2_path):
                                
                                print("Files matched!")
                                
                                developer = self.pull_requests.find_one({"_id": ObjectId(pr_1)})["creator_id"]
                                
                                actual_developer = str(self.BRID.reverse_identity_dict[developer])
                                
                                pr_2_review = self.pull_request_reviews.find_one({"pull_request_id": ObjectId(pr_2)})
                                
                                review_id = str(pr_2_review["_id"])
                                reviewer_id = pr_2_review["creator_id"]
                                
                                actual_reviewer = str(self.BRID.reverse_identity_dict[reviewer_id])
                            
                                metapaths.append(
                                    {
                                        "reviewer": actual_reviewer,   
                                        "pr_2_review": review_id,   
                                        "pr_2": pr_2,      
                                        "file": file_1_path,
                                        "pr_1": pr_1,                                                                                                                                                    
                                        "developer_1": actual_developer
                                    }
                                )
        
        return metapaths
        
    def metapath_8_req_4(self, mean_time):
        
        prs_list = STQ.find_more_than_all_prs(mean_time)
        
        metapaths = []
        
        for pr_1 in prs_list:
            
            files_list_1 = prs_list[pr_1]
            
            for pr_2 in prs_list:
                
                if(pr_1 is not pr_2):
                    
                    files_list_2 = prs_list[pr_2]
                    
                    for file_1 in files_list_1:
                        
                        file_1_path = self.pull_request_files.find_one({"_id": ObjectId(file_1)})["path"]
                        # print(file_1_path)
                        
                        for file_2 in files_list_2:
                            
                            file_2_path = self.pull_request_files.find_one({"_id": ObjectId(file_2)})["path"]
                            # print(file_2_path)
                            
                            if(file_1_path is file_2_path):
                                
                                print("Files matched!")
                                
                                developer = self.pull_requests.find_one({"_id": ObjectId(pr_1)})["creator_id"]
                                
                                actual_developer = str(self.BRID.reverse_identity_dict[developer])
                                
                                pr_2_review = self.pull_request_reviews.find_one({"pull_request_id": ObjectId(pr_2)})
                                
                                review_id = str(pr_2_review["_id"])
                                reviewer_id = pr_2_review["creator_id"]
                                
                                actual_reviewer = str(self.BRID.reverse_identity_dict[reviewer_id])
                            
                                metapaths.append(
                                    {
                                        "reviewer": actual_reviewer,   
                                        "pr_2_review": review_id,   
                                        "pr_2": pr_2,      
                                        "file": file_1_path,
                                        "pr_1": pr_1,                                                                                                                                                    
                                        "developer_1": actual_developer
                                    }
                                )
        
        return metapaths
        
    def metapath_8_req_5(self, mean_time):
        
        prs_list = STQ.find_less_than_accepted_prs(mean_time)
        
        metapaths = []
        
        for pr_1 in prs_list:
            
            files_list_1 = prs_list[pr_1]
            
            for pr_2 in prs_list:
                
                if(pr_1 is not pr_2):
                    
                    files_list_2 = prs_list[pr_2]
                    
                    for file_1 in files_list_1:
                        
                        file_1_path = self.pull_request_files.find_one({"_id": ObjectId(file_1)})["path"]
                        # print(file_1_path)
                        
                        for file_2 in files_list_2:
                            
                            file_2_path = self.pull_request_files.find_one({"_id": ObjectId(file_2)})["path"]
                            # print(file_2_path)
                            
                            if(file_1_path is file_2_path):
                                
                                print("Files matched!")
                                
                                developer = self.pull_requests.find_one({"_id": ObjectId(pr_1)})["creator_id"]
                                
                                actual_developer = str(self.BRID.reverse_identity_dict[developer])
                                
                                pr_2_review = self.pull_request_reviews.find_one({"pull_request_id": ObjectId(pr_2)})
                                
                                review_id = str(pr_2_review["_id"])
                                reviewer_id = pr_2_review["creator_id"]
                                
                                actual_reviewer = str(self.BRID.reverse_identity_dict[reviewer_id])
                            
                                metapaths.append(
                                    {
                                        "developer_1": actual_developer,
                                        "pr_1": pr_1,
                                        "file": file_1_path,
                                        "pr_2": pr_2,
                                        "pr_2_review": review_id,
                                        "reviewer": actual_reviewer
                                    }
                                )
        
        return metapaths
        
    def metapath_8_req_6(self, mean_time):
        
        prs_list = STQ.find_more_than_accepted_prs(mean_time)
        
        metapaths = []
        
        for pr_1 in prs_list:
            
            files_list_1 = prs_list[pr_1]
            
            for pr_2 in prs_list:
                
                if(pr_1 is not pr_2):
                    
                    files_list_2 = prs_list[pr_2]
                    
                    for file_1 in files_list_1:
                        
                        file_1_path = self.pull_request_files.find_one({"_id": ObjectId(file_1)})["path"]
                        # print(file_1_path)
                        
                        for file_2 in files_list_2:
                            
                            file_2_path = self.pull_request_files.find_one({"_id": ObjectId(file_2)})["path"]
                            # print(file_2_path)
                            
                            if(file_1_path is file_2_path):
                                
                                print("Files matched!")
                                
                                developer = self.pull_requests.find_one({"_id": ObjectId(pr_1)})["creator_id"]
                                
                                actual_developer = str(self.BRID.reverse_identity_dict[developer])
                                
                                pr_2_review = self.pull_request_reviews.find_one({"pull_request_id": ObjectId(pr_2)})
                                
                                review_id = str(pr_2_review["_id"])
                                reviewer_id = pr_2_review["creator_id"]
                                
                                actual_reviewer = str(self.BRID.reverse_identity_dict[reviewer_id])
                            
                                metapaths.append(
                                    {
                                        "developer_1": actual_developer,
                                        "pr_1": pr_1,
                                        "file": file_1_path,
                                        "pr_2": pr_2,
                                        "pr_2_review": review_id,
                                        "reviewer": actual_reviewer
                                    }
                                )
        
        return metapaths
                
    def metapath_8_req_7(self, mean_time):
        
        prs_list = STQ.find_less_than_rejected_prs(mean_time)
        
        metapaths = []
        
        for pr_1 in prs_list:
            
            files_list_1 = prs_list[pr_1]
            
            for pr_2 in prs_list:
                
                if(pr_1 is not pr_2):
                    
                    files_list_2 = prs_list[pr_2]
                    
                    for file_1 in files_list_1:
                        
                        file_1_path = self.pull_request_files.find_one({"_id": ObjectId(file_1)})["path"]
                        # print(file_1_path)
                        
                        for file_2 in files_list_2:
                            
                            file_2_path = self.pull_request_files.find_one({"_id": ObjectId(file_2)})["path"]
                            # print(file_2_path)
                            
                            if(file_1_path is file_2_path):
                                
                                print("Files matched!")
                                
                                developer = self.pull_requests.find_one({"_id": ObjectId(pr_1)})["creator_id"]
                                
                                actual_developer = str(self.BRID.reverse_identity_dict[developer])
                                
                                pr_2_review = self.pull_request_reviews.find_one({"pull_request_id": ObjectId(pr_2)})
                                
                                review_id = str(pr_2_review["_id"])
                                reviewer_id = pr_2_review["creator_id"]
                                
                                actual_reviewer = str(self.BRID.reverse_identity_dict[reviewer_id])
                            
                                metapaths.append(
                                    {
                                        "developer_1": actual_developer,
                                        "pr_1": pr_1,
                                        "file": file_1_path,
                                        "pr_2": pr_2,
                                        "pr_2_review": review_id,
                                        "reviewer": actual_reviewer
                                    }
                                )
        
        return metapaths

    def metapath_8_req_8(self, mean_time):
        
        prs_list = STQ.find_more_than_rejected_prs(mean_time)
                
        metapaths = []
        
        for pr_1 in prs_list:
            
            files_list_1 = prs_list[pr_1]
            
            for pr_2 in prs_list:
                
                if(pr_1 is not pr_2):
                    
                    files_list_2 = prs_list[pr_2]
                    
                    for file_1 in files_list_1:
                        
                        file_1_path = self.pull_request_files.find_one({"_id": ObjectId(file_1)})["path"]
                        # print(file_1_path)
                        
                        for file_2 in files_list_2:
                            
                            file_2_path = self.pull_request_files.find_one({"_id": ObjectId(file_2)})["path"]
                            # print(file_2_path)
                            
                            if(file_1_path is file_2_path):
                                
                                print("Files matched!")
                                
                                developer = self.pull_requests.find_one({"_id": ObjectId(pr_1)})["creator_id"]
                                
                                actual_developer = str(self.BRID.reverse_identity_dict[developer])
                                
                                pr_2_review = self.pull_request_reviews.find_one({"pull_request_id": ObjectId(pr_2)})
                                
                                review_id = str(pr_2_review["_id"])
                                reviewer_id = pr_2_review["creator_id"]
                                
                                actual_reviewer = str(self.BRID.reverse_identity_dict[reviewer_id])
                            
                                metapaths.append(
                                    {
                                        "developer_1": actual_developer,
                                        "pr_1": pr_1,
                                        "file": file_1_path,
                                        "pr_2": pr_2,
                                        "pr_2_review": review_id,
                                        "reviewer": actual_reviewer
                                    }
                                )
        
        return metapaths
                                    
if __name__ == "__main__":
    
    STQ = smartshark()
    query = metapath_8()

    rejection_mean_time = STQ.find_mean_rejected_prs()
    acceptance_mean_time = STQ.find_mean_accepted_prs()
    all_mean_time = STQ.find_mean_all_prs()
    # print(mean_time)
    
    # RQ1
    print("Requirement 1 for Metapath 8:\n")
    req1 = query.metapath_8_req_1(rejection_mean_time)  
    print(req1)
    # Calling HRanking Function
    HRank.perform_asymmetric(req1)
    print()
    
    # RQ2
    print("Requirement 2 for Metapath 8:\n")    
    req2 = query.metapath_8_req_2(rejection_mean_time)
    # Calling HRanking Function
    HRank.perform_asymmetric(req2)
    print()
    
    # RQ3
    print("Requirement 3 for Metapath 8:\n")    
    req3 = query.metapath_8_req_3(all_mean_time)
    # Calling HRanking Function
    HRank.perform_asymmetric(req3)
    print()
    
    # RQ4
    print("Requirement 4 for Metapath 8:\n")    
    req4 = query.metapath_8_req_4(all_mean_time)
    # Calling HRanking Function
    HRank.perform_asymmetric(req4)
    print()
    
    # RQ5
    print("Requirement 5 for Metapath 8:\n")    
    req5 = query.metapath_8_req_5(acceptance_mean_time)
    # Calling HRanking Function
    HRank.perform_asymmetric(req5)
    print()
    
    # RQ6
    print("Requirement 6 for Metapath 8:\n")    
    req6 = query.metapath_8_req_6(acceptance_mean_time)
    # Calling HRanking Function
    HRank.perform_asymmetric(req6)
    print()
   
    # RQ7
    print("Requirement 7 for Metapath 8:\n")    
    req7 = query.metapath_8_req_7(rejection_mean_time)
    # Calling HRanking Function
    HRank.perform_asymmetric(req7)
    print()
            
    # RQ8
    print("Requirement 8 for Metapath 8:\n")    
    req8 = query.metapath_8_req_8(rejection_mean_time)
    # Calling HRanking Function
    HRank.perform_asymmetric(req8)
    print()
    