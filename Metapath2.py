## D ---> PR ---> File ---> PR ---> Developer

from pymongo import MongoClient
from HRanking import HRank
from Requirements import smartshark
from Build_reverse_identity_dictionary import Build_reverse_identity_dictionary
from bson import ObjectId

class metapath_2():
    
    def __init__(self):
        
        self.project = "giraph"
        
        self.client = MongoClient("mongodb://localhost:27017/")
        self.db = self.client["smartshark"]       
        
        target_repo = "https://github.com/apache/" + self.project  
        
        self.pull_requests = self.db["pull_request"]
        self.pull_request_files = self.db["pull_request_file"]
        
        self.BRID = Build_reverse_identity_dictionary()
        self.BRID.reading_identity_and_people_and_building_reverse_identity_dictionary()
                
    def metapath_2_req_1(self, mean_time):
        
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
                                
                                developer_1 = self.pull_requests.find_one({"_id": ObjectId(pr_1)})["creator_id"]
                                developer_2 = self.pull_requests.find_one({"_id": ObjectId(pr_2)})["creator_id"]
                                
                                actual_developer_1 = str(self.BRID.reverse_identity_dict[developer_1])
                                actual_developer_2 = str(self.BRID.reverse_identity_dict[developer_2])
                                
                                metapaths.append(
                                    {
                                        "developer_1": actual_developer_1,
                                        "pr_1": pr_1,
                                        "file": file_1_path,
                                        "pr_2": pr_2,
                                        "developer_2": actual_developer_2
                                    }
                                )
        
        return metapaths

    def metapath_2_req_2(self, mean_time):
        
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
                                
                                developer_1 = self.pull_requests.find_one({"_id": ObjectId(pr_1)})["creator_id"]
                                developer_2 = self.pull_requests.find_one({"_id": ObjectId(pr_2)})["creator_id"]
                                
                                actual_developer_1 = str(self.BRID.reverse_identity_dict[developer_1])
                                actual_developer_2 = str(self.BRID.reverse_identity_dict[developer_2])
                                
                                metapaths.append(
                                    {
                                        "developer_1": actual_developer_1,
                                        "pr_1": pr_1,
                                        "file": file_1_path,
                                        "pr_2": pr_2,
                                        "developer_2": actual_developer_2
                                    }
                                )
        
        return metapaths
                                    
if __name__ == "__main__":
    
    STQ = smartshark()
    query = metapath_2()
    rejection_mean_time = STQ.find_mean_rejected_prs()
    # print(mean_time)
    
    # RQ1
    print("Requirement 1 for Metapath 4:\n")
    req1 = query.metapath_2_req_1(rejection_mean_time)  
    print(req1)
    # Calling HRanking Function
    HRank.perform_asymmetric(req1)
    print()
    
    # RQ2
    print("Requirement 2 for Metapath 2:\n")    
    req2 = query.metapath_2_req_2(rejection_mean_time)
    # Calling HRanking Function
    HRank.perform_asymmetric(req2)
    print()
    


        
 