## D ---> PR ---> File ---> PR ---> Developer

from pymongo import MongoClient
from HRanking import HRank
from Requirements import smartshark
from Build_reverse_identity_dictionary import Build_reverse_identity_dictionary
from bson import ObjectId

class metapath_9():
    
    def __init__(self):
        
        self.project = "giraph"
        
        self.client = MongoClient("mongodb://localhost:27017/")
        self.db = self.client["smartshark"]       
        
        target_repo = "https://github.com/apache/" + self.project  
        
        self.pull_requests = self.db["pull_request"]
        self.pull_request_files = self.db["pull_request_file"]
        self.pull_request_reviews = self.db["pull_request_review"]
        self.pull_request_event = self.db["pull_request_event"]
        self.events_list = list(self.pull_request_event.find({"author_id": {"$exists": True}}))
        
        self.BRID = Build_reverse_identity_dictionary()
        self.BRID.reading_identity_and_people_and_building_reverse_identity_dictionary()
        
    def metapath_9_req_1(self, mean_time):
        
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
                                                                
                                developer = self.pull_requests.find_one({"_id": ObjectId(pr_1)})["creator_id"]
                                
                                actual_developer = str(self.BRID.reverse_identity_dict[developer])
                                
                                for event in self.events_list:
                                    
                                    event_type = event["event_type"]
                                    event_creator = event["author_id"]
                                    
                                    actual_creator = str(self.BRID.reverse_identity_dict[event_creator])
                                    
                                    if(str(event["pull_request_id"]) == pr_2 and event_type == "merged"):
                                        
                                        metapaths.append(
                                            {
                                                "developer": actual_developer,
                                                "pr_1": pr_1,
                                                "file": file_1_path,
                                                "pr_2": pr_2,
                                                "integrated_by": actual_creator
                                            }
                                        )
                                
        return metapaths
        
    def metapath_9_req_2(self, mean_time):
        
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
                                                                
                                developer = self.pull_requests.find_one({"_id": ObjectId(pr_1)})["creator_id"]
                                
                                actual_developer = str(self.BRID.reverse_identity_dict[developer])
                                
                                for event in self.events_list:
                                    
                                    event_type = event["event_type"]
                                    event_creator = event["author_id"]
                                    
                                    actual_creator = str(self.BRID.reverse_identity_dict[event_creator])
                                    
                                    if(str(event["pull_request_id"]) == pr_2 and event_type == "merged"):
                                        
                                        metapaths.append(
                                            {
                                                "developer": actual_developer,
                                                "pr_1": pr_1,
                                                "file": file_1_path,
                                                "pr_2": pr_2,
                                                "integrated_by": actual_creator
                                            }
                                        )
        
        return metapaths
                                    
if __name__ == "__main__":
    
    STQ = smartshark()
    query = metapath_9()
    rejection_mean_time = STQ.find_mean_rejected_prs()
    # print(mean_time)
    
    # RQ1
    print("Requirement 1 for Metapath 9:\n")
    req1 = query.metapath_9_req_1(rejection_mean_time)  
    print(req1)
    # Calling HRanking Function
    HRank.perform_asymmetric(req1)
    print()
    
    # RQ2
    print("Requirement 2 for Metapath 9:\n")    
    req2 = query.metapath_9_req_2(rejection_mean_time)
    # Calling HRanking Function
    HRank.perform_asymmetric(req2)
    print()
    
    