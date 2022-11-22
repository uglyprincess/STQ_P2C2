## D ---> PR ---> Integrated By

from pymongo import MongoClient
from HRanking import HRank
from Requirements import smartshark
from Build_reverse_identity_dictionary import Build_reverse_identity_dictionary

class metapath_5():
    
    def __init__(self):
        
        self.project = "giraph"
        
        self.client = MongoClient("mongodb://localhost:27017/")
        self.db = self.client["smartshark"]       
        
        target_repo = "https://github.com/apache/" + self.project  
        
        self.pull_requests = self.db["pull_request"]
        self.pull_requests_list = list(self.pull_requests.find({"creator_id": {"$exists": True}}))
        self.pull_request_event = self.db["pull_request_event"]
        self.events_list = list(self.pull_request_event.find({"author_id": {"$exists": True}}))
        
        self.BRID = Build_reverse_identity_dictionary()
        self.BRID.reading_identity_and_people_and_building_reverse_identity_dictionary()
                
    def metapath_5_req_1(self, mean_time):
        
        prs_list = STQ.find_less_than_rejected_prs(mean_time)
                
        metapaths = []
        
        for event in self.events_list:
            
            event_type = event["event_type"]
            event_creator = event["author_id"]
            
            actual_creator_id = str(self.BRID.reverse_identity_dict[event_creator])
                        
            for pr in prs_list:
                if(str(event["pull_request_id"]) == pr and event_type == "merged"):
                                                            
                    for pr_info in self.pull_requests_list:
                        
                        actual_developer_id = str(self.BRID.reverse_identity_dict[pr_info["creator_id"]])
                        
                        if(str(pr_info["_id"])==pr):
                            
                            metapaths.append(
                                {
                                    "developer": actual_developer_id,
                                    "pull_request": pr,
                                    "integrated_by": actual_creator_id,
                                }
                            )
        
        return metapaths

    def metapath_5_req_2(self, mean_time):
        
        prs_list = STQ.find_more_than_rejected_prs(mean_time)
                
        metapaths = []
        
        for event in self.events_list:
            
            event_type = event["event_type"]
            event_creator = event["author_id"]
            
            actual_creator_id = str(self.BRID.reverse_identity_dict[event_creator])
                        
            for pr in prs_list:
                if(str(event["pull_request_id"]) == pr):
                                        
                    for pr_info in self.pull_requests_list:
                        
                        actual_developer_id = str(self.BRID.reverse_identity_dict[pr_info["creator_id"]])
                        
                        if(str(pr_info["_id"])==pr):
                            
                            metapaths.append(
                                {
                                    "developer": actual_developer_id,
                                    "pull_request": pr,
                                    "integrated_by": actual_creator_id,
                                }
                            )
        
        return metapaths
                                    
if __name__ == "__main__":
    
    STQ = smartshark()
    query = metapath_5()
    rejection_mean_time = STQ.find_mean_rejected_prs()
    # print(mean_time)
    
    # RQ1
    print("Requirement 1 for Metapath 5:\n")
    req1 = query.metapath_5_req_1(rejection_mean_time)  
    # Calling HRanking Function
    HRank.perform_asymmetric(req1)
    print()
    
    # RQ2
    print("Requirement 2 for Metapath 5:\n")    
    req2 = query.metapath_5_req_2(rejection_mean_time)
    # Calling HRanking Function
    HRank.perform_asymmetric(req2)
    print()
    


        
 