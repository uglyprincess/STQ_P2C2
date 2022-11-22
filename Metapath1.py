## D ---> File ---> PR ---> File ---> Developer

from pymongo import MongoClient
from HRanking import HRank
from Requirements import smartshark
from Build_reverse_identity_dictionary import Build_reverse_identity_dictionary
from bson import ObjectId

class metapath_1():
    
    def __init__(self):
        
        self.project = "giraph"
        
        self.client = MongoClient("mongodb://localhost:27017/")
        self.db = self.client["smartshark"]       
        
        target_repo = "https://github.com/apache/" + self.project  
        
        self.pull_request_files = self.db["pull_request_file"]
        self.files = self.db["file"]
        self.file_action = self.db["file_action"]
        self.commits = self.db["commit"]
        
        self.BRID = Build_reverse_identity_dictionary()
        self.BRID.reading_identity_and_people_and_building_reverse_identity_dictionary()
                
    def metapath_1_req_1(self, mean_time):
        
        prs_list = STQ.find_less_than_rejected_prs(mean_time)
                
        metapaths = []
        
        for pr in prs_list:
            
            developer_and_commit = dict()
            
            all_files = prs_list[pr]
            
            for file in all_files:
                
                file_path = self.pull_request_files.find_one({"_id": ObjectId(file)})["path"]
                
                if(file_path is not None):
                    
                    file_id = self.files.find_one({"path": file_path})
                    
                    if(file_id is not None):
                        
                        file_commits = list(self.file_action.find({"file_id": file_id["_id"]}))
                        
                        for commit in file_commits:
                                                   
                            commit_id = self.commits.find_one({"_id": commit["commit_id"]})
                                
                            committer_id = commit_id["author_id"]
                                
                            if((committer_id not in developer_and_commit) and (committer_id is not None)):    
                                developer_and_commit[committer_id] = list()
                                    
                            if(committer_id is not None):
                                developer_and_commit[committer_id].append(file)
                            
            for developer_1 in developer_and_commit:
                
                # print("Developer 1: ", developer_1)
                for developer_2 in developer_and_commit:
                    
                    # print("Developer 2: ", developer_2)
                    if(developer_1 != developer_2):
                        
                        dev_1_commits = developer_and_commit[developer_1]
                        dev_2_commits = developer_and_commit[developer_2]
                        
                        for commit_1 in dev_1_commits:
                            
                            # print("Commit 1: ", commit_1)
                            for commit_2 in dev_2_commits:
                                                      
                                # print("Commit 2:", commit_2)          
                                metapaths.append(
                                    {
                                        "developer_1": developer_1,
                                        "commit_1": commit_1,
                                        "pr": pr,
                                        "commit_2": commit_2,
                                        "developer_2": developer_2
                                    }
                                )
            
        return metapaths

    def metapath_1_req_2(self, mean_time):
        
        prs_list = STQ.find_more_than_rejected_prs(mean_time)
                
        metapaths = []
        
        for pr in prs_list:
            
            developer_and_commit = dict()
            
            all_files = prs_list[pr]
            
            for file in all_files:
                
                file_path = self.pull_request_files.find_one({"_id": ObjectId(file)})["path"]
                
                if(file_path is not None):
                    
                    file_id = self.files.find_one({"path": file_path})
                    
                    if(file_id is not None):
                        
                        file_commits = list(self.file_action.find({"file_id": file_id["_id"]}))
                        
                        for commit in file_commits:
                                                   
                            commit_id = self.commits.find_one({"_id": commit["commit_id"]})
                                
                            committer_id = commit_id["author_id"]
                                
                            if((committer_id not in developer_and_commit) and (committer_id is not None)):    
                                developer_and_commit[committer_id] = list()
                                    
                            if(committer_id is not None):
                                developer_and_commit[committer_id].append(file)
                            
            for developer_1 in developer_and_commit:
                
                # print("Developer 1: ", developer_1)
                for developer_2 in developer_and_commit:
                    
                    # print("Developer 2: ", developer_2)
                    if(developer_1 != developer_2):
                        
                        dev_1_commits = developer_and_commit[developer_1]
                        dev_2_commits = developer_and_commit[developer_2]
                        
                        for commit_1 in dev_1_commits:
                            
                            # print("Commit 1: ", commit_1)
                            for commit_2 in dev_2_commits:
                                                      
                                # print("Commit 2:", commit_2)          
                                metapaths.append(
                                    {
                                        "developer_1": developer_1,
                                        "commit_1": commit_1,
                                        "pr": pr,
                                        "commit_2": commit_2,
                                        "developer_2": developer_2
                                    }
                                )
            
        return metapaths
                                    
if __name__ == "__main__":
    
    STQ = smartshark()
    query = metapath_1()
    rejection_mean_time = STQ.find_mean_rejected_prs()
    # print(mean_time)
    
    # RQ1
    print("Requirement 1 for Metapath 1:\n")
    req1 = query.metapath_1_req_1(rejection_mean_time)  
    # Calling HRanking Function
    HRank.perform_asymmetric(req1)
    print()
    
    # RQ2
    print("Requirement 2 for Metapath 1:\n")    
    req2 = query.metapath_1_req_2(rejection_mean_time)
    # Calling HRanking Function
    HRank.perform_asymmetric(req2)
    print()



        
 