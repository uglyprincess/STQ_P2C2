from pymongo import MongoClient

class smartshark():
    
    def __init__(self):
        
        self.project = "giraph"
        
        self.reject_pr_mean_time = 0
        self.accept_pr_mean_time = 0
        self.all_pr_mean_time = 0
        
        self.less_than_rejected_pr_ranking = dict()
        self.more_than_rejected_pr_ranking = dict()
        self.less_than_accepted_pr_ranking = dict()
        self.more_than_accepted_pr_ranking = dict()
        self.less_than_all_pr_ranking = dict()
        self.more_than_all_pr_ranking = dict()
    
        self.client = MongoClient("mongodb://localhost:27017/")
    
        self.db = self.client["smartshark"] 
        
        target_repo = "https://github.com/apache/" + self.project
        
        self.pull_request_reviews = self.db["pull_request_review"]
        self.pull_request = self.db["pull_request"]
        self.pull_request_files = self.db["pull_request_file"]
        
        self.list_reviews = list(self.pull_request_reviews.find({"creator_id": {"$exists": True}}))
        self.list_prs = list(self.pull_request.find({"target_repo_url": target_repo, "creator_id": {"$exists": True}}))
        
    def find_mean_rejected_prs(self):
            
        target_repo = "https://github.com/apache/" + self.project
        
        self.pull_request_reviews = self.db["pull_request_review"]
        self.pull_request = self.db["pull_request"]
        
        self.list_reviews = list(self.pull_request_reviews.find({}))
        self.list_prs = list(self.pull_request.find({"target_repo_url": target_repo}))
        
        decision = "APPROVED"
        
        total_reviews = 0
        total_time = 0
        
        for review in self.list_reviews:
            
            corresponding_pr = review["pull_request_id"]
            review_time = int(round(review["submitted_at"].timestamp()))
                        
            for pr in self.list_prs:
                
                if(corresponding_pr == pr["_id"] and review["state"] != decision):
                    
                    creation_time = int(round(pr["created_at"].timestamp()))
                    
                    # print(f"Review time is {int(round(review_time.timestamp()))}")
                    # print(f"Submission time is {int(round(creation_time.timestamp()))}")
                    
                    total_time = total_time + review_time - creation_time
                    total_reviews = total_reviews + 1
                    
        self.reject_pr_mean_time = int(round(total_time/total_reviews))
        print(f"\nAverage rejection time for Rejected PRs is {self.reject_pr_mean_time} seconds\n")
        
        return self.reject_pr_mean_time
    
    def find_mean_accepted_prs(self):
            
        target_repo = "https://github.com/apache/" + self.project
        
        self.pull_request_reviews = self.db["pull_request_review"]
        self.pull_request = self.db["pull_request"]
        
        self.list_reviews = list(self.pull_request_reviews.find({}))
        self.list_prs = list(self.pull_request.find({"target_repo_url": target_repo}))
        
        decision = "APPROVED"
        
        total_reviews = 0
        total_time = 0
        
        for review in self.list_reviews:
            
            corresponding_pr = review["pull_request_id"]
            review_time = int(round(review["submitted_at"].timestamp()))
                        
            for pr in self.list_prs:
                
                if(corresponding_pr == pr["_id"] and review["state"] == decision):
                    
                    creation_time = int(round(pr["created_at"].timestamp()))
                    
                    # print(f"Review time is {int(round(review_time.timestamp()))}")
                    # print(f"Submission time is {int(round(creation_time.timestamp()))}")
                    
                    total_time = total_time + review_time - creation_time
                    total_reviews = total_reviews + 1
                    
        self.accept_pr_mean_time = int(round(total_time/total_reviews))
        print(f"\nAverage acceptance time for Accepted PRs is {self.reject_pr_mean_time} seconds\n")
        
        return self.accept_pr_mean_time
    
    def find_mean_all_prs(self):
            
        target_repo = "https://github.com/apache/" + self.project
        
        self.pull_request_reviews = self.db["pull_request_review"]
        self.pull_request = self.db["pull_request"]
        
        self.list_reviews = list(self.pull_request_reviews.find({}))
        self.list_prs = list(self.pull_request.find({"target_repo_url": target_repo}))
        
        decision = "APPROVED"
        
        total_reviews = 0
        total_time = 0
        
        for review in self.list_reviews:
            
            corresponding_pr = review["pull_request_id"]
            review_time = int(round(review["submitted_at"].timestamp()))
                        
            for pr in self.list_prs:
                
                if(corresponding_pr == pr["_id"] and review["state"]):
                    
                    creation_time = int(round(pr["created_at"].timestamp()))
                    
                    # print(f"Review time is {int(round(review_time.timestamp()))}")
                    # print(f"Submission time is {int(round(creation_time.timestamp()))}")
                    
                    total_time = total_time + review_time - creation_time
                    total_reviews = total_reviews + 1
                    
        self.all_pr_mean_time = int(round(total_time/total_reviews))
        print(f"\nAverage decision time for All PRs is {self.reject_pr_mean_time} seconds\n")
        
        return self.all_pr_mean_time
        
    def find_less_than_rejected_prs(self, reject_pr_mean_time):
        
        decision = "APPROVED"
        
        for review in self.list_reviews:
            
            corresponding_pr = review["pull_request_id"]
            review_time = int(round(review["submitted_at"].timestamp()))
                        
            for pr in self.list_prs:
                
                if(corresponding_pr == pr["_id"] and review["state"] != decision):
                    
                    creation_time = int(round(pr["created_at"].timestamp()))
                                        
                    pr_id = str(pr["_id"])
                                                            
                    if(review_time - creation_time < reject_pr_mean_time):
                        
                        if(pr_id not in self.less_than_rejected_pr_ranking):
                            self.less_than_rejected_pr_ranking[pr_id] = list()
                                                
                        list_files_in_pr = list(self.pull_request_files.find({"pull_request_id": pr["_id"]}))
                        
                        for file in list_files_in_pr:
                            
                            self.less_than_rejected_pr_ranking[pr_id].append(str(file["_id"]))
        
        return self.less_than_rejected_pr_ranking
    
    def find_more_than_rejected_prs(self, reject_pr_mean_time):
        
        decision = "APPROVED"
        
        for review in self.list_reviews:
            
            corresponding_pr = review["pull_request_id"]
            review_time = int(round(review["submitted_at"].timestamp()))
                        
            for pr in self.list_prs:
                
                if(corresponding_pr == pr["_id"] and review["state"] != decision):
                    
                    creation_time = int(round(pr["created_at"].timestamp()))
                                        
                    pr_id = str(pr["_id"])
                                                            
                    if(review_time - creation_time > reject_pr_mean_time):
                        
                        if(pr_id not in self.more_than_rejected_pr_ranking):
                            self.more_than_rejected_pr_ranking[pr_id] = list()
                                                
                        list_files_in_pr = list(self.pull_request_files.find({"pull_request_id": pr["_id"]}))
                        
                        for file in list_files_in_pr:
                            
                            self.more_than_rejected_pr_ranking[pr_id].append(str(file["_id"]))
        
        return self.more_than_rejected_pr_ranking
    
    def find_less_than_accepted_prs(self, reject_pr_mean_time):
        
        decision = "APPROVED"
        
        for review in self.list_reviews:
            
            corresponding_pr = review["pull_request_id"]
            review_time = int(round(review["submitted_at"].timestamp()))
                        
            for pr in self.list_prs:
                
                if(corresponding_pr == pr["_id"] and review["state"] == decision):
                    
                    creation_time = int(round(pr["created_at"].timestamp()))
                                        
                    pr_id = str(pr["_id"])
                                                            
                    if(review_time - creation_time < reject_pr_mean_time):
                        
                        if(pr_id not in self.less_than_accepted_pr_ranking):
                            self.less_than_accepted_pr_ranking[pr_id] = list()
                                                
                        list_files_in_pr = list(self.pull_request_files.find({"pull_request_id": pr["_id"]}))
                        
                        for file in list_files_in_pr:
                            
                            self.less_than_accepted_pr_ranking[pr_id].append(str(file["_id"]))
        
        return self.less_than_accepted_pr_ranking
    
    def find_more_than_accepted_prs(self, reject_pr_mean_time):
        
        decision = "APPROVED"
        
        for review in self.list_reviews:
            
            corresponding_pr = review["pull_request_id"]
            review_time = int(round(review["submitted_at"].timestamp()))
                        
            for pr in self.list_prs:
                
                if(corresponding_pr == pr["_id"] and review["state"] == decision):
                    
                    creation_time = int(round(pr["created_at"].timestamp()))
                                        
                    pr_id = str(pr["_id"])
                                                            
                    if(review_time - creation_time > reject_pr_mean_time):
                        
                        if(pr_id not in self.more_than_accepted_pr_ranking):
                            self.more_than_accepted_pr_ranking[pr_id] = list()
                                                
                        list_files_in_pr = list(self.pull_request_files.find({"pull_request_id": pr["_id"]}))
                        
                        for file in list_files_in_pr:
                            
                            self.more_than_accepted_pr_ranking[pr_id].append(str(file["_id"]))
        
        return self.more_than_accepted_pr_ranking
    
    def find_less_than_all_prs(self, reject_pr_mean_time):
                
        for review in self.list_reviews:
            
            corresponding_pr = review["pull_request_id"]
            review_time = int(round(review["submitted_at"].timestamp()))
                        
            for pr in self.list_prs:
                
                if(corresponding_pr == pr["_id"]):
                    
                    creation_time = int(round(pr["created_at"].timestamp()))
                                    
                    pr_id = str(pr["_id"])
                                                            
                    if(review_time - creation_time < reject_pr_mean_time):
                        
                        if(pr_id not in self.less_than_all_pr_ranking):
                            self.less_than_all_pr_ranking[pr_id] = list()
                                                
                        list_files_in_pr = list(self.pull_request_files.find({"pull_request_id": pr["_id"]}))
                        
                        for file in list_files_in_pr:
                            
                            self.less_than_all_pr_ranking[pr_id].append(str(file["_id"]))
                            
        return self.less_than_all_pr_ranking
    
    def find_more_than_all_prs(self, reject_pr_mean_time):
                
        for review in self.list_reviews:
            
            corresponding_pr = review["pull_request_id"]
            review_time = int(round(review["submitted_at"].timestamp()))
                        
            for pr in self.list_prs:
                
                if(corresponding_pr == pr["_id"]):
                    
                    creation_time = int(round(pr["created_at"].timestamp()))
                                        
                    pr_id = str(pr["_id"])
                                                            
                    if(review_time - creation_time > reject_pr_mean_time):
                        
                        if(pr_id not in self.more_than_all_pr_ranking):
                            self.more_than_all_pr_ranking[pr_id] = list()
                                                
                        list_files_in_pr = list(self.pull_request_files.find({"pull_request_id": pr["_id"]}))
                        
                        for file in list_files_in_pr:
                            
                            self.more_than_all_pr_ranking[pr_id].append(str(file["_id"]))
        
        return self.more_than_all_pr_ranking