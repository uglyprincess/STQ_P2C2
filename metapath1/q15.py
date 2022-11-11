from pymongo import MongoClient

class smart_shark:
    
    def __init__(self):
        
        self.accept_pr_mean_time = 0
        
        self.accepted_pr_ranking = dict()
    
        self.client = MongoClient("mongodb://localhost:27017/")
    
        self.db = self.client["smartshark"] 
        
    def find_mean_accepted_prs(self):
        
        target_repo = "https://github.com/apache/giraph"
        
        self.pull_request_reviews = self.db["pull_request_review"]
        self.pull_request = self.db["pull_request"]
        
        self.list_reviews = list(self.pull_request_reviews.find({}))
        self.list_prs = list(self.pull_request.find({"target_repo_url": target_repo}))
        # self.list_prs = list(self.pull_request.find({}))
        
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
        print(f"Average acceptance time for Accepted PRs is {self.accept_pr_mean_time} seconds")
        
    def find_less_than_mean_prs(self):
        
        target_repo = "https://github.com/apache/giraph"
        
        self.pull_request_reviews = self.db["pull_request_review"]
        self.pull_request = self.db["pull_request"]
        
        self.list_reviews = list(self.pull_request_reviews.find({"creator_id": {"$exists": True}}))
        self.list_prs = list(self.pull_request.find({"target_repo_url": target_repo}))
        # self.list_prs = list(self.pull_request.find({}))
        
        decision = "APPROVED"
        
        for review in self.list_reviews:
            
            reviewer_id = str(review["creator_id"])
            
            corresponding_pr = review["pull_request_id"]
            review_time = int(round(review["submitted_at"].timestamp()))
                        
            for pr in self.list_prs:
                
                if(corresponding_pr == pr["_id"] and review["state"] == decision):
                    
                    creation_time = int(round(pr["created_at"].timestamp()))
                    
                    if(review_time - creation_time < self.accept_pr_mean_time):
                        
                        if(reviewer_id not in self.accepted_pr_ranking):
                            self.accepted_pr_ranking[reviewer_id] = 1
                        else:
                            self.accepted_pr_ranking[reviewer_id] = self.accepted_pr_ranking[reviewer_id] + 1
                                                
        for rejected_prs in dict(sorted(self.accepted_pr_ranking.items(), key=lambda item: item[1])):
            print(f"Reviewer {rejected_prs} reviewed {self.accepted_pr_ranking[rejected_prs]} PRs in less than the mean time")
                
                                    
if __name__ == "__main__":
    
    STQ = smart_shark()
    STQ.find_mean_accepted_prs()
    STQ.find_less_than_mean_prs()
        
        