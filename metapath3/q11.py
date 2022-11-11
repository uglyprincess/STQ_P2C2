## D ---> PR ---> PR Comment ---> Developer


from pymongo import MongoClient
import numpy as np
from collections import defaultdict

class smart_shark:
    
    def __init__(self):
        
        self.project = "giraph"
        
        self.reject_pr_mean_time = 0
        
        self.less_rejected_pr_ranking = list()
    
        # self.more_rejected_pr_ranking = dict()
    
        self.client = MongoClient("mongodb://localhost:27017/")
    
        self.db = self.client["smartshark"] 
        
    def find_mean_rejected_prs(self):
            
        target_repo = "https://github.com/apache/" + self.project
        
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
                
                if(corresponding_pr == pr["_id"] and review["state"] != decision):
                    
                    creation_time = int(round(pr["created_at"].timestamp()))
                    
                    # print(f"Review time is {int(round(review_time.timestamp()))}")
                    # print(f"Submission time is {int(round(creation_time.timestamp()))}")
                    
                    total_time = total_time + review_time - creation_time
                    total_reviews = total_reviews + 1
                    
        self.reject_pr_mean_time = int(round(total_time/total_reviews))
        print(f"Average rejection time for Rejected PRs is {self.reject_pr_mean_time} seconds")
        
    def find_less_than_mean_prs(self):
        
        target_repo = "https://github.com/apache/" + self.project
        
        self.pull_request_reviews = self.db["pull_request_review"]
        self.pull_request = self.db["pull_request"]
        self.pull_request_files = self.db["pull_request_file"]
        
        self.list_reviews = list(self.pull_request_reviews.find({"creator_id": {"$exists": True}}))
        self.list_prs = list(self.pull_request.find({"target_repo_url": target_repo, "creator_id": {"$exists": True}}))
        
        decision = "APPROVED"
        
        for review in self.list_reviews:
            
            reviewer_id = str(review["creator_id"])
            review_id = str(review["_id"])
            
            corresponding_pr = review["pull_request_id"]
            review_time = int(round(review["submitted_at"].timestamp()))
                        
            for pr in self.list_prs:
                
                if(corresponding_pr == pr["_id"] and review["state"] != decision):
                    
                    creation_time = int(round(pr["created_at"].timestamp()))
                    
                    developer_id = str(pr["creator_id"]) 
                    
                    pr_id = str(pr["_id"])
                                                            
                    if(review_time - creation_time < self.reject_pr_mean_time):
                        
                        # print(f"Got a PR here: {pr_id}")
                        
                        list_files_in_pr = list(self.pull_request_files.find({"pull_request_id": pr["_id"]}))
                        
                        for file in list_files_in_pr:
                        
                            # print("Great success!")
                        
                            # self.less_rejected_pr_ranking[developer_id] = review_time - creation_time
                            self.less_rejected_pr_ranking.append(
                                {
                                    "developer": developer_id,
                                    "pull_request": pr_id,
                                    "pull_request_review": review_id,
                                    "reviewer": reviewer_id,
                                }
                            )
        
        # for doc in self.less_rejected_pr_ranking:
        #     print(doc)
        
        return self.less_rejected_pr_ranking

    def construct_ranks(self, metapaths, dic, ranks_hrank):
        
        keys = list(metapaths[0].keys())
        vals = list(dic[keys[0]])
        ranks_unsorted = defaultdict(int)

        for i in range(len(ranks_hrank)):
            ranks_unsorted[vals[i]] = ranks_hrank[i]
            ranks_sorted = sorted(ranks_unsorted.items(), key=lambda kv: kv[1], reverse=True)
            ranks_final = defaultdict(int)
        for i, (key, val) in enumerate(ranks_sorted):
            ranks_final[key] = i + 1
            
        return ranks_final
    
    def transition_probability_matrix(self, metapaths):
        is_path = defaultdict(lambda: defaultdict(int))
        dic = defaultdict(set)
        for i, path in enumerate(metapaths):
            keys, vals = list(path.keys()), list(path.values())
        for i in range(len(keys) - 1):
            is_path[vals[i]][vals[i + 1]] = 1
            dic[keys[i]].add(vals[i])
            dic[keys[-1]].add(vals[-1])
        # Adjacency Matrix (W)
        W = []
        keys, vals = list(dic.keys()), list(dic.values())
        for i in range(len(keys) - 1):
            A1, A2 = keys[i], keys[i + 1]
            len1, len2 = len(vals[i]), len(vals[i + 1])
            adj = np.zeros((len1, len2))
            for j, x1 in enumerate(vals[i]):
                for k, x2 in enumerate(vals[i + 1]):
                    adj[j][k] = is_path[x1][x2]
            
            W.append(adj)
        # Transition Probability Matrix (U = W / |W|)
        for i, w in enumerate(W):
            row_sums = w.sum(axis=1)[:, None]
            for j, sum_ in enumerate(row_sums):
                if sum_ > 0:
                    W[i][j] /= sum_
        print(W[0])
        
        return W, dic
    
    def get_Mp(self, metapaths, U):

        Mp = U[0]
        print(Mp.shape)
        for i in range(1, len(U)):
            Mp = Mp @ U[i]
        row_sums = Mp.sum(axis=1)[:, None]
        for i, sum_ in enumerate(row_sums):
            if sum_ > 0:
                Mp[i] /= sum_
                
        print("Metapath Matrix:", Mp)
        return Mp
    
    def hrank_SY(self, Mp, alpha, n_iter):

        length = len(Mp)
        rank = np.zeros(length) + 1 / length
        for i in range(n_iter):
            rank = alpha * rank @ Mp + (1 - alpha) / length
        return rank
                                    
if __name__ == "__main__":
    
    STQ = smart_shark()

    # Populating Metapaths
    
    STQ.find_mean_rejected_prs()
    metapaths = STQ.find_less_than_mean_prs()
    
    # Create H-Rankings
    
    U, dic = STQ.transition_probability_matrix(metapaths)
    Mp = STQ.get_Mp(metapaths, U)
    alpha = 0.87
    number_of_iterations = 15
    
    ranking = STQ.hrank_SY(Mp, alpha, number_of_iterations)
    final_ranking = STQ.construct_ranks(metapaths, dic, ranking)
    
    


        
 