import numpy as np
from collections import defaultdict

class Calc:
    
    def construct_ranks(metapaths, dic, ranks_hrank):
        
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
    
    def transition_probability_matrix(metapaths):
        
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
                    
        # print(W)
        return W, dic
    
    def get_Mp(U):

        Mp = U[0]
        for i in range(1, len(U)):
            Mp = Mp @ U[i]
        row_sums = Mp.sum(axis=1)[:, None]
        for i, sum_ in enumerate(row_sums):
            if sum_ > 0:
                Mp[i] /= sum_
        return Mp
    
    def hrank_symmetric(Mp, alpha, n_iter):
        
        length = len(Mp)
        rank = np.zeros(length) + 1 / length
        for i in range(n_iter):
            rank = alpha * rank @ Mp + (1 - alpha) / length
        return rank
    
    def hrank_asymmetric(Mp, Mp_inv, alpha, n_iter):
        
        length1 = len(Mp)
        length2 = len(Mp_inv)
        rank = np.zeros(length1) + 1 / length1
        rank_inv = np.zeros(length2) + 1 / length2
        for i in range(n_iter):
            rank_inv = alpha * rank @ Mp + (1 - alpha) / length2
            rank = alpha * rank_inv @ Mp_inv + (1 - alpha) / length1
        return rank, rank_inv
    
class HRank:
        
    def perform_asymmetric(metapaths):
            
        reversed_metapaths = list()
        for dic in metapaths:
            reversed_metapaths.append(dict(reversed(list(dic.items()))))
        
        # Create H-Rankings
        
        U, dic = Calc.transition_probability_matrix(metapaths)
        Mp = Calc.get_Mp(U)
        
        inv_U, inv_dic = Calc.transition_probability_matrix(reversed_metapaths)
        inv_Mp = Calc.get_Mp(inv_U)
        
        alpha = 0.87
        number_of_iterations = 15
        
        ranks_hrank, _ = Calc.hrank_asymmetric(Mp, inv_Mp, alpha, number_of_iterations)
        ranks3 = Calc.construct_ranks(metapaths, dic, ranks_hrank)
        
        print(dict(ranks3))
        
        return dict(ranks3)