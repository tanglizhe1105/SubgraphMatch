#Subgraph Matching

##Find all these subgraphs matching to query graph in data graph.
##The matching requirs vertex tag matching and edges matching

for example:
```
Data graph vertex(id, tag):
1 1
2 1
3 4
4 2
5 3
6 2

Data graph edge(id, id):
1 3
3 1
1 4
4 1
1 5
5 1
2 4
2 5
5 2
2 6
4 3
4 5
5 4

Query graph vertex(id, tag):
1 1
2 2
3 4
4 3

Query graph edge(id, id):
1 4
1 2
2 3
4 2

the matching result is:
1,4,3,5
2,4,3,5

These results means there exist 2 matched subgraph
the matched subgraph vertex id in data graph 1,4,3,5 response to query graph vertex 1,2,3,4 respectively
```

This project is about computing subgraph matching with spark in distributed environment.
The work's idea comes from following paper:
Sun, Zhao, et al. "Efficient subgraph matching on billion node graphs." Proceedings of the VLDB Endowment 5.9 (2012): 788-799.
Developed by Tang Lizhe, National Laboratory for Parallel and Distributed Processing, National University of Defense Technology, Changsha, China;