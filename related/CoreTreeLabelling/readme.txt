## compile
g++ -O3 -fopenmp -std=c++11 CoreTreeLabeling.cpp -o run

## convert datasets to bin files
## each graph must be in a folder and 
## the file name must be graph.txt
./run txt-to-bin fb/
@1 path of the graph folder

## construct tree index
./run decompose_bt fb/ 100 32
@1 path of the graph folder
@2 tree-width allowed
@3 number of threads

## construct core index
./run decompose_core fb/ 100 32
@1 path of the graph folder
@2 tree-width allowed
@3 number of threads

## convert datasets and construct all the indices
## at once, for a given tree-width
preprocessing.sh 100
@1 tree-width allowed

## run batch of queries
./run query-batch fb/ 100 100000
@1 path of the graph folder
@2 tree-width allowed
@3 number of queries

## run single query
./run query-dis fb/ 100 1 2
@1 path of the graph folder
@2 tree-width allowed
@3 start vertex
@4 target vertex

## run all queries in a query file
query.sh 100
@1 tree-width allowed