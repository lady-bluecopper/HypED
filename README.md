# HypED

## Overview
This package includes an algorithm to answer point-to-point s-distance queries in hypergraphs, approximately.
The algorithm can answer three types of queries: vertex-to-hyperedge, vertex-to-vertex, and hyperedge-to-hyperedge.
This is achieved by constructing a distance oracle, which can be stored on disk for future usages. The distance oracle stores distances from landmark hyperedges to reachable hyperedges, so that the distance between two hyperedges can be approximated via triangle inequalities.
The algorithm requires in input a number of landmark *L* used to compute the desired oracle size *O = L x |E|*, where *|E|* is the number of hyperedges in the hypergraph. Please note that *L* is not the actual number of landmarks used by the distance oracle.

The package includes a Jupyter Notebook (*Results.ipynb*) with the results of the experimental evaluation of HypED, and the source code (*related*) of the two competitors considered in the evaluation.

## Content

	datasets/ .....
	related/.......
	scripts/ ......
	src/ ..........
	Results.ipynb..
	LICENSE .......

## Requirements

	Java JRE v1.8.0

## Input Format

The input file must be a space separated list of integers, where each integer represents a vertex and each line represents a hyperedge in the hypergraph.
The algorithm assumes that the file does not contain duplicate hyperedges.
The script *run.sh* assumes that the file extension is *.hg*.
The folder *datasets* includes all the datasets used in our experimental evaluation.

## Usage

You can use HypED either by running the script *run.sh* included in this package, or by running the following command:

	java -cp HypED.jar:lib/* eu.centai.hypeq.test.EvaluateQueryTime dataFolder=<input_data> outputFolder=<output_data> dataFile=<file_name> numLandmarks=<num_landmarks> samplePerc=<ratio_of_hyperedges_to_sample> landmarkSelection=<strategy_to_select_landmarks> numQueries=<number_of_random_queries_to_test> store=<whether_the_oracle_should_be_stored_on_disk>  landmarkAssignment=<landmark_assignment_strategy> lb=<min_cc_size> maxS=<max_min_overlap> alpha=<alpha> beta=<beta> seed=<seed> isApproximate=<whether_exact_distances_should_be_computed_as_well> kind=<type_of_query>

The command evaluates the performance of HypED on a set of *numQueries* random queries. To evaluate the performance of the algorithm on a specific set of queries, such queries must be stored in a space-separated file, given in input with the option *queryFile=<file_name>*. 
The code assumes that the query file is located in the same folder where the graph file is located.

### Using the Script

The value of each parameter used by HypED must be set in the configuration file *config.cfg*:

#### General Settings
 - input_data: path to the folder containing the graph file.
 - output_data: path to the folder to store the results.
 - landmarkSelection: how to select the landmarks within the s-connected components (random, degree, farthest, bestcover, between).
 - landmarkAssignment: how to assign landmarks to s-connected components (ranking, prob).
 - alpha: importance factor of the s-connected component sizes.
 - beta: importance factor of the min overlap size s.
 - seed: seed for reproducibility.
 - kind: type of distance query to answer (*vertex* for vertex-to-vertex, *edge* for hyperedge-to-hyperedge, *both* for vertex-to-hyperedge).
 - isApproximate: whether we want to compute only the approximate distances, or also the exact distances. 

#### Dataset-related Settings
 - Dataset names: names of the files (without file extension).
 - Default values: comma-separated list of default values for each dataset, i.e., number of landmarks, percentage of hyperedges to sample, number of queries, lower bound lb for a s-connected component size to be considered for landmark assignment, max min overlap s, whether the oracle should be saved on disk.
 - Num Landmarks: comma-separated list of landmark numbers to test.
 - Experimental flags: test to perform among (1) compare strategies to find the s-connected components, (2) compare HypED with two baselines, (3) compare the performance using different importance factors, (4) compare the landmark selection strategies, (5) answer random queries, (6) perform a search by random tag, (7) find the s-line graphs of the hypergraph.

Then, the arrays that store the names, the number of landmarks, and the experimental flags of each dataset to test must be declared at the beginning of the script *run.sh*.

## Output Format

The algorithm produces two output files: one contains the approximate distances, and the other one some statistics.

1. Output File: comma-separated list including src_id, dst_id, s, real s-distance (only if isApproximate was set to *False*), lower-bound to the s-distance, upper-bound to the s-distance, and approximate s-distance (computed as the median between lower and upper bound).
2. Statistics File: tab-separated list including dataset name, timestamp, oracle creation time, query time, max min overlap s, lower bound lb, number of landmarks L, actual number of landmarks, number of distance pairs stored in the oracle, number of queries answered, landmark selection strategy, landmark assignment strategy, alpha, and beta.     

## Related Code

The folder *related* includes the source code of the two competitor algorithms considered in our experimental evaluation.

CTL[1] (folder *CoreTreeLabelling*) improves the state-of-the-art 2-hop pruned landmark labeling approach, by first decomposing the input graph in a large core and a forest of smaller trees, and then constructing two different indices on the core-tree structure previously generated.
Distance queries can be answered exactly as the min between the distances provided by the two indices.

HL [2] (folder *highway_labelling-master*) is a landmark-based algorithm that first selects a set of *l* vertices, and then populates two indices: the highway and the distance index.
The distance index is populated starting BFSs from the *l* vertices, and is guaranteed to be minimal given that set of vertices.
At query time, the algorithm first finds an upper-bound to the distance exploiting the highway index, and then, finds the distance in a sparsified version of the original graph.

Both approaches are designed for connected graphs, and hence do not guarantee to provide exact answers when the graph is disconnected.
We used them to construct indices for the s-line graphs of the hypergraphs.

### Usage

Both approaches assume that the node ids take values in [0, |V|], where |V| is the total number of vertices in the graph.
If you need to remap the vertices (and hence the query files), you can use the Python script *graph_query_remapping.py*.
The script includes some comments on its usage.

The meta-structures used by the algorithms can be created using the bash script *preprocessing.sh*. 
This script includes some variables that must be properly set:
1. *file_path*: path to the graph files
2. *datasets*: space-separated list of graph names
3. *proj*: space separated list of s values, where each value gives the name of the s-line graph
4. other parameters: CTL requires a list of tree-witdh values (*tws*), while HL requires a list of numbers of vertices (*lands*)

The queries can be answered using the bash script *query.sh*.
This script includes some comments on its usage.

For further information, please refer to the readme files included in the folders, or to the original repositories [3, 4].

## License

This package is free for use (GNU General Public License). 

## References

[1] Wentao Li, Miao Qiao, Lu Qin, Ying Zhang, Lijun Chang, and Xuemin Lin. 2020. Scaling up distance labeling on graphs with core-periphery properties. In SIGMOD. 1367–1381.

[2] Muhammad Farhan, Qing Wang, Yu Lin, and Brendan Mckay. 2019. A highly scalable labelling approach for exact distance queries in complex networks. In EDBT.

[3] [Core-Tree Labelling Code](https://wentaoli-92.github.io/file/CTL_CODE_2020.zip)

[4] [Highway Labelling Repository](https://github.com/mufarhan/highway_labelling)
