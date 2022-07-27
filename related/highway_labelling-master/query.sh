#!/bin/bash

## path to the graph folders 
file_path='./data'
## space-separated list of graph names
datasets=( threads-stack-overflow-2 )
## space separated list of s values
# each value gives the name of the s-line graph
# of the hypergraph "graph": <graph>-<s>.txt
proj=( 1 2 3 4 5 6 7 8 9 10 )
# number of landmarks to test
lands=( 10 20 100 200 )

# answer queries for each s-line graph of each hypergraph in datasets
# we assume the query file is in the same folder of the graph file
# and is named <graph>-<s>-queries.txt
for l in ${lands[@]}
do
	echo "LANDS $l"
	for dataset in ${datasets[@]}
	do
		for s in ${proj[@]}
		do
			if [ -e ${file_path}/${dataset}-${s}.txt ]; then
				num_edges=$(wc -l < ${file_path}/${dataset}-${s}.txt)
                if [ $num_edges -gt 1 ]; then
					query_file="${file_path}/${dataset}-${s}-queries.txt"
					start=$(($(date +%s%N)/1000000))
		    		bin/query_distance ${file_path}/${dataset}-${s}.txt $l ./output_$l/${dataset}-${s} ${query_file}
					end=$(($(date +%s%N)/1000000))
                    runtime=$((end-start))
   					echo "${dataset} ${s} query ${runtime}"
				fi
			fi
		done
	done
done

