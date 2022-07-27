#!/bin/bash

## path to the graph folders 
file_path='./data'
## space-separated list of graph names
# each graph name is the name of the folder
# where the corresponding s-line graphs and query files are placed
datasets=( threads-stack-overflow-2 )
## space separated list of s values
# each value is the name of the folder
# where the s-line graph and the query file are placed
proj=( 1 2 3 4 5 6 7 8 9 10 )
# number of tree-width values to test
tws=( 10 20 100 200 )


for t in ${tws[@]}
do
	echo "Tree Width $t"
	for dataset in ${datasets[@]}
	do
		for s in ${proj[@]}
		do
			if [ -e ${file_path}/${dataset}/${s}/graph.txt ]; then
				num_edges=$(wc -l < ${file_path}/${dataset}/${s}/graph.txt)
				if [ $num_edges -gt 1 ]; then
					start=$(($(date +%s%N)/1000000))
					./run txt-to-bin ${file_path}/${dataset}/${s}/
					end=$(($(date +%s%N)/1000000))
					runtime=$((end-start))
					echo "${dataset} ${s} convert ${runtime}"

					start=$(($(date +%s%N)/1000000))
					./run decompose_bt ${file_path}/${dataset}/${s}/ $t 32
					end=$(($(date +%s%N)/1000000))
					runtime=$((end-start))
					echo "${dataset} ${s} tree-index ${runtime}"

					start=$(($(date +%s%N)/1000000))
					./run decompose_core ${file_path}/${dataset}/${s}/ $t 32
					end=$(($(date +%s%N)/1000000))
					runtime=$((end-start))
					echo "${dataset} ${s} core-index ${runtime}"
				fi
			fi
		done
	done
done
