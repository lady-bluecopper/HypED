#!/bin/bash

## path to the graph folders 
file_path='./data'
## name of the query file (space-separated src-dest pairs)
query_name='queries.txt'
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
					query_file="${file_path}/${dataset}/${s}/${query_name}"
					if [ -e ${query_file} ]; then
						num_queries=$(wc -l < ${query_file})
						if [ ${num_queries} -gt 0 ]; then
							runtime=0
	                        for ((i=1;i<=${num_queries};i++)); do
	                            pair=$(cat ${query_file} | head -${i} | tail -1)
								IFS=' ' read -ra VERT <<< "$pair"
	                            start=$(($(date +%s%N)/1000000))
								./run query-dis ${file_path}/${dataset}/${s}/ $t ${VERT[0]} ${VERT[1]}
	                            end=$(($(date +%s%N)/1000000))
	                            let "runtime+=$((end-start))"
	                        done
	                        echo "${dataset} ${s} ${runtime}"
						fi
					fi
				fi
			fi
		done
	done
done
