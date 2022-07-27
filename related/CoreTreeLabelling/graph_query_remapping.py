#!/usr/bin/env python3

import sys
import time
import os

# directory with the original graph data
raw_dir = sys.argv[1]
# directory with the original query files
# we assume that the file name of the query file
# for a hypergraph "graph" is <graph>_queries.txt
query_dir = sys.argv[2]
# directory to store the remapped graphs and queries
data_dir = sys.argv[3]
# list of graph names
# we assume that the file name of the s-line graph 
# of a hypergraph "graph" is <graph>_S<s>.lg
graphs = ['threads-stack-overflow-2']
# max s value to consider (s-line graph)
smax = 10

# load graph and remap the ids from 0 to |V|
def remap_graph_ids(f, data_dir, graph, sv):
    vertex_map = dict()
    remapped = set()
    with open(f) as graph_f:
        for line in graph_f.readlines():
            lst = line.split(' ')
            fst = int(lst[0])
            sec = int(lst[1])
            if fst not in vertex_map:
                vertex_map[fst] = len(vertex_map)
            if sec not in vertex_map:
                vertex_map[sec] = len(vertex_map)
            if vertex_map[fst] < vertex_map[sec]:
                remapped.add((vertex_map[fst], vertex_map[sec]))
            else:
                remapped.add((vertex_map[sec], vertex_map[fst]))
    if not os.path.exists(f'{data_dir}/{graph}/{sv}'):
        os.makedirs(f'{data_dir}/{graph}/{sv}')
    out_path = f'{data_dir}/{graph}/{sv}/graph.txt'
    with open(out_path, 'w') as out_f:
        for t in remapped:
            out_f.write(f'{t[0]} {t[1]}\n')
    print(f'written {out_path}')
    return vertex_map


# load and remap the queries
def get_query_vertices(f, v_map):
    vertex_pairs = set()
    with open(f) as in_f:
        for line in in_f.readlines():
            lst = line.split(' ')
            first = v_map.get(int(lst[0]), -1)
            second = v_map.get(int(lst[1]), -1)
            if first != -1 and second != -1:
                vertex_pairs.add((first, second))
    return vertex_pairs


if __name__ == "__main__":
    for graph in graphs:
        for s in range(1, smax + 1):
            try:
                raw_path = f'{raw_dir}/{graph}_S{s}.lg'
                print(f'remapping graph {raw_path}...')
                vmap = remap_graph_ids(raw_path, data_dir, graph, s)
                file_path = f'{query_dir}/{graph}_queries.txt'
                print(f'preparing queries for {graph} {s}')
                queries = get_query_vertices(file_path, vmap)
                print(f'writing {len(queries)} queries for {graph} {s}...')
                with open(f'{data_dir}/{graph}/{s}/queries.txt', 'w') as out_f:
                    for t in queries:
                        out_f.write(f'{t[0]} {t[1]}\n')
                with open(f'{data_dir}/{graph}-{s}-vmap.txt', 'w') as out_f:
                    for k,v in vmap.items():
                        out_f.write(f'{k} {v}\n')
            except Exception as e:
                print(f'Exception {e}')
                continue
