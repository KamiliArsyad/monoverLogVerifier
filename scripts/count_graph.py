import os

import sys
import time

import vf3py

from generate_graph import create_graph, create_digraph_generic, visualize_graph, out_vf3

FILE_NAME = 'output.log'

def are_isomorph(g1, g2):
    return vf3py.are_isomorphic(g1, g2)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python count_graph.py <directory>")
        exit(-1)

    base_dir = sys.argv[1]

    if not os.path.isdir(base_dir):
        print(f"The provided path '{base_dir}' is not a directory.")
        exit(-1)

    graphs = list()
    count = 0
    for root, dirs, files in os.walk(base_dir):
        if FILE_NAME in files:
            count += 1
            file_path = os.path.join(root, FILE_NAME)
            if count > 89:
                print(f"stop counting at {file_path}")
                break
            adj_list = create_graph(file_path)
            digraph = create_digraph_generic(adj_list)
            print(f"File {count}: {file_path.split('/')[6]}", end=" -- ")
            timer = time.time()
            seen = any(are_isomorph(digraph, graph) for graph in graphs)
            print(f"took {(time.time() - timer) * 1000} ms")

            if not seen:
                graphs.append(digraph)


    print(f"Number of non-isomorphic graphs: {len(graphs)}")