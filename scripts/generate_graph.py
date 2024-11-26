import re
from functools import reduce
import pygraphviz as pgv

import sys


def create_graph(filename: str) -> dict[set[int]]:
    ses_tid_map = {}

    read_logs = dict(list())
    write_logs = dict(list())
    out = dict(set())

    with open(filename, 'r') as f:
        contents = f.read()

    statements = re.split(r'\$_\$_\$\s*', contents)
    statements = [stmt.strip() for stmt in statements if stmt.strip()]
    maxId = findMaxId(statements) + 1

    line_number = 0
    for line in statements:
        line_number += 1
        # Parse the operation and transaction ID
        match_op = re.search(r'Op:\s*(\S+)', line)
        match_tx = re.search(r'Tx:\s*(\S+)', line)

        if not match_op or not match_tx:
            print(f"Error parsing line {line_number}: {line}")
            sys.exit(1)

        op = match_op.group(1)
        ses_id = int(match_tx.group(1))
        txid = ses_id

        if ses_id not in ses_tid_map:
            ses_tid_map[ses_id] = txid
        else:
            txid = ses_tid_map[ses_id]

        # Process the operation
        if op == 'BEGIN':
            if (txid - maxId) in out:
                out[txid - maxId].add(txid)
            out[txid] = set()
        elif op == 'COMMIT':
            ses_tid_map[ses_id] += maxId
            continue
        elif op == 'WRITE':
            obj = re.search(r'Obj:\s*(\S+)', line).group(1)

            if obj in write_logs:
                for preceding_xact in write_logs[obj]:
                    if preceding_xact == txid: continue
                    out[preceding_xact].add(txid)
            else:
                write_logs[obj] = []

            if obj in read_logs:
                for preceding_xact in read_logs[obj]:
                    if preceding_xact == txid: continue
                    out[preceding_xact].add(txid)

            write_logs[obj].append(txid)
        elif op == 'READ':
            obj = re.search(r'Obj:\s*(\S+)', line).group(1)
            if obj in write_logs and write_logs[obj][-1] is not txid:
                out[write_logs[obj][-1]].add(txid)

            if obj not in read_logs:
                read_logs[obj] = []

            read_logs[obj].append(txid)

    return out

def findMaxId(statements):
    return reduce(lambda acc, curr: max(acc, int(re.search(r'Tx:\s*(\S+)', curr).group(1))), statements, -1)


def out_vf3(graph, with_comments=True):
    # Assign node IDs to the nodes (IDs are from 0 to N-1)
    nodes = list(graph.keys())
    node_id_map = {node: idx for idx, node in enumerate(nodes)}
    num_nodes = len(nodes)

    # Start building the output
    output_lines = []
    append_conditional = lambda condition, statement: condition and output_lines.append(statement)

    # Add the number of nodes
    append_conditional(with_comments, "# Number of nodes")
    output_lines.append(str(num_nodes))
    append_conditional(with_comments, "")

    # Add node attributes
    append_conditional(with_comments, "# Node attributes")
    for node in nodes:
        node_id = node_id_map[node]
        output_lines.append(f"{node_id} {node}")
    append_conditional(with_comments, "")

    # Add edges for each node
    for node in nodes:
        node_id = node_id_map[node]
        neighbors = graph[node]
        append_conditional(with_comments, f"# Edges coming out of node {node_id} (initially {node})")
        output_lines.append(str(len(neighbors)))
        for neighbor in neighbors:
            edge_start = node_id
            edge_end = node_id_map[neighbor]
            output_lines.append(f"{edge_start} {edge_end}")
        append_conditional(with_comments, "")

    # Join the lines with newline characters
    result = "\n".join(output_lines)
    print(result)


def visualize_graph(adj_list, output_file):
    """
    Visualize a directed graph from an adjacency list and save the output as an image or other file formats.

    :param adj_list: Dictionary where keys are nodes, and values are sets of adjacent nodes.
    :param output_file: Path to the output file (e.g., 'graph.png', 'graph.pdf').
    """
    # Create a directed graph using PyGraphviz
    graph = pgv.AGraph(directed=True)

    # Add nodes and edges from adjacency list
    for node, neighbors in adj_list.items():
        graph.add_node(node, label=str(node))  # Add node with label
        for neighbor in neighbors:
            graph.add_edge(node, neighbor)

    # Configure graph aesthetics (optional)
    graph.graph_attr.update(dpi=300)
    graph.node_attr.update(shape='circle', style='filled', fillcolor='lightblue')
    graph.edge_attr.update(fontsize=10, fontname='Arial')

    # Write the graph to the specified output file
    graph.draw(output_file, prog='dot')


if __name__ == '__main__':
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print("Usage: python generate_graph.py <filename> [<output_image>]")
        sys.exit(1)
    file = sys.argv[1]
    res = create_graph(file)
    if len(sys.argv) > 2:
        visualize_graph(res, sys.argv[2])
    out_vf3(res, False)