import re
from functools import reduce

import pygraphviz as pgv
import networkx as nx

import sys
from networkx import DiGraph


def create_graph(filename: str) -> dict[set[int]]:
    ses_tid_map = {}

    read_logs = dict(list())
    write_logs = dict(list())
    out = dict(set())

    with open(filename, 'rb') as f:  # Open in binary mode
        raw_contents = f.read()

    # Decode content while replacing invalid characters
    contents = raw_contents.decode('utf-8', errors='replace')

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
            continue

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
    return reduce(
        lambda acc, curr: max(
            acc,
            int(match.group(1)) if (match := re.search(r'Tx:\s*(\S+)', curr)) else acc
        ),
        statements,
        -1
    )

def out_vf3(graph, with_comments=True, file_path=None):
    """
    Generate and output the VF3 graph representation from a given graph.

    This function takes a graph (in adjacency list form), processes it to
    assign node IDs, and generates an output in VF3-compatible format. The
    output can include comments for readability, and it can be written to
    a specified file or printed to the console.

    Parameters:
    ----------
    graph : dict
        A dictionary representing the graph as an adjacency list. Keys are
        node identifiers, and values are lists of neighboring node identifiers.
        Example:
            {
                "A": ["B", "C"],
                "B": ["C"],
                "C": []
            }
    with_comments : bool, optional
        If True, adds explanatory comments to the output for better readability
        (default is True).
    file_path : str or None, optional
        If provided, the output is written to the specified file. If None,
        the output is printed to the console (default is None).

    Output Format:
    --------------
    The output includes:
    1. The number of nodes in the graph.
    2. Node attributes: Node ID and its original identifier.
    3. Edges for each node: The number of edges and their source and target
       node IDs.

    Notes:
    ------
    - The function assigns node IDs sequentially from 0 to N-1 based on the
      keys of the input dictionary.
    - Comments are optional and can be toggled using the `with_comments` parameter.
    """
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

    # Write to file or print to console
    if file_path:
        with open(file_path, "w") as file:
            file.write(result)
    else:
        print(result)


def create_digraph_generic(adj_list: dict[set[int]]) -> DiGraph:
    G = nx.DiGraph()

    for node, neighbors in adj_list.items():
        G.add_node(node)
        for neighbor in neighbors:
            G.add_edge(node, neighbor)

    return G


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