import csv
import os
import sys
import time
import subprocess
import tempfile
import multiprocessing
import matplotlib.pyplot as plt

import vf3py

from generate_graph import create_graph, create_digraph_generic, out_vf3

FILE_NAME = 'output.log'

def are_isomorph_vf3p(g1_adj_list, g2_adj_list, vf3p_executable=None):
    VF3P_EXECUTABLE = '/path/to/vf3p' if vf3p_executable is None else vf3p_executable

    try:
        # Write g1 to a temporary file
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as tmp1:
            out_vf3(g1_adj_list, False, file_path=tmp1.name)
            g1_file = tmp1.name

        # Write g2 to a temporary file
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as tmp2:
            out_vf3(g2_adj_list, False, file_path=tmp2.name)
            g2_file = tmp2.name

        # Construct the command line arguments
        cmd = [
            VF3P_EXECUTABLE,
            '-a', '1',          # Parallel strategy version
            '-t', '8',          # Number of threads (adjust as needed)
            g1_file,
            g2_file
        ]

        # Run the VF3P executable
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        # Check for errors
        if result.returncode != 0:
            print(f"VF3P error: {result.stderr}")
            is_isomorphic = False
            exit(-1)
        else:
            output = result.stdout.strip()
            parts = output.split()
            if len(parts) >= 1:
                num_solutions = int(parts[0])
                is_isomorphic = num_solutions > 0
            else:
                is_isomorphic = False

    finally:
        # Clean up temporary files
        if os.path.exists(g1_file):
            os.unlink(g1_file)
        if os.path.exists(g2_file):
            os.unlink(g2_file)

    return is_isomorphic

def are_isomorph(g1_nx, g2_nx, g1_adj_list, g2_adj_list, vf3p_bin, timeout=5):
    # Try vf3py first with NetworkX graphs
    with multiprocessing.Pool(processes=1) as pool:
        bigger = g1_nx if len(g1_adj_list) > len(g2_adj_list) else g2_nx
        smaller = g1_nx if len(g1_adj_list) <= len(g2_adj_list) else g2_nx
        result = pool.apply_async(vf3py.has_subgraph, args=(smaller, bigger))
        try:
            is_iso = result.get(timeout=timeout)
            return is_iso
        except multiprocessing.TimeoutError:
            print(f"\nvf3py exceeded {timeout} seconds, switching to vf3p...")
        finally:
            pool.terminate()
            pool.join()

    # Use the adjacency lists with vf3p as a fallback
    bigger = g1_adj_list if len(g1_adj_list) > len(g2_adj_list) else g2_adj_list
    smaller = g1_adj_list if len(g1_adj_list) <= len(g2_adj_list) else g2_adj_list
    return are_isomorph_vf3p(smaller, bigger, vf3p_bin)

def write_results_to_csv(output_dir, results):
    output_csv = os.path.join(output_dir, 'results.csv')
    with open(output_csv, mode='w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Total Count', 'Number of Isomorphic', 'Elapsed Time (s)'])
        writer.writerows(results)

def save_plot_results(results, output_dir):
    """
    Plots the graph processing results and saves the plot to the output directory.

    Parameters:
        results (list of tuples): Each tuple contains (total_count, number_of_isomorphic, elapsed_time).
        output_dir (str): Directory where the plot will be saved.
    """
    # Extract data for plotting
    total_count = [r[0] for r in results]
    number_of_isomorphic = [r[1] for r in results]
    elapsed_time = [r[2] for r in results]

    plt.figure(figsize=(12, 6))

    # Plot 1: Total Count vs Elapsed Time
    plt.subplot(1, 2, 1)
    plt.plot(total_count, elapsed_time, marker='o')
    plt.xlabel('Total Count')
    plt.ylabel('Elapsed Time (s)')
    plt.title('Processing Time per Total Count')
    plt.grid()

    # Plot 2: Total Count vs Number of Isomorphic Graphs
    plt.subplot(1, 2, 2)
    plt.plot(total_count, number_of_isomorphic, marker='o', color='orange')
    plt.xlabel('Total Count')
    plt.ylabel('Number of Non-Isomorphic Graphs')
    plt.title('Non-Isomorphic Graphs over Total Count')
    plt.grid()

    plot_path = os.path.join(output_dir, 'results_plot.png')
    plt.tight_layout()
    plt.savefig(plot_path, dpi=300)
    plt.close()

    print(f"Plot saved to {plot_path}")


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: python count_graph.py <directory> <output_directory> [path/to/vf3p_binary]")
        exit(-1)

    base_dir = sys.argv[1]
    output_dir = sys.argv[2]
    vf3p_bin = sys.argv[3] if len(sys.argv) > 3 else None

    if not os.path.isdir(base_dir):
        print(f"The provided path '{base_dir}' is not a directory.")
        exit(-1)

    if not os.path.isdir(output_dir):
        print(f"The provided path '{output_dir}' is not a directory.")
        exit(-1)

    graphs = []  # List to store tuples of (NetworkX graph, adjacency list)
    results = []

    LIMIT = 10
    count = 0
    for root, dirs, files in os.walk(base_dir):
        if FILE_NAME not in files: continue

        count += 1
        file_path = os.path.join(root, FILE_NAME)
        if count > LIMIT:
            print(f"stop counting at {file_path}")
            break
        adj_list = create_graph(file_path)            # Get the adjacency list
        nx_graph = create_digraph_generic(adj_list)   # Create the NetworkX graph
        print(f"File {count}: {file_path.split('/')[6]} ---- {nx_graph}", end=" -- ")
        timer = time.time()

        # Check for isomorphism using both representations
        seen = False
        for existing_nx_graph, existing_adj_list in graphs:
            if are_isomorph(nx_graph, existing_nx_graph, adj_list, existing_adj_list, vf3p_bin):
                print(f"Previously seen on {existing_nx_graph}", end=" -- ")
                seen = True
                break

        elapsed_time = time.time() - timer
        print(f"took {elapsed_time * 1000:.2f} ms")

        if not seen:
            graphs.append((nx_graph, adj_list))

        total_seen = count
        cardinality_unique = len(graphs)
        results.append((total_seen, cardinality_unique, elapsed_time))

    write_results_to_csv(output_dir, results)
    save_plot_results(results, output_dir)
    print(f"Number of non-isomorphic graphs: {len(graphs)}")
