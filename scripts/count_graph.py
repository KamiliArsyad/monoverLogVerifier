import os
import sys
import time
import subprocess
import tempfile

from generate_graph import create_graph, out_vf3

FILE_NAME = 'output.log'

def are_isomorph(g1_adj_list, g2_adj_list, vf3p_executable=None):
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
            '-t', '6',          # Number of threads (adjust as needed)
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

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python count_graph.py <directory> [path/to/vf3p]")
        exit(-1)

    base_dir = sys.argv[1]
    vf3p_bin = sys.argv[2] if len(sys.argv) > 2 else None

    if not os.path.isdir(base_dir):
        print(f"The provided path '{base_dir}' is not a directory.")
        exit(-1)

    graphs = []
    count = 0
    for root, dirs, files in os.walk(base_dir):
        if FILE_NAME in files:
            count += 1
            file_path = os.path.join(root, FILE_NAME)
            if count > 89:
                print(f"stop counting at {file_path}")
                break
            adj_list = create_graph(file_path)
            print(f"File {count}: {file_path.split('/')[6]}", end=" -- ")
            timer = time.time()
            seen = any(are_isomorph(adj_list, graph_adj_list, vf3p_bin) for graph_adj_list in graphs)
            print(f"took {(time.time() - timer) * 1000:.2f} ms")

            if not seen:
                graphs.append(adj_list)

    print(f"Number of non-isomorphic graphs: {len(graphs)}")
