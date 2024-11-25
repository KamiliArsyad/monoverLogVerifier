import sys
import re

def validate_history(filename):
    active_transactions = {}
    line_number = 0

    with open(filename, 'r') as f:
        content = f.read()
    statements = re.split(r'\$_\$_\$\s*', content)
    statements = [stmt.strip() for stmt in statements if stmt.strip()]

    for line in statements:
        line_number += 1

        # Parse the operation and transaction ID
        match_op = re.search(r'Op:\s*(\S+)', line)
        match_tx = re.search(r'Tx:\s*(\S+)', line)

        if not match_op or not match_tx:
            print(f"Error parsing line {line_number}: {line}")
            sys.exit(1)

        op = match_op.group(1)
        txid = match_tx.group(1)

        # Process the operation
        if op == 'BEGIN':
            # txid = txid[:-2]
            if txid in active_transactions and active_transactions[txid]:
                print(f"Error: Nested BEGIN in transaction {txid} at line {line_number}")
                sys.exit(1)
            else:
                active_transactions[txid] = True
        elif op == 'COMMIT':
            # txid = txid[:-2]
            if txid not in active_transactions or not active_transactions[txid]:
                print(f"Error: COMMIT without BEGIN in transaction {txid} at line {line_number}")
                sys.exit(1)
            else:
                active_transactions[txid] = False
        else:
            # Other operations
            if txid not in active_transactions or not active_transactions[txid]:
                print(f"Error: Operation {op} in transaction {txid} outside of BEGIN ... COMMIT block at line {line_number}")
                sys.exit(1)

    # After processing all lines, check if any transaction is still active
    for txid, active in active_transactions.items():
        if active:
            print(f"Error: Transaction {txid} begins but does not commit")
            sys.exit(1)
    print("History is valid.")

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python validate_history.py <filename>")
        sys.exit(1)
    filename = sys.argv[1]
    validate_history(filename)
