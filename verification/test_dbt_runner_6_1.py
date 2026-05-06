#!/usr/bin/env python3
"""
verification/test_dbt_runner_6_1.py
Task 6.1 verification: derive_execution_order and stream_dbt_layer
"""

import sys
sys.path.insert(0, '/app')

from pipeline.dbt_runner import derive_execution_order, CompileError

try:
    order = derive_execution_order()
    print("Silver order:", order['silver'])
    print("Gold order:", order['gold'])

    if 'silver' in order and 'gold' in order:
        print("✓ derive_execution_order returns both silver and gold keys")
    else:
        print("✗ Missing keys in order dict")
        sys.exit(1)

    if len(order['silver']) > 0:
        print(f"✓ Silver models found: {len(order['silver'])}")
    else:
        print("✗ No silver models found")
        sys.exit(1)

    if len(order['gold']) > 0:
        print(f"✓ Gold models found: {len(order['gold'])}")
    else:
        print("✗ No gold models found")
        sys.exit(1)

    if 'silver_accounts' in order['silver'] and 'silver_transactions' in order['silver']:
        idx_accts = order['silver'].index('silver_accounts')
        idx_txns = order['silver'].index('silver_transactions')
        if idx_accts < idx_txns:
            print("✓ silver_accounts appears before silver_transactions (topological order correct)")
        else:
            print("✗ silver_transactions appears before silver_accounts (dependency violation)")
            sys.exit(1)

    print("All checks passed.")
except CompileError as e:
    print(f"✗ CompileError raised: {e}")
    sys.exit(1)
except Exception as e:
    print(f"✗ Unexpected error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
