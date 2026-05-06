#!/usr/bin/env python3
"""
verification/verify_task61_streaming.py
Task 6.1 — stream_dbt_layer() event streaming verification
Tests NodeStart/NodeFinished event generation and exit event presence.
"""

import sys
import json
sys.path.insert(0, '/app')

from pipeline.dbt_runner import stream_dbt_layer

try:
    events = []
    model_vars = {
        "target_date": "2024-01-01",
        "run_id": "verify-streaming-001",
        "target_weeks": "[]"
    }

    print("Calling stream_dbt_layer with tag='silver'...")
    for event in stream_dbt_layer('silver', model_vars['run_id'], model_vars):
        events.append(event)
        print(f"  Event: {event.get('event')} - {event.get('model', event.get('returncode'))}")

    if len(events) == 0:
        print("✗ No events yielded from stream_dbt_layer")
        sys.exit(1)

    print(f"✓ Received {len(events)} event(s)")

    # Check that we have NodeStart and NodeFinished events
    event_types = [e.get('event') for e in events]
    has_start = 'start' in event_types
    has_finish = 'finish' in event_types
    has_exit = 'exit' in event_types

    if has_start:
        print("✓ At least one NodeStart event received")
    else:
        print("✗ No NodeStart events found")

    if has_finish:
        print("✓ At least one NodeFinished event received")
    else:
        print("✗ No NodeFinished events found")

    if has_exit:
        print("✓ Exit event received at stream completion")
    else:
        print("✗ No exit event found")

    # Verify exit event is last
    last_event = events[-1]
    if last_event.get('event') == 'exit':
        print("✓ Exit event is final event in stream")
        exit_code = last_event.get('returncode')
        print(f"  dbt exit code: {exit_code}")
    else:
        print(f"✗ Last event is not exit: {last_event.get('event')}")
        sys.exit(1)

    # Check event structure
    event_keys_ok = True
    for event in events:
        if event.get('event') == 'start':
            required = {'event', 'model', 'started_at'}
            if not required.issubset(set(event.keys())):
                print(f"✗ NodeStart event missing keys: {required - set(event.keys())}")
                event_keys_ok = False
        elif event.get('event') == 'finish':
            required = {'event', 'model', 'status', 'started_at', 'completed_at'}
            if not required.issubset(set(event.keys())):
                print(f"✗ NodeFinished event missing keys: {required - set(event.keys())}")
                event_keys_ok = False
        elif event.get('event') == 'exit':
            required = {'event', 'returncode'}
            if not required.issubset(set(event.keys())):
                print(f"✗ Exit event missing keys: {required - set(event.keys())}")
                event_keys_ok = False

    if event_keys_ok:
        print("✓ All events have expected key structure")
    else:
        sys.exit(1)

    print("\nAll streaming checks passed.")

except Exception as e:
    print(f"✗ Unexpected error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
