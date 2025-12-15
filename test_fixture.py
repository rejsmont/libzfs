#!/usr/bin/env python3
"""Quick test of the mock fixture."""

import subprocess
from unittest.mock import MagicMock, patch

# Simulate what the fixture does
mock_popen = MagicMock()
mock_queue = []

def setup_mock(stdout='', stderr='', returncode=0):
    """Helper to configure mock subprocess behavior for a single call."""
    process_mock = MagicMock()
    
    # Setup stdout
    if isinstance(stdout, list):
        process_mock.stdout.readline.side_effect = stdout + ['']
    else:
        process_mock.stdout.readline.return_value = stdout
    
    # Setup stderr
    if isinstance(stderr, list):
        process_mock.stderr.readline.side_effect = stderr + ['']
    else:
        process_mock.stderr.readline.return_value = stderr
    
    # Setup return code
    process_mock.poll.side_effect = [None] * len(stdout) + [returncode] if isinstance(stdout, list) else [returncode]
    
    # Set as return_value and also add to queue
    mock_popen.return_value = process_mock
    mock_queue.append(process_mock)
    return process_mock

def setup_multi(*configs):
    """Helper to configure multiple subprocess calls in sequence."""
    mock_queue.clear()
    mock_popen.side_effect = popen_side_effect  # Enable side_effect for multi-call
    for config in configs:
        if len(config) == 1:
            setup_mock(stdout=config[0])
        elif len(config) == 2:
            setup_mock(stdout=config[0], stderr=config[1])
        else:
            setup_mock(stdout=config[0], stderr=config[1], returncode=config[2])

def popen_side_effect(*args, **kwargs):
    if mock_queue:
        return mock_queue.pop(0)
    # Fallback
    fallback = MagicMock()
    fallback.stdout.readline.return_value = ''
    fallback.stderr.readline.return_value = ''
    fallback.poll.return_value = 0
    return fallback

# Test 1: Single setup
print("Test 1: Single setup")
setup_mock(['line1', 'line2'])
proc = mock_popen()
print(f"readline 1: {proc.stdout.readline()}")
print(f"poll 1: {proc.poll()}")
print(f"readline 2: {proc.stdout.readline()}")
print(f"poll 2: {proc.poll()}")
print(f"readline 3: {proc.stdout.readline()}")
print(f"poll 3: {proc.poll()}")
print()

# Test 2: Multi setup
print("Test 2: Multi setup")
setup_multi(
    (['line1'],),
    (['line2'],)
)
proc1 = mock_popen()
proc2 = mock_popen()
print(f"proc1 readline: {proc1.stdout.readline()}")
print(f"proc1 poll: {proc1.poll()}")
print(f"proc2 readline: {proc2.stdout.readline()}")
print(f"proc2 poll: {proc2.poll()}")
