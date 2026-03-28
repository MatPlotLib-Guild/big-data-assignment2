#!/usr/bin/env python3

import os
import sys


for key in sorted(os.environ):
    key_lower = key.lower()
    if "input" in key_lower or "map" in key_lower:
        print(f"ENV\t{key}\t{os.environ[key]}")

for raw_line in sys.stdin:
    print(f"LINE\t{raw_line.rstrip()}")
    break
