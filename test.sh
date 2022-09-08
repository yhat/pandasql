#!/usr/bin/env bash
set -eou pipefail
python3 ./tests/test_pandasql.py
python3 ./tests/test_utils.py
