#!/usr/bin/env bash
set -eou pipefail
py ./tests/test_pandasql.py
py ./tests/test_utils.py
