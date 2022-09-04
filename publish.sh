#!/usr/bin/env bash
set -eou pipefail
py -m pip install --upgrade build
py -m build
py -m pip install --upgrade twine
py -m twine upload dist/*
