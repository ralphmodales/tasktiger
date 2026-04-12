#!/bin/bash
set -e
cd "$(dirname "$0")"
python -m pytest tests/test_rate_limiter.py -x -v --junitxml=test_results.xml --tb=short
