#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import argparse
import requests
import time
import logging
import sys

# === Constants ===
DEFAULT_QUERY_FILE = "query.txt"
DEFAULT_API_URL = "http://localhost:8000/timeseries/api/v1/query_range"
DEFAULT_DURATION_SECONDS = 3600
AUTH_TOKEN = "Basic YWRtaW46dmVyeXNlY3JldA=="

# === Logger Setup ===
def setup_logger():
    logger = logging.getLogger("TimeseriesQuery")
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    return logger

# === File Reader ===
def read_query_file(path, logger):
    try:
        with open(path, 'r') as f:
            return f.read()
    except Exception as e:
        logger.error(f"Failed to read query file '{path}': {e}")
        sys.exit(1)

# === Request Builder ===
def build_request(query, start, end, token=None):
    params = {
        'language': 'm3ql',
        'start': start,
        'end': end,
        'query': query
    }
    headers = {}
    if token:
        headers['Authorization'] = token
    return params, headers

# === Main Entry ===
def main():
    parser = argparse.ArgumentParser(description="Query a time series API endpoint.")
    parser.add_argument('--query-file', default=DEFAULT_QUERY_FILE, help=f'Path to the query file (default: {DEFAULT_QUERY_FILE})')
    parser.add_argument('--url', default=DEFAULT_API_URL, help=f'API endpoint URL (default: {DEFAULT_API_URL})')
    parser.add_argument('--auth', action='store_true', help='Enable Authorization header using default token')
    parser.add_argument('--duration', type=int, default=DEFAULT_DURATION_SECONDS, help=f'Query time range duration in seconds (default: {DEFAULT_DURATION_SECONDS})')
    args = parser.parse_args()

    logger = setup_logger()
    logger.info(f"Using query file: {args.query_file}")
    query = read_query_file(args.query_file, logger)

    start_time = int(time.time())
    end_time = start_time + args.duration

    token = AUTH_TOKEN if args.auth else None
    params, headers = build_request(query, start_time, end_time, token)

    try:
        logger.info(f"Sending request to {args.url}")
        response = requests.get(args.url, params=params, headers=headers)
        logger.info(f"Response text: {response.text}")
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Request failed: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
