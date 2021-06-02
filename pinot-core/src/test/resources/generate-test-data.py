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
import sys
from fastavro import writer

parser = argparse.ArgumentParser()
parser.add_argument('output_file', help='Output Avro data file')
parser.add_argument('--num_records', dest='num_records', default=1024, type=int, help='Number of records to generate (default: 1024)')
parser.add_argument('--num_time_buckets', dest='num_time_buckets', default=16, type=int, help='Number of time buckets')

args = parser.parse_args()

print 'Generating {} records'.format(args.num_records)

schema = {
    'name': 'TestRecord',
    'type': 'record',
    'fields': [
        { 'name': 'D0', 'type': 'string', 'pinotType': 'DIMENSION' },
        { 'name': 'D1', 'type': 'string', 'pinotType': 'DIMENSION' },
        { 'name': 'D2', 'type': 'string', 'pinotType': 'DIMENSION' },
        { 'name': 'daysSinceEpoch', 'type': 'long', 'pinotType': 'TIME' },
        { 'name': 'M0', 'type': 'long', 'pinotType': 'METRIC' },
        { 'name': 'M1', 'type': 'double', 'pinotType': 'METRIC' }
    ]
}

records = []

for i in xrange(args.num_records):
    record = {
        'D0': str(i % 2),
        'D1': str(i % 4),
        'D2': str(i % 8),
        'daysSinceEpoch': int(i % args.num_time_buckets),
        'M0': 1,
        'M1': 1.0
    }
    records.append(record)

print 'Writing {}'.format(sys.argv[1])

with open(sys.argv[1], 'wb') as out:
    writer(out, schema, records)
