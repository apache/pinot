--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.
--

CREATE TABLE segment_stats (
  table_name    TEXT NOT NULL,
  segment_name  TEXT NOT NULL,
  crc           INTEGER NOT NULL,
  total_docs    INTEGER NOT NULL,
  size_bytes    INTEGER NOT NULL,
  start_time_ms INTEGER NOT NULL,
  end_time_ms   INTEGER NOT NULL,
  consuming     INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL,
  PRIMARY KEY (table_name, segment_name)
);
CREATE TABLE segment_col_stats (
  table_name     TEXT NOT NULL,
  segment_name   TEXT NOT NULL,
  column_name    TEXT NOT NULL,
  ndv            INTEGER NOT NULL,
  min_value      TEXT,
  max_value      TEXT,
  min_trusted    INTEGER NOT NULL,
  avg_bytes      REAL NOT NULL,
  null_fraction  REAL NOT NULL,
  updated_at_ms  INTEGER NOT NULL,
  PRIMARY KEY (table_name, segment_name, column_name)
);
CREATE INDEX idx_segment_stats_time ON segment_stats(table_name, start_time_ms, end_time_ms);
