/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.hadoop.job;


public class JobConfigConstants {
  // Required
  public static final String PUSH_LOCATION = "push.location";
  public static final String PATH_TO_INPUT = "path.to.input";
  public static final String SEGMENT_TABLE_NAME = "segment.table.name";

  // Customized for specific legacy naming schemes. Please do not use otherwise.
  public static final String SEGMENT_NAME_KEY = "segment.naming.key";

  // Internal to Pinot only
  public static final String TABLE_RAW_INDEX_COLUMNS = "table.raw.index.columns";
  public static final String SEGMENT_PARTITIONERS = "segment.partitioners";

  // Miscellaneous Constants
  public static final String SEGMENT_NAME_KEYED_PREFIX = "segmentname.keyedprefix.";
  public static final String SEPARATOR = ",'";
  public static final String TARGZ = ".tar.gz";

  public static final String TIME_COLUMN_NAME = "table.time.column.name";
  public static final String TIME_COLUMN_TYPE = "table.time.column.type";
  public static final String TABLE_PUSH_TYPE = "table.push.type";
  public static final String TABLE_PUSH_FREQUENCY = "table.push.frequency";
  public static final String SEGMENT_PREFIX = "segmentname.prefix";
  public static final String SEGMENT_EXCLUDE_SEQUENCE_ID = "segmentname.exclude.sequenceid";
  public static final String PATH_TO_OUTPUT = "path.to.output";

  // This parameter takes in the maximum number of segments we want to push at the same time within a colo.
  // EX: if push location is EI and push.parallelism.segments is set to 5?
  // EX2: if push location is EI and push.parallelism.segments is 5, we will push 10 segments at the same time, 5 to ltx1 and 5 to lca1
  // If push-location is prod and push.parallelism.segments is set to 2, then we will push 8 segments at the same time, 2 to each prod colo
  public static final String PUSH_PARALLELISM_SEGMENTS = "push.parallelism.segments";

  public static final String MAX_TIMEOUT_SEGMENT_PUSH_SECONDS = "max.timeout.segment.push.seconds";

  public static final String TABLE_INVERTED_INDEX_COLUMNS = "table.inverted.index.columns";

  // Get time format from schema to name segments correctly if time column is of type simple date
  public static final String SCHEMA_TIME_FORMAT = "schema.time.format";
}
