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


public class TableConfigConstants {
  public static final String TIME_COLUMN_NAME = "table.time.column.name";
  public static final String TIME_COLUMN_TYPE = "table.time.column.type";
  public static final String TABLE_PUSH_TYPE = "table.push.type";
  public static final String TABLE_PUSH_FREQUENCY = "table.push.frequency";
  public static final String SEGMENT_PREFIX = "segmentname.prefix";
  public static final String SEGMENT_EXCLUDE_SEQUENCE_ID = "segmentname.exclude.sequenceid";
  public static final String SEGMENT_NAME_KEYED_PREFIX = "segmentname.keyedprefix.";
  public static final String APPEND = "APPEND";
  public static final String REFRESH = "REFRESH";
  public static final String GENERATE_INVERTED_INDEX_BEFORE_PUSH = "generate.inverted.index.before.push";
}
