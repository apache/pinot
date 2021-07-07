/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.hadoop.job;

/**
 * Internal-only constants for Hadoop MapReduce jobs. These constants are propagated across different segment creation
 * jobs. They are not meant to be set externally.
 */
public class InternalConfigConstants {
  public static final String TIME_COLUMN_CONFIG = "time.column";
  public static final String TIME_COLUMN_VALUE = "time.column.value";
  public static final String IS_APPEND = "is.append";
  public static final String SEGMENT_PUSH_FREQUENCY = "segment.push.frequency";
  public static final String SEGMENT_TIME_TYPE = "segment.time.type";
  public static final String SEGMENT_TIME_FORMAT = "segment.time.format";
  public static final String SEGMENT_TIME_SDF_PATTERN = "segment.time.sdf.pattern";

  // The operations of preprocessing that is enabled.
  public static final String PREPROCESS_OPERATIONS = "preprocessing.operations";

  // Partitioning configs
  public static final String PARTITION_COLUMN_CONFIG = "partition.column";
  public static final String NUM_PARTITIONS_CONFIG = "num.partitions";
  public static final String PARTITION_FUNCTION_CONFIG = "partition.function";

  public static final String SORTING_COLUMN_CONFIG = "sorting.column";
  public static final String SORTING_COLUMN_TYPE = "sorting.type";
  public static final String ENABLE_PARTITIONING = "enable.partitioning";

  @Deprecated
  // Use PREPROCESSING_MAX_NUM_RECORDS_PER_FILE.
  public static final String PARTITION_MAX_RECORDS_PER_FILE = "partition.max.records.per.file";
  // max records per file in each partition. No effect otherwise.
  public static final String PREPROCESSING_MAX_NUM_RECORDS_PER_FILE = "preprocessing.max.num.records.per.file";

  // Number of segments we want generated.
  public static final String PREPROCESSING_NUM_REDUCERS = "preprocessing.num.reducers";

  public static final String FAIL_ON_SCHEMA_MISMATCH = "fail.on.schema.mismatch";
}
