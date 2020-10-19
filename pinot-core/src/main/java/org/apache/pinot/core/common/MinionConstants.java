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
package org.apache.pinot.core.common;

public class MinionConstants {
  private MinionConstants() {
  }

  public static final String TASK_TIME_SUFFIX = ".time";

  public static final String TABLE_NAME_KEY = "tableName";
  public static final String SEGMENT_NAME_KEY = "segmentName";
  public static final String DOWNLOAD_URL_KEY = "downloadURL";
  public static final String UPLOAD_URL_KEY = "uploadURL";
  public static final String URL_SEPARATOR = ",";

  /**
   * When minion downloads a segment to do work on, we will save that CRC. We will send that to the controller in an
   * If-Match header when we upload the segment to ensure that the minion-completed segment hasn't been replaced by
   * hadoop while minion has been working on it.
   */
  public static final String ORIGINAL_SEGMENT_CRC_KEY = "crc";

  /**
   * For retry policy.
   */
  public static final String MAX_NUM_ATTEMPTS_KEY = "maxNumAttempts";
  public static final String INITIAL_RETRY_DELAY_MS_KEY = "initialRetryDelayMs";
  public static final String RETRY_SCALE_FACTOR_KEY = "retryScaleFactor";

  public static final String TABLE_MAX_NUM_TASKS_KEY = "tableMaxNumTasks";

  public static class ConvertToRawIndexTask {
    public static final String TASK_TYPE = "ConvertToRawIndexTask";
    public static final String COLUMNS_TO_CONVERT_KEY = "columnsToConvert";
  }

  // Purges rows inside segment that match chosen criteria
  public static class PurgeTask {
    public static final String TASK_TYPE = "PurgeTask";
  }

  public static class MergeRollupTask {
    public static final String TASK_TYPE = "mergeRollupTask";
    public static final String MERGE_TYPE_KEY = "mergeTypeKey";
    public static final String MERGED_SEGMENT_NAME_KEY = "mergedSegmentNameKey";
  }

  public static class RealtimeToOfflineSegmentsTask {
    public static final String TASK_TYPE = "realtimeToOfflineSegmentsTask";
    // window
    public static final String WINDOW_START_MILLIS_KEY = "windowStartMillis";
    public static final String WINDOW_END_MILLIS_KEY = "windowEndMillis";
    // segment processing
    public static final String TIME_COLUMN_TRANSFORM_FUNCTION_KEY = "timeColumnTransformFunction";
    public static final String COLLECTOR_TYPE_KEY = "collectorType";
    public static final String AGGREGATION_TYPE_KEY_SUFFIX = ".aggregationType";
    public static final String MAX_NUM_RECORDS_PER_SEGMENT_KEY = "maxNumRecordsPerSegment";

  }
}
