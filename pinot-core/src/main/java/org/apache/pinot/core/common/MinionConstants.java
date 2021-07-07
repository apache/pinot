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
  public static final String TASK_BUCKET_GRANULARITY_SUFFIX = ".bucketGranularity";

  public static final String TABLE_NAME_KEY = "tableName";
  public static final String SEGMENT_NAME_KEY = "segmentName";
  public static final String DOWNLOAD_URL_KEY = "downloadURL";
  public static final String UPLOAD_URL_KEY = "uploadURL";
  public static final String DOT_SEPARATOR = ".";
  public static final String URL_SEPARATOR = ",";
  public static final String AUTH_TOKEN = "authToken";

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
  public static final String ENABLE_REPLACE_SEGMENTS_KEY = "enableReplaceSegments";

  public static class ConvertToRawIndexTask {
    public static final String TASK_TYPE = "ConvertToRawIndexTask";
    public static final String COLUMNS_TO_CONVERT_KEY = "columnsToConvert";
  }

  // Purges rows inside segment that match chosen criteria
  public static class PurgeTask {
    public static final String TASK_TYPE = "PurgeTask";
  }

  public static class MergeRollupTask {
    public static final String TASK_TYPE = "MergeRollupTask";

    public static final String MERGE_TYPE_KEY = "mergeType";
    public static final String GRANULARITY_KEY = "granularity";

    // Rollup aggregate function related configs
    public static final String AGGREGATE_KEY_PREFIX = "aggregate";

    // Merge properties related configs
    public static final String MERGE_KEY_PREFIX = "merge";
    public static final String BUFFER_TIME = "bufferTime";
    public static final String MAX_NUM_RECORDS_PER_SEGMENT = "maxNumRecordsPerSegment";
    public static final String MAX_NUM_RECORDS_PER_TASK = "maxNumRecordsPerTask";

    // Segment name generator related configs
    public static final String MERGED_SEGMENT_NAME_KEY = "mergedSegmentNameKey";
  }

  /**
   * Creates segments for the OFFLINE table, using completed segments from the corresponding REALTIME table
   */
  public static class RealtimeToOfflineSegmentsTask {
    public static final String TASK_TYPE = "RealtimeToOfflineSegmentsTask";

    /**
     * The time window size for the task.
     * e.g. if set to "1d", then task is scheduled to run for a 1 day window
     */
    public static final String BUCKET_TIME_PERIOD_KEY = "bucketTimePeriod";
    /**
     * The time period to wait before picking segments for this task
     * e.g. if set to "2d", no task will be scheduled for a time window younger than 2 days
     */
    public static final String BUFFER_TIME_PERIOD_KEY = "bufferTimePeriod";

    // Window start and window end set by task generator
    public static final String WINDOW_START_MS_KEY = "windowStartMs";
    public static final String WINDOW_END_MS_KEY = "windowEndMs";
    // Segment processing related configs
    public static final String TIME_COLUMN_TRANSFORM_FUNCTION_KEY = "timeColumnTransformFunction";
    public static final String COLLECTOR_TYPE_KEY = "collectorType";
    public static final String AGGREGATION_TYPE_KEY_SUFFIX = ".aggregationType";
    public static final String MAX_NUM_RECORDS_PER_SEGMENT_KEY = "maxNumRecordsPerSegment";
  }

  // Generate segment and push to controller based on batch ingestion configs
  public static class SegmentGenerationAndPushTask {
    public static final String TASK_TYPE = "SegmentGenerationAndPushTask";
    public static final String CONFIG_NUMBER_CONCURRENT_TASKS_PER_INSTANCE =
        "SegmentGenerationAndPushTask.numConcurrentTasksPerInstance";
  }

}
