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

import java.util.EnumSet;
import org.apache.pinot.segment.spi.AggregationFunctionType;

import static org.apache.pinot.segment.spi.AggregationFunctionType.*;


public class MinionConstants {
  private MinionConstants() {
  }

  public static final String TASK_TIME_SUFFIX = ".time";

  public static final String TABLE_NAME_KEY = "tableName";
  public static final String SEGMENT_NAME_KEY = "segmentName";
  public static final String DOWNLOAD_URL_KEY = "downloadURL";
  public static final String UPLOAD_URL_KEY = "uploadURL";
  public static final String DOT_SEPARATOR = ".";
  public static final String URL_SEPARATOR = ",";
  public static final String SEGMENT_NAME_SEPARATOR = ",";
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

  /**
   * Cluster level configs
   */
  public static final String TIMEOUT_MS_KEY_SUFFIX = ".timeoutMs";
  public static final String NUM_CONCURRENT_TASKS_PER_INSTANCE_KEY_SUFFIX = ".numConcurrentTasksPerInstance";
  public static final String MAX_ATTEMPTS_PER_TASK_KEY_SUFFIX = ".maxAttemptsPerTask";

  /**
   * Table level configs
   */
  public static final String TABLE_MAX_NUM_TASKS_KEY = "tableMaxNumTasks";
  public static final String ENABLE_REPLACE_SEGMENTS_KEY = "enableReplaceSegments";
  public static final long DEFAULT_TABLE_MAX_NUM_TASKS = 1;

  /**
   * Job configs
   */
  public static final int DEFAULT_MAX_ATTEMPTS_PER_TASK = 1;

  // Purges rows inside segment that match chosen criteria
  public static class PurgeTask {
    public static final String TASK_TYPE = "PurgeTask";
    public static final String LAST_PURGE_TIME_THREESOLD_PERIOD = "lastPurgeTimeThresholdPeriod";
    public static final String DEFAULT_LAST_PURGE_TIME_THRESHOLD_PERIOD = "14d";
  }

  // Common config keys for segment merge tasks.
  public static abstract class MergeTask {

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

    // Time handling config
    public static final String WINDOW_START_MS_KEY = "windowStartMs";
    public static final String WINDOW_END_MS_KEY = "windowEndMs";
    public static final String NEGATE_WINDOW_FILTER = "negateWindowFilter";
    public static final String ROUND_BUCKET_TIME_PERIOD_KEY = "roundBucketTimePeriod";
    public static final String PARTITION_BUCKET_TIME_PERIOD_KEY = "partitionBucketTimePeriod";

    // Merge config
    public static final String MERGE_TYPE_KEY = "mergeType";
    public static final String AGGREGATION_TYPE_KEY_SUFFIX = ".aggregationType";
    public static final String AGGREGATION_FUNCTION_PARAMETERS_PREFIX = "aggregationFunctionParameters.";
    public static final String MODE = "mode";
    public static final String PROCESS_FROM_WATERMARK_MODE = "processFromWatermark";
    public static final String PROCESS_ALL_MODE = "processAll";

    // Segment config
    public static final String MAX_NUM_RECORDS_PER_TASK_KEY = "maxNumRecordsPerTask";
    public static final String MAX_NUM_RECORDS_PER_SEGMENT_KEY = "maxNumRecordsPerSegment";
    public static final String SEGMENT_MAPPER_FILE_SIZE_IN_BYTES = "segmentMapperFileSizeThresholdInBytes";
    public static final String MAX_NUM_PARALLEL_BUCKETS = "maxNumParallelBuckets";
    public static final String SEGMENT_NAME_PREFIX_KEY = "segmentNamePrefix";
    public static final String SEGMENT_NAME_POSTFIX_KEY = "segmentNamePostfix";
    public static final String FIXED_SEGMENT_NAME_KEY = "fixedSegmentName";

    // This field is set in segment metadata custom map to indicate if the segment is safe to be merged.
    // Tasks can take use of this field to coordinate with the merge task. By default, segment is safe
    // to merge, so existing segments w/o this field can be merged just as before.
    public static final String SEGMENT_ZK_METADATA_SHOULD_NOT_MERGE_KEY = "shouldNotMerge";
  }

  public static class MergeRollupTask extends MergeTask {
    public static final String TASK_TYPE = "MergeRollupTask";

    public static final String MERGE_LEVEL_KEY = "mergeLevel";

    public static final String SEGMENT_ZK_METADATA_MERGE_LEVEL_KEY = TASK_TYPE + "." + MERGE_LEVEL_KEY;
    public static final String SEGMENT_ZK_METADATA_TIME_KEY = TASK_TYPE + TASK_TIME_SUFFIX;

    public static final String MERGED_SEGMENT_NAME_PREFIX = "merged_";

    // Custom segment group manager class name
    public static final String SEGMENT_GROUP_MANAGER_CLASS_NAME_KEY = "segment.group.manager.class.name";

    public static final String ERASE_DIMENSION_VALUES_KEY = "eraseDimensionValues";
  }

  /**
   * Creates segments for the OFFLINE table, using completed segments from the corresponding REALTIME table
   */
  public static class RealtimeToOfflineSegmentsTask extends MergeTask {
    public static final String TASK_TYPE = "RealtimeToOfflineSegmentsTask";

    @Deprecated // Replaced by MERGE_TYPE_KEY
    public static final String COLLECTOR_TYPE_KEY = "collectorType";

    public static final String BUCKET_TIME_PERIOD_KEY = "bucketTimePeriod";
    public static final String BUFFER_TIME_PERIOD_KEY = "bufferTimePeriod";
    public static final String ROUND_BUCKET_TIME_PERIOD_KEY = "roundBucketTimePeriod";
    public static final String MERGE_TYPE_KEY = "mergeType";
    public static final String AGGREGATION_TYPE_KEY_SUFFIX = ".aggregationType";

    public final static EnumSet<AggregationFunctionType> AVAILABLE_CORE_VALUE_AGGREGATORS =
        EnumSet.of(MIN, MAX, SUM, DISTINCTCOUNTHLL, DISTINCTCOUNTRAWHLL, DISTINCTCOUNTTHETASKETCH,
            DISTINCTCOUNTRAWTHETASKETCH, DISTINCTCOUNTTUPLESKETCH, DISTINCTCOUNTRAWINTEGERSUMTUPLESKETCH,
            SUMVALUESINTEGERSUMTUPLESKETCH, AVGVALUEINTEGERSUMTUPLESKETCH, DISTINCTCOUNTHLLPLUS,
            DISTINCTCOUNTRAWHLLPLUS, DISTINCTCOUNTCPCSKETCH, DISTINCTCOUNTRAWCPCSKETCH, DISTINCTCOUNTULL,
            DISTINCTCOUNTRAWULL);
  }

  // Generate segment and push to controller based on batch ingestion configs
  public static class SegmentGenerationAndPushTask {
    public static final String TASK_TYPE = "SegmentGenerationAndPushTask";
  }

  /**
   * Minion task to refresh segments when there are changes to tableConfigs and Schema. This task currently supports the
   * following functionality:
   * 1. Adding/Removing/Updating indexes.
   * 2. Adding new columns (also supports transform configs for new columns).
   * 3. Converting segment versions.
   * 4. Compatible datatype changes to columns (Note that the minion task will fail if the data in the column is not
   *    compatible with target datatype)
   *
   * This is an alternative to performing reload of existing segments on Servers. The reload on servers is sub-optimal
   * for many reasons:
   * 1. Requires an explicit reload call when index configurations change.
   * 2. Is very slow. Happens one (or few - configurable) segment at time to avoid query impact.
   * 3. Compute price is paid on all servers hosting the segment.q
   * 4. Increases server startup time as more and more segments require reload.
   */
  public static class RefreshSegmentTask {
    public static final String TASK_TYPE = "RefreshSegmentTask";

    // Maximum number of tasks to create per table per run.
    public static final int MAX_NUM_TASKS_PER_TABLE = 20;
  }

  public static class UpsertCompactionTask {
    public static final String TASK_TYPE = "UpsertCompactionTask";
    /**
     * The time period to wait before picking segments for this task
     * e.g. if set to "2d", no task will be scheduled for a time window younger than 2 days
     */
    public static final String BUFFER_TIME_PERIOD_KEY = "bufferTimePeriod";
    /**
     * The maximum percent of old records allowed for a completed segment.
     * e.g. if the percent surpasses 30, then the segment may be compacted
     */
    public static final String INVALID_RECORDS_THRESHOLD_PERCENT = "invalidRecordsThresholdPercent";
    /**
     * The maximum count of old records for a completed segment
     * e.g. if the count surpasses 100k, then the segment may be compacted
     */
    public static final String INVALID_RECORDS_THRESHOLD_COUNT = "invalidRecordsThresholdCount";

    /**
     * Valid doc ids type
     */
    public static final String VALID_DOC_IDS_TYPE = "validDocIdsType";

    /**
     * Value for the key VALID_DOC_IDS_TYPE
     */
    public static final String SNAPSHOT = "snapshot";

    /**
     * key representing if upsert compaction task executor should ignore crc mismatch or not during task execution
     */
    public static final String IGNORE_CRC_MISMATCH_KEY = "ignoreCrcMismatch";

    /**
     * default value for the key IGNORE_CRC_MISMATCH_KEY: false
     */
    public static final boolean DEFAULT_IGNORE_CRC_MISMATCH = false;

    /**
     * number of segments to query in one batch to fetch valid doc id metadata, by default 500
     */
    public static final String NUM_SEGMENTS_BATCH_PER_SERVER_REQUEST = "numSegmentsBatchPerServerRequest";
  }

  public static class UpsertCompactMergeTask {
    public static final String TASK_TYPE = "UpsertCompactMergeTask";

    /**
     * The time period to wait before picking segments for this task
     * e.g. if set to "2d", no task will be scheduled for a time window younger than 2 days
     */
    public static final String BUFFER_TIME_PERIOD_KEY = "bufferTimePeriod";

    /**
     * number of segments to query in one batch to fetch valid doc id metadata, by default 500
     */
    public static final String NUM_SEGMENTS_BATCH_PER_SERVER_REQUEST = "numSegmentsBatchPerServerRequest";

    /**
     * prefix for the new segment name that is created,
     * {@link org.apache.pinot.segment.spi.creator.name.UploadedRealtimeSegmentNameGenerator} will add __ as delimiter
     * so not adding _ as a suffix here.
     */
    public static final String MERGED_SEGMENT_NAME_PREFIX = "compacted";

    /**
     * maximum number of records to process in a single task, sum of all docs in to-be-merged segments
     */
    public static final String MAX_NUM_RECORDS_PER_TASK_KEY = "maxNumRecordsPerTask";

    /**
     * default maximum number of records to process in a single task, same as the value in {@link MergeRollupTask}
     */
    public static final long DEFAULT_MAX_NUM_RECORDS_PER_TASK = 50_000_000;

    /**
     * maximum number of records in the output segment
     */
    public static final String MAX_NUM_RECORDS_PER_SEGMENT_KEY = "maxNumRecordsPerSegment";

    /**
     * default maximum number of records in output segment, same as the value in
     * {@link org.apache.pinot.core.segment.processing.framework.SegmentConfig}
     */
    public static final long DEFAULT_MAX_NUM_RECORDS_PER_SEGMENT = 5_000_000;

    /**
     * maximum number of segments to process in a single task
     */
    public static final String MAX_NUM_SEGMENTS_PER_TASK_KEY = "maxNumSegmentsPerTask";

    /**
     * default maximum number of segments to process in a single task
     */
    public static final long DEFAULT_MAX_NUM_SEGMENTS_PER_TASK = 10;

    public static final String MERGED_SEGMENTS_ZK_SUFFIX = ".mergedSegments";
  }
}
