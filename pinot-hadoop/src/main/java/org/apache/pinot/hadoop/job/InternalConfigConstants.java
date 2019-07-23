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

  // Partitioning configs
  public static final String PARTITION_COLUMN_CONFIG = "partition.column";
  public static final String NUM_PARTITIONS_CONFIG = "num.partitions";
  public static final String PARTITION_FUNCTION_CONFIG = "partition.function";

  public static final String SORTED_COLUMN_CONFIG = "sorted.column";
}
