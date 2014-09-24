package com.linkedin.pinot.controller.helix.core;

public class IncomingConfigParamKeys {

  public static final String EXTERNAL = "external";
  public static final String CLUSTER_TYPE = "clusterType";
  public static final String OFFLINE_PROPERTIES = "offlineProperties";
  public static final String REALTIME_PROPERTIES = "realtimeProperties";
  public static final String HYBRID_PROPERTIES = "hybridProperties";

  public enum Offline {
    offlineResourceName,
    offlineNumReplicas,
    offlineNumInstancesPerReplica,
    offlineRetentionTimeUnit,
    offlineRetentionTimeColumn,
    offlineRetentionDuration,
    host,
    port,
    partitions;
  }

  public enum Realtime {
    realtimeResourceName,
    realtimeNumReplicas,
    realtimeMaxPartitionId,
    realtimeInstances,
    realtimeKafkaTopicName,
    realtimeSchema,
    realtimeRetentionTimeUnit,
    realtimeRetentionDynamincDuration,
    realtimeRetentionDuration,
    realtimeSortedColumns,
    realtimeShardedColumn,
    columName,
    columnType,
    isMulti,
    isMetric,
    host,
    port,
    partitions;
  }

  public enum Hybrid {
    hybridResourceName,
    realtimeResourceName,
    offlineResourceName,
    timeColumn;
  }
}
