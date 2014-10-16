package com.linkedin.pinot.common.utils;

public class CommonConstants {
  public static class Helix {
    public static final String PREFIX_OF_BROKER_RESOURCE_TAG = "broker_";
    public static final String PREFIX_OF_SERVER_INSTANCE = "Server_";
    public static final String PREFIX_OF_BROKER_INSTANCE = "Broker_";

    public static final String BROKER_RESOURCE_INSTANCE = "brokerResource";
    public static final String UNTAGGED_SERVER_INSTANCE = "server_untagged";
    public static final String UNTAGGED_BROKER_INSTANCE = "broker_untagged";

    public static class DataSource {
      public static final String RESOURCE_NAME = "resourceName";
      public static final String TABLE_NAME = "tableName";
      public static final String TIME_COLUMN_NAME = "timeColumnName";
      public static final String TIME_TYPE = "timeType";
      public static final String NUMBER_OF_DATA_INSTANCES = "numberOfDataInstances";
      public static final String NUMBER_OF_COPIES = "numberOfCopies";
      public static final String RETENTION_TIME_UNIT = "retentionTimeUnit";
      public static final String RETENTION_TIME_VALUE = "retentionTimeValue";
      public static final String PUSH_FREQUENCY = "pushFrequency";
      public static final String SEGMENT_ASSIGNMENT_STRATEGY = "segmentAssignmentStrategy";
      public static final String BROKER_TAG_NAME = "brokerTagName";
      public static final String NUMBER_OF_BROKER_INSTANCES = "numberOfBrokerInstances";
    }

    public static final String KEY_OF_SEGMENT_ASSIGNMENT_STRATEGY = "segmentAssignmentStrategy";
    public static final String DEFAULT_SEGMENT_ASSIGNMENT_STRATEGY = "BalanceNumSegmentAssignmentStrategy";

  }
}
