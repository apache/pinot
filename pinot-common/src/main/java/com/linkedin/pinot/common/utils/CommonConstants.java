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
      public static final String REQUEST_TYPE = "requestType";
    }

    public static class DataSourceRequestType {
      public static final String CREATE = "create";
      public static final String UPDATE_DATA_RESOURCE = "updateDataResource";
      public static final String EXPAND_DATA_RESOURCE = "expandDataResource";
      public static final String UPDATE_DATA_RESOURCE_CONFIG = "updateDataResourceConfig";
      public static final String UPDATE_BROKER_RESOURCE = "updateBrokerResource";

    }

    public static final String KEY_OF_SEGMENT_ASSIGNMENT_STRATEGY = "segmentAssignmentStrategy";
    public static final String DEFAULT_SEGMENT_ASSIGNMENT_STRATEGY = "BalanceNumSegmentAssignmentStrategy";
    public static final String KEY_OF_SERVER_NETTY_PORT = "pinot.server.netty.port";
    public static final int DEFAULT_SERVER_NETTY_PORT = 8098;
    public static final String KEY_OF_BROKER_QUERY_PORT = "pinot.broker.client.queryPort";
    public static final int DEFAULT_BROKER_QUERY_PORT = 8099;
    public static final String KEY_OF_SERVER_NETTY_HOST = "pinot.server.netty.host";

  }
}
