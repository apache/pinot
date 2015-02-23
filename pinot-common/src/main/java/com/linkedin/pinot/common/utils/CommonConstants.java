package com.linkedin.pinot.common.utils;

import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.Set;


public class CommonConstants {
  public static class Helix {
    public static final String PREFIX_OF_BROKER_RESOURCE_TAG = "broker_";
    public static final String PREFIX_OF_SERVER_INSTANCE = "Server_";
    public static final String PREFIX_OF_BROKER_INSTANCE = "Broker_";

    public static final String BROKER_RESOURCE_INSTANCE = "brokerResource";
    public static final String UNTAGGED_SERVER_INSTANCE = "server_untagged";
    public static final String UNTAGGED_BROKER_INSTANCE = "broker_untagged";

    public static class StateModel {
      public static class SegmentOnlineOfflineStateModel {
        public static final String ONLINE = "ONLINE";
        public static final String OFFLINE = "OFFLINE";
        public static final String DROPPED = "DROPPED";
      }

      public static class BrokerOnlineOfflineStateModel {
        public static final String ONLINE = "ONLINE";
        public static final String OFFLINE = "OFFLINE";
        public static final String DROPPED = "DROPPED";
      }
    }

    /**
     * Resources names that are not Pinot resources (such as broker resource)
     */
    public static final Set<String> NON_PINOT_RESOURCE_RESOURCE_NAMES =
        Collections.unmodifiableSet(Sets.newHashSet(BROKER_RESOURCE_INSTANCE));

    public static class DataSource {
      public static final String RESOURCE_NAME = "resourceName";
      public static final String RESOURCE_TYPE = "resourceType";
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
      public static final String METADATA = "metadata";
    }

    public static class DataSourceRequestType {
      public static final String CREATE = "create";
      public static final String UPDATE_DATA_RESOURCE = "updateDataResource";
      public static final String EXPAND_DATA_RESOURCE = "expandDataResource";
      public static final String UPDATE_DATA_RESOURCE_CONFIG = "updateDataResourceConfig";
      public static final String UPDATE_BROKER_RESOURCE = "updateBrokerResource";
      public static final String ADD_TABLE_TO_RESOURCE = "addTableToResource";
      public static final String REMOVE_TABLE_FROM_RESOURCE = "removeTableFromResource";
    }

    public static enum ResourceType {
      OFFLINE,
      REALTIME,
      HYBRID;
    }

    public static final String KEY_OF_SEGMENT_ASSIGNMENT_STRATEGY = "segmentAssignmentStrategy";
    public static final String DEFAULT_SEGMENT_ASSIGNMENT_STRATEGY = "BalanceNumSegmentAssignmentStrategy";
    public static final String KEY_OF_SERVER_NETTY_PORT = "pinot.server.netty.port";
    public static final int DEFAULT_SERVER_NETTY_PORT = 8098;
    public static final String KEY_OF_BROKER_QUERY_PORT = "pinot.broker.client.queryPort";
    public static final int DEFAULT_BROKER_QUERY_PORT = 8099;
    public static final String KEY_OF_SERVER_NETTY_HOST = "pinot.server.netty.host";

  }

  public static class Server {
    public static final String CONFIG_OF_INSTANCE_DATA_DIR = "pinot.server.instance.dataDir";
    public static final String CONFIG_OF_INSTANCE_SEGMENT_TAR_DIR = "pinot.server.instance.segmentTarDir";
    public static final String CONFIG_OF_INSTANCE_READ_MODE = "pinot.server.instance.readMode";
    public static final String CONFIG_OF_INSTANCE_DATA_MANAGER_CLASS = "pinot.server.instance.data.manager.class";
    public static final String CONFIG_OF_INSTANCE_SEGMENT_METADATA_LOADER_CLASS = "pinot.server.instance.segment.metadata.loader.class";
    public static final String CONFIG_OF_QUERY_EXECUTOR_PRUNER_CLASS = "pinot.server.query.executor.pruner.class";
    public static final String CONFIG_OF_QUERY_EXECUTOR_TIMEOUT = "pinot.server.query.executor.timeout";
    public static final String CONFIG_OF_QUERY_EXECUTOR_CLASS = "pinot.server.query.executor.class";
    public static final String CONFIG_OF_REQUEST_HANDLER_FACTORY_CLASS = "pinot.server.requestHandlerFactory.class";
    public static final String CONFIG_OF_NETTY_PORT = "pinot.server.netty.port";

    public static final String DEFAULT_READ_MODE = "heap";
    public static final String DEFAULT_INSTANCE_DATA_DIR = "/tmp/PinotServer/test/index";
    public static final String DEFAULT_INSTANCE_SEGMENT_TAR_DIR = "/tmp/PinotServer/test/segmentTar";
    public static final String DEFAULT_SEGMENT_METADATA_LOADER_CLASS = "com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentMetadataLoader";
    public static final String DEFAULT_DATA_MANAGER_CLASS = "com.linkedin.pinot.server.starter.helix.HelixInstanceDataManager";
    public static final String DEFAULT_QUERY_EXECUTOR_CLASS = "com.linkedin.pinot.core.query.executor.ServerQueryExecutorV1Impl";
    public static final String DEFAULT_QUERY_EXECUTOR_TIMEOUT = "150000";
    public static final String DEFAULT_REQUEST_HANDLER_FACTORY_CLASS = "com.linkedin.pinot.server.request.SimpleRequestHandlerFactory";
  }

  public static class Metric {
    public static class Server {
      public static final String CURRENT_NUMBER_OF_SEGMENTS = "currentNumberOfSegments";
      public static final String CURRENT_NUMBER_OF_DOCUMENTS = "currentNumberOfDocuments";
      public static final String NUMBER_OF_DELETED_SEGMENTS = "numberOfDeletedSegments";
    }

  }

  public static class Broker {
    public static class DataResource {
      public static final String RESOURCE_NAME = "resourceName";
      public static final String NUM_BROKER_INSTANCES = "numBrokerInstances";
      public static final String TAG = "tag";
      public static final String REALTIME_RESOURCE_SUFFIX = "_R";
      public static final String OFFLINE_RESOURCE_SUFFIX = "_O";
    }

    public static class TagResource {
      public static final String NUM_BROKER_INSTANCES = "numBrokerInstances";
      public static final String TAG = "tag";
    }
  }
}
