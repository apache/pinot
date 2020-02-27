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
package org.apache.pinot.common.utils;

import java.io.File;
import org.apache.pinot.common.response.BrokerResponseFactory;


public class CommonConstants {

  public static final String PREFIX_OF_SSL_SUBSET = "ssl";

  public static class Helix {
    public static final String IS_SHUTDOWN_IN_PROGRESS = "shutdownInProgress";
    public static final String QUERIES_DISABLED = "queriesDisabled";

    public static final String INSTANCE_CONNECTED_METRIC_NAME = "helix.connected";

    public static final String PREFIX_OF_CONTROLLER_INSTANCE = "Controller_";
    public static final String PREFIX_OF_BROKER_INSTANCE = "Broker_";
    public static final String PREFIX_OF_SERVER_INSTANCE = "Server_";
    public static final String PREFIX_OF_MINION_INSTANCE = "Minion_";

    public static final String BROKER_RESOURCE_INSTANCE = "brokerResource";
    public static final String LEAD_CONTROLLER_RESOURCE_NAME = "leadControllerResource";

    public static final String LEAD_CONTROLLER_RESOURCE_ENABLED_KEY = "RESOURCE_ENABLED";

    // More information on why these numbers are set can be found in the following doc:
    // https://cwiki.apache.org/confluence/display/PINOT/Controller+Separation+between+Helix+and+Pinot
    public static final int NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE = 24;
    public static final int LEAD_CONTROLLER_RESOURCE_REPLICA_COUNT = 1;
    public static final int MIN_ACTIVE_REPLICAS = 0;
    public static final int REBALANCE_DELAY_MS = 300_000; // 5 minutes.

    // Instance tags
    public static final String CONTROLLER_INSTANCE = "controller";
    public static final String UNTAGGED_BROKER_INSTANCE = "broker_untagged";
    public static final String UNTAGGED_SERVER_INSTANCE = "server_untagged";
    public static final String UNTAGGED_MINION_INSTANCE = "minion_untagged";

    public static class StateModel {
      public static class SegmentOnlineOfflineStateModel {
        public static final String ONLINE = "ONLINE";
        public static final String OFFLINE = "OFFLINE";
        public static final String ERROR = "ERROR";
      }

      public static class RealtimeSegmentOnlineOfflineStateModel {
        public static final String ONLINE = "ONLINE";
        public static final String OFFLINE = "OFFLINE";
        public static final String ERROR = "ERROR";
        public static final String CONSUMING = "CONSUMING";
      }

      public static class BrokerOnlineOfflineStateModel {
        public static final String ONLINE = "ONLINE";
        public static final String OFFLINE = "OFFLINE";
        public static final String ERROR = "ERROR";
      }
    }

    public static class ZkClient {
      public static final long DEFAULT_CONNECT_TIMEOUT_SEC = 60L;
    }

    public static class DataSource {
      public enum SegmentAssignmentStrategyType {
        RandomAssignmentStrategy,
        BalanceNumSegmentAssignmentStrategy,
        BucketizedSegmentAssignmentStrategy,
        ReplicaGroupSegmentAssignmentStrategy
      }
    }

    public static class Instance {
      public static final String INSTANCE_ID_KEY = "instanceId";
      public static final String DATA_DIR_KEY = "dataDir";
      public static final String ADMIN_PORT_KEY = "adminPort";
    }

    public enum InstanceType {
      CONTROLLER, BROKER, SERVER, MINION
    }

    public enum TableType {
      OFFLINE, REALTIME;

      public ServerType getServerType() {
        if (this == OFFLINE) {
          return ServerType.OFFLINE;
        }
        return ServerType.REALTIME;
      }
    }
    public static final String SET_INSTANCE_ID_TO_HOSTNAME_KEY = "pinot.set.instance.id.to.hostname";

    public static final String KEY_OF_SERVER_NETTY_PORT = "pinot.server.netty.port";
    public static final int DEFAULT_SERVER_NETTY_PORT = 8098;
    public static final String KEY_OF_BROKER_QUERY_PORT = "pinot.broker.client.queryPort";
    public static final int DEFAULT_BROKER_QUERY_PORT = 8099;
    public static final String KEY_OF_SERVER_NETTY_HOST = "pinot.server.netty.host";

    // NOTE: Helix will disconnect the manager and disable the instance if it detects flapping (too frequent disconnect
    // from ZooKeeper). Setting flapping time window to a small value can avoid this from happening. Helix ignores the
    // non-positive value, so set the default value as 1.
    public static final String CONFIG_OF_CONTROLLER_FLAPPING_TIME_WINDOW_MS = "pinot.controller.flapping.timeWindowMs";
    public static final String CONFIG_OF_BROKER_FLAPPING_TIME_WINDOW_MS = "pinot.broker.flapping.timeWindowMs";
    public static final String CONFIG_OF_SERVER_FLAPPING_TIME_WINDOW_MS = "pinot.server.flapping.timeWindowMs";
    public static final String CONFIG_OF_MINION_FLAPPING_TIME_WINDOW_MS = "pinot.minion.flapping.timeWindowMs";
    public static final String DEFAULT_FLAPPING_TIME_WINDOW_MS = "1";
  }

  public static class Broker {
    public static final String ROUTING_TABLE_CONFIG_PREFIX = "pinot.broker.routing.table";
    public static final String ACCESS_CONTROL_CONFIG_PREFIX = "pinot.broker.access.control";
    public static final String METRICS_CONFIG_PREFIX = "pinot.broker.metrics";
    public static final String CONFIG_OF_METRICS_NAME_PREFIX = "pinot.broker.metrics.prefix";
    public static final String DEFAULT_METRICS_NAME_PREFIX = "pinot.broker.";
    public static final boolean DEFAULT_METRICS_GLOBAL_ENABLED = false;

    public static final String CONFIG_OF_DELAY_SHUTDOWN_TIME_MS = "pinot.broker.delayShutdownTimeMs";
    public static final long DEFAULT_DELAY_SHUTDOWN_TIME_MS = 10_000L;
    public static final String CONFIG_OF_ENABLE_TABLE_LEVEL_METRICS = "pinot.broker.enableTableLevelMetrics";
    public static final String CONFIG_OF_REQUEST_HANDLER_TYPE = "pinot.broker.requestHandlerType";
    public static final String SINGLE_CONNECTION_REQUEST_HANDLER_TYPE = "singleConnection";
    public static final String CONNECTION_POOL_REQUEST_HANDLER_TYPE = "connectionPool";
    public static final String DEFAULT_REQUEST_HANDLER_TYPE = SINGLE_CONNECTION_REQUEST_HANDLER_TYPE;

    public static final String CONFIG_OF_BROKER_QUERY_RESPONSE_LIMIT = "pinot.broker.query.response.limit";
    public static final int DEFAULT_BROKER_QUERY_RESPONSE_LIMIT = Integer.MAX_VALUE;
    public static final String CONFIG_OF_BROKER_QUERY_LOG_LENGTH = "pinot.broker.query.log.length";
    public static final int DEFAULT_BROKER_QUERY_LOG_LENGTH = Integer.MAX_VALUE;
    public static final String CONFIG_OF_BROKER_QUERY_LOG_MAX_RATE_PER_SECOND =
        "pinot.broker.query.log.maxRatePerSecond";
    public static final double DEFAULT_BROKER_QUERY_LOG_MAX_RATE_PER_SECOND = 10_000d;
    public static final String CONFIG_OF_BROKER_TIMEOUT_MS = "pinot.broker.timeoutMs";
    public static final long DEFAULT_BROKER_TIMEOUT_MS = 10_000L;
    public static final String CONFIG_OF_BROKER_ID = "pinot.broker.id";
    public static final BrokerResponseFactory.ResponseType DEFAULT_BROKER_RESPONSE_TYPE =
        BrokerResponseFactory.ResponseType.BROKER_RESPONSE_TYPE_NATIVE;
    // The sleep interval time of the thread used by the Brokers to refresh TimeboundaryInfo upon segment refreshing
    // events.
    public static final String CONFIG_OF_BROKER_REFRESH_TIMEBOUNDARY_INFO_SLEEP_INTERVAL =
        "pinot.broker.refresh.timeBoundaryInfo.sleepInterval";
    public static final long DEFAULT_BROKER_REFRESH_TIMEBOUNDARY_INFO_SLEEP_INTERVAL_MS = 10000L;
    // Configuration to consider the broker ServiceStatus as being STARTED if the percent of resources (tables) that
    // are ONLINE for this this broker has crossed the threshold percentage of the total number of tables
    // that it is expected to serve.
    public static final String CONFIG_OF_BROKER_MIN_RESOURCE_PERCENT_FOR_START =
        "pinot.broker.startup.minResourcePercent";
    public static final double DEFAULT_BROKER_MIN_RESOURCE_PERCENT_FOR_START = 100.0;

    public static class Request {
      public static final String PQL = "pql";
      public static final String SQL = "sql";
      public static final String TRACE = "trace";
      public static final String DEBUG_OPTIONS = "debugOptions";
      public static final String QUERY_OPTIONS = "queryOptions";

      public static class QueryOptionKey {
        public static final String PRESERVE_TYPE = "preserveType";
        public static final String RESPONSE_FORMAT = "responseFormat";
        public static final String GROUP_BY_MODE = "groupByMode";
      }
    }
  }

  public static class Server {
    public static final String CONFIG_OF_INSTANCE_ID = "pinot.server.instance.id";
    public static final String CONFIG_OF_INSTANCE_DATA_DIR = "pinot.server.instance.dataDir";
    public static final String CONFIG_OF_CONSUMER_DIR = "pinot.server.instance.consumerDir";
    public static final String CONFIG_OF_INSTANCE_SEGMENT_TAR_DIR = "pinot.server.instance.segmentTarDir";
    public static final String CONFIG_OF_INSTANCE_READ_MODE = "pinot.server.instance.readMode";
    public static final String CONFIG_OF_INSTANCE_DATA_MANAGER_CLASS = "pinot.server.instance.data.manager.class";
    public static final String CONFIG_OF_QUERY_EXECUTOR_PRUNER_CLASS = "pinot.server.query.executor.pruner.class";
    public static final String CONFIG_OF_QUERY_EXECUTOR_TIMEOUT = "pinot.server.query.executor.timeout";
    public static final String CONFIG_OF_QUERY_EXECUTOR_CLASS = "pinot.server.query.executor.class";
    public static final String CONFIG_OF_REQUEST_HANDLER_FACTORY_CLASS = "pinot.server.requestHandlerFactory.class";
    public static final String CONFIG_OF_NETTY_PORT = "pinot.server.netty.port";
    public static final String CONFIG_OF_ADMIN_API_PORT = "pinot.server.adminapi.port";

    public static final String CONFIG_OF_SEGMENT_FORMAT_VERSION = "pinot.server.instance.segment.format.version";
    public static final String CONFIG_OF_ENABLE_SPLIT_COMMIT = "pinot.server.instance.enable.split.commit";
    public static final String CONFIG_OF_ENABLE_COMMIT_END_WITH_METADATA =
        "pinot.server.instance.enable.commitend.metadata";
    public static final String CONFIG_OF_REALTIME_OFFHEAP_ALLOCATION = "pinot.server.instance.realtime.alloc.offheap";
    public static final String CONFIG_OF_REALTIME_OFFHEAP_DIRECT_ALLOCATION =
        "pinot.server.instance.realtime.alloc.offheap.direct";
    public static final String PREFIX_OF_CONFIG_OF_PINOT_FS_FACTORY = "pinot.server.storage.factory";
    public static final String PREFIX_OF_CONFIG_OF_PINOT_CRYPTER = "pinot.server.crypter";
    // Configuration to consider the server ServiceStatus as being STARTED if the percent of resources (tables) that
    // are ONLINE for this this server has crossed the threshold percentage of the total number of tables
    // that it is expected to serve.
    public static final String CONFIG_OF_SERVER_MIN_RESOURCE_PERCENT_FOR_START =
        "pinot.server.startup.minResourcePercent";
    public static final double DEFAULT_SERVER_MIN_RESOURCE_PERCENT_FOR_START = 100.0;
    public static final String CONFIG_OF_STARTUP_REALTIME_CONSUMPTION_CATCHUP_WAIT_MS =
        "pinot.server.starter.realtimeConsumptionCatchupWaitMs";
    public static final int DEFAULT_STARTUP_REALTIME_CONSUMPTION_CATCHUP_WAIT_MS = 0;

    public static final int DEFAULT_ADMIN_API_PORT = 8097;
    public static final String DEFAULT_READ_MODE = "heap";
    public static final String DEFAULT_INSTANCE_BASE_DIR =
        System.getProperty("java.io.tmpdir") + File.separator + "PinotServer";
    public static final String DEFAULT_INSTANCE_DATA_DIR = DEFAULT_INSTANCE_BASE_DIR + File.separator + "index";
    public static final String DEFAULT_INSTANCE_SEGMENT_TAR_DIR =
        DEFAULT_INSTANCE_BASE_DIR + File.separator + "segmentTar";
    public static final String DEFAULT_DATA_MANAGER_CLASS =
        "org.apache.pinot.server.starter.helix.HelixInstanceDataManager";
    public static final String DEFAULT_QUERY_EXECUTOR_CLASS =
        "org.apache.pinot.core.query.executor.ServerQueryExecutorV1Impl";
    public static final long DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS = 15_000L;
    public static final String DEFAULT_REQUEST_HANDLER_FACTORY_CLASS =
        "org.apache.pinot.server.request.SimpleRequestHandlerFactory";
    public static final String PREFIX_OF_CONFIG_OF_SEGMENT_FETCHER_FACTORY = "pinot.server.segment.fetcher";

    // Configs for server starter startup/shutdown checks
    // Startup: timeout for the startup checks
    public static final String CONFIG_OF_STARTUP_TIMEOUT_MS = "pinot.server.startup.timeoutMs";
    public static final long DEFAULT_STARTUP_TIMEOUT_MS = 600_000L;
    // Startup: enable service status check before claiming server up
    public static final String CONFIG_OF_STARTUP_ENABLE_SERVICE_STATUS_CHECK =
        "pinot.server.startup.enableServiceStatusCheck";
    public static final boolean DEFAULT_STARTUP_ENABLE_SERVICE_STATUS_CHECK = true;
    public static final String CONFIG_OF_STARTUP_SERVICE_STATUS_CHECK_INTERVAL_MS =
        "pinot.server.startup.serviceStatusCheckIntervalMs";
    public static final long DEFAULT_STARTUP_SERVICE_STATUS_CHECK_INTERVAL_MS = 10_000L;
    // Shutdown: timeout for the shutdown checks
    public static final String CONFIG_OF_SHUTDOWN_TIMEOUT_MS = "pinot.server.shutdown.timeoutMs";
    public static final long DEFAULT_SHUTDOWN_TIMEOUT_MS = 600_000L;
    // Shutdown: enable query check before shutting down the server
    //           Will drain queries (no incoming queries and all existing queries finished)
    public static final String CONFIG_OF_SHUTDOWN_ENABLE_QUERY_CHECK = "pinot.server.shutdown.enableQueryCheck";
    public static final boolean DEFAULT_SHUTDOWN_ENABLE_QUERY_CHECK = true;
    // Shutdown: threshold to mark that there is no incoming queries, use max query time as the default threshold
    public static final String CONFIG_OF_SHUTDOWN_NO_QUERY_THRESHOLD_MS = "pinot.server.shutdown.noQueryThresholdMs";
    // Shutdown: enable resource check before shutting down the server
    //           Will wait until all the resources in the external view are neither ONLINE nor CONSUMING
    //           No need to enable this check if startup service status check is enabled
    public static final String CONFIG_OF_SHUTDOWN_ENABLE_RESOURCE_CHECK = "pinot.server.shutdown.enableResourceCheck";
    public static final boolean DEFAULT_SHUTDOWN_ENABLE_RESOURCE_CHECK = false;
    public static final String CONFIG_OF_SHUTDOWN_RESOURCE_CHECK_INTERVAL_MS =
        "pinot.server.shutdown.resourceCheckIntervalMs";
    public static final long DEFAULT_SHUTDOWN_RESOURCE_CHECK_INTERVAL_MS = 10_000L;

    // TODO: remove the deprecated config keys in the new release
    @Deprecated
    public static final String CONFIG_OF_STARTER_ENABLE_SEGMENTS_LOADING_CHECK =
        "pinot.server.starter.enableSegmentsLoadingCheck";
    @Deprecated
    public static final String CONFIG_OF_STARTER_TIMEOUT_IN_SECONDS = "pinot.server.starter.timeoutInSeconds";
    @Deprecated
    public static final String CONFIG_OF_ENABLE_SHUTDOWN_DELAY = "pinot.server.instance.enable.shutdown.delay";
    @Deprecated
    public static final String CONFIG_OF_INSTANCE_MAX_SHUTDOWN_WAIT_TIME =
        "pinot.server.instance.starter.maxShutdownWaitTime";
    @Deprecated
    public static final String CONFIG_OF_INSTANCE_CHECK_INTERVAL_TIME =
        "pinot.server.instance.starter.checkIntervalTime";

    public static final String DEFAULT_COLUMN_MIN_MAX_VALUE_GENERATOR_MODE = "TIME";

    public static class SegmentCompletionProtocol {
      public static final String PREFIX_OF_CONFIG_OF_SEGMENT_UPLOADER = "pinot.server.segment.uploader";

      public static final String CONFIG_OF_CONTROLLER_HTTPS_ENABLED = "enabled";
      public static final String CONFIG_OF_CONTROLLER_HTTPS_PORT = "controller.port";
      public static final String CONFIG_OF_SEGMENT_UPLOAD_REQUEST_TIMEOUT_MS = "upload.request.timeout.ms";

      public static final int DEFAULT_SEGMENT_UPLOAD_REQUEST_TIMEOUT_MS = 300_000;
      public static final int DEFAULT_OTHER_REQUESTS_TIMEOUT = 10_000;
    }

    public static final String DEFAULT_METRICS_PREFIX = "pinot.server.";
    public static final boolean DEFAULT_METRICS_GLOBAL_ENABLED = false;
  }

  public static class Controller {
    public static final String PREFIX_OF_CONFIG_OF_SEGMENT_FETCHER_FACTORY = "pinot.controller.segment.fetcher";
    public static final String PREFIX_OF_CONFIG_OF_PINOT_FS_FACTORY = "pinot.controller.storage.factory";
    public static final String HOST_HTTP_HEADER = "Pinot-Controller-Host";
    public static final String VERSION_HTTP_HEADER = "Pinot-Controller-Version";
    public static final String SEGMENT_NAME_HTTP_HEADER = "Pinot-Segment-Name";
    public static final String TABLE_NAME_HTTP_HEADER = "Pinot-Table-Name";
    public static final String PREFIX_OF_CONFIG_OF_PINOT_CRYPTER = "pinot.controller.crypter";

    public static final String CONFIG_OF_CONTROLLER_METRICS_PREFIX = "controller.metrics.prefix";
    // FYI this is incorrect as it generate metrics named without a dot after pinot.controller part,
    // but we keep this default for backward compatibility in case someone relies on this format
    // see Server or Broker class for correct prefix format you should use
    public static final String DEFAULT_METRICS_PREFIX = "pinot.controller";
  }

  public static class Minion {
    public static final String CONFIG_OF_METRICS_PREFIX = "pinot.minion.";
    public static final String METADATA_EVENT_OBSERVER_PREFIX = "metadata.event.notifier";

    // Config keys
    public static final String CONFIG_OF_METRICS_PREFIX_KEY = "metricsPrefix";
    public static final String METRICS_REGISTRY_REGISTRATION_LISTENERS_KEY = "metricsRegistryRegistrationListeners";

    // Default settings
    public static final int DEFAULT_HELIX_PORT = 9514;
    public static final String DEFAULT_INSTANCE_BASE_DIR =
        System.getProperty("java.io.tmpdir") + File.separator + "PinotMinion";
    public static final String DEFAULT_INSTANCE_DATA_DIR = DEFAULT_INSTANCE_BASE_DIR + File.separator + "data";
    public static final String PREFIX_OF_CONFIG_OF_PINOT_FS_FACTORY = "storage.factory";
    public static final String PREFIX_OF_CONFIG_OF_SEGMENT_FETCHER_FACTORY = "segment.fetcher";
    public static final String PREFIX_OF_CONFIG_OF_SEGMENT_UPLOADER = "segment.uploader";
    public static final String PREFIX_OF_CONFIG_OF_PINOT_CRYPTER = "crypter";
  }

  public static class Segment {
    public static class Realtime {
      public enum Status {
        IN_PROGRESS, DONE
      }

      /**
       * During realtime segment completion, the value of this enum decides how  non-winner servers should replace  the completed segment.
       */
      public enum CompletionMode {
        DEFAULT, // default behavior - if the in memory segment in the non-winner server is equivalent to the committed segment, then build and replace, else download
        DOWNLOAD // non-winner servers always download the segment, never build it
      }

      public static final String STATUS = "segment.realtime.status";
    }

    public static class Offline {
      public static final String DOWNLOAD_URL = "segment.offline.download.url";
      public static final String PUSH_TIME = "segment.offline.push.time";
      public static final String REFRESH_TIME = "segment.offline.refresh.time";
    }

    public static final String SEGMENT_NAME = "segment.name";
    public static final String SEGMENT_TYPE = "segment.type";
    public static final String CRYPTER_NAME = "segment.crypter";
    public static final String INDEX_VERSION = "segment.index.version";
    public static final String START_TIME = "segment.start.time";
    public static final String END_TIME = "segment.end.time";
    public static final String TIME_UNIT = "segment.time.unit";
    public static final String TOTAL_DOCS = "segment.total.docs";
    public static final String CRC = "segment.crc";
    public static final String CREATION_TIME = "segment.creation.time";
    public static final String FLUSH_THRESHOLD_SIZE = "segment.flush.threshold.size";
    public static final String FLUSH_THRESHOLD_TIME = "segment.flush.threshold.time";
    public static final String PARTITION_METADATA = "segment.partition.metadata";
    /**
     * This field is used for parallel push protection to lock the segment globally.
     * We put the segment upload start timestamp so that if the previous push failed without unlock the segment, the
     * next upload won't be blocked forever.
     */
    public static final String SEGMENT_UPLOAD_START_TIME = "segment.upload.start.time";

    public static final String CUSTOM_MAP = "custom.map";

    public static final String SEGMENT_BACKUP_DIR_SUFFIX = ".segment.bak";
    public static final String SEGMENT_TEMP_DIR_SUFFIX = ".segment.tmp";

    public static final String LOCAL_SEGMENT_SCHEME = "file";

    public enum SegmentType {
      OFFLINE, REALTIME
    }

    public static class AssignmentStrategy {
      public static String BALANCE_NUM_SEGMENT_ASSIGNMENT_STRATEGY = "BalanceNumSegmentAssignmentStrategy";
      public static String REPLICA_GROUP_SEGMENT_ASSIGNMENT_STRATEGY = "ReplicaGroupSegmentAssignmentStrategy";
    }

    @Deprecated
    public static final String TABLE_NAME = "segment.table.name";
  }

  public static class SegmentOperations {
    public static class HadoopSegmentOperations {
      public static final String PRINCIPAL = "hadoop.kerberos.principle";
      public static final String KEYTAB = "hadoop.kerberos.keytab";
      public static final String HADOOP_CONF_PATH = "hadoop.conf.path";
    }

    public static class AzureSegmentOperations {
      public static final String ACCOUNT_ID = "accountId";
      public static final String AUTH_ENDPOINT = "authEndpoint";
      public static final String CLIENT_ID = "clientId";
      public static final String CLIENT_SECRET = "clientSecret";
    }

    public static final String RETRY = "retry.count";
    public static final int RETRY_DEFAULT = 3;
    public static final String RETRY_WAITIME_MS = "retry.wait.ms";
    public static final int RETRY_WAITIME_MS_DEFAULT = 100;
  }
}
