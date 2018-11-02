/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.utils;

import com.linkedin.pinot.common.response.BrokerResponseFactory;
import java.io.File;


public class CommonConstants {

  public static final String PREFIX_OF_SSL_SUBSET = "ssl";

  public static class Helix {
    public static final String IS_SHUTDOWN_IN_PROGRESS = "shutdownInProgress";

    public static final String PREFIX_OF_SERVER_INSTANCE = "Server_";
    public static final String PREFIX_OF_BROKER_INSTANCE = "Broker_";

    public static final String SERVER_INSTANCE_TYPE = "server";
    public static final String BROKER_INSTANCE_TYPE = "broker";

    public static final String BROKER_RESOURCE_INSTANCE = "brokerResource";

    public static final String UNTAGGED_SERVER_INSTANCE = "server_untagged";
    public static final String UNTAGGED_BROKER_INSTANCE = "broker_untagged";

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

    public enum TableType {
      OFFLINE,
      REALTIME;

      public ServerType getServerType() {
        if (this == OFFLINE) {
          return ServerType.OFFLINE;
        }
        return ServerType.REALTIME;
      }
    }

    public static final String KEY_OF_SERVER_NETTY_PORT = "pinot.server.netty.port";
    public static final int DEFAULT_SERVER_NETTY_PORT = 8098;
    public static final String KEY_OF_BROKER_QUERY_PORT = "pinot.broker.client.queryPort";
    public static final int DEFAULT_BROKER_QUERY_PORT = 8099;
    public static final String KEY_OF_SERVER_NETTY_HOST = "pinot.server.netty.host";

    public static final String HELIX_MANAGER_FLAPPING_TIME_WINDOW_KEY = "helixmanager.flappingTimeWindow";
    public static final String HELIX_MANAGER_MAX_DISCONNECT_THRESHOLD_KEY = "helixmanager.maxDisconnectThreshold";
    public static final String CONFIG_OF_HELIX_FLAPPING_TIMEWINDOW_MS = "pinot.server.flapping.timeWindowMs";
    public static final String CONFIG_OF_HELIX_MAX_DISCONNECT_THRESHOLD = "pinot.server.flapping.maxDisconnectThreshold";
    public static final String DEFAULT_HELIX_FLAPPING_TIMEWINDOW_MS = "1";
    public static final String DEFAULT_HELIX_FLAPPING_MAX_DISCONNECT_THRESHOLD = "100";
  }

  public static class Broker {
    public static final String CONFIG_OF_BROKER_QUERY_RESPONSE_LIMIT = "pinot.broker.query.response.limit";
    public static final int DEFAULT_BROKER_QUERY_RESPONSE_LIMIT = Integer.MAX_VALUE;
    public static final String CONFIG_OF_BROKER_QUERY_LOG_LENGTH = "pinot.broker.query.log.length";
    public static final int DEFAULT_BROKER_QUERY_LOG_LENGTH = Integer.MAX_VALUE;
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
    public static class Request {
      public static final String PQL = "pql";
      public static final String TRACE = "trace";
      public static final String DEBUG_OPTIONS = "debugOptions";

      public static class QueryOptionKey {
        public static final String PRESERVE_TYPE = "preserveType";
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
    public static final String CONFIG_OF_STARTER_ENABLE_SEGMENTS_LOADING_CHECK = "pinot.server.starter.enableSegmentsLoadingCheck";
    public static final String CONFIG_OF_STARTER_TIMEOUT_IN_SECONDS = "pinot.server.starter.timeoutInSeconds";

    public static final String CONFIG_OF_SEGMENT_FORMAT_VERSION = "pinot.server.instance.segment.format.version";
    public static final String CONFIG_OF_ENABLE_DEFAULT_COLUMNS = "pinot.server.instance.enable.default.columns";
    public static final String CONFIG_OF_ENABLE_SHUTDOWN_DELAY = "pinot.server.instance.enable.shutdown.delay";
    public static final String CONFIG_OF_ENABLE_SPLIT_COMMIT = "pinot.server.instance.enable.split.commit";
    public static final String CONFIG_OF_REALTIME_OFFHEAP_ALLOCATION = "pinot.server.instance.realtime.alloc.offheap";
    public static final String CONFIG_OF_REALTIME_OFFHEAP_DIRECT_ALLOCATION = "pinot.server.instance.realtime.alloc.offheap.direct";
    public static final String PREFIX_OF_CONFIG_OF_PINOT_FS_FACTORY = "pinot.server.storage.factory";
    public static final String PREFIX_OF_CONFIG_OF_PINOT_CRYPTER = "pinot.server.crypter";
    public static final String CONFIG_OF_INSTANCE_MAX_SHUTDOWN_WAIT_TIME = "pinot.server.instance.starter.maxShutdownWaitTime";
    public static final String CONFIG_OF_INSTANCE_CHECK_INTERVAL_TIME = "pinot.server.instance.starter.checkIntervalTime";

    public static final int DEFAULT_ADMIN_API_PORT = 8097;
    public static final boolean DEFAULT_STARTER_ENABLE_SEGMENTS_LOADING_CHECK = false;
    public static final int DEFAULT_STARTER_TIMEOUT_IN_SECONDS = 600;
    public static final String DEFAULT_READ_MODE = "heap";
    public static final String DEFAULT_INSTANCE_BASE_DIR =
        System.getProperty("java.io.tmpdir") + File.separator + "PinotServer";
    public static final String DEFAULT_INSTANCE_DATA_DIR = DEFAULT_INSTANCE_BASE_DIR + File.separator + "index";
    public static final String DEFAULT_INSTANCE_SEGMENT_TAR_DIR =
        DEFAULT_INSTANCE_BASE_DIR + File.separator + "segmentTar";
    public static final String DEFAULT_DATA_MANAGER_CLASS =
        "com.linkedin.pinot.server.starter.helix.HelixInstanceDataManager";
    public static final String DEFAULT_QUERY_EXECUTOR_CLASS =
        "com.linkedin.pinot.core.query.executor.ServerQueryExecutorV1Impl";
    public static final long DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS = 15_000L;
    public static final String DEFAULT_REQUEST_HANDLER_FACTORY_CLASS =
        "com.linkedin.pinot.server.request.SimpleRequestHandlerFactory";
    public static final String PREFIX_OF_CONFIG_OF_SEGMENT_FETCHER_FACTORY = "pinot.server.segment.fetcher";
    public static final String PREFIX_OF_CONFIG_OF_SEGMENT_UPLOADER = "pinot.server.segment.uploader";
    public static final String DEFAULT_STAR_TREE_FORMAT_VERSION = "OFF_HEAP";
    public static final String DEFAULT_COLUMN_MIN_MAX_VALUE_GENERATOR_MODE = "TIME";
    public static final long DEFAULT_MAX_SHUTDOWN_WAIT_TIME_MS = 600_000L;
    public static final long DEFAULT_CHECK_INTERVAL_TIME_MS = 60_000L;
  }

  public static class Controller {
    public static final String PREFIX_OF_CONFIG_OF_SEGMENT_FETCHER_FACTORY = "pinot.controller.segment.fetcher";
    public static final String PREFIX_OF_CONFIG_OF_PINOT_FS_FACTORY = "pinot.controller.storage.factory";
    public static final String HOST_HTTP_HEADER = "Pinot-Controller-Host";
    public static final String VERSION_HTTP_HEADER = "Pinot-Controller-Version";
    public static final String SEGMENT_NAME_HTTP_HEADER = "Pinot-Segment-Name";
    public static final String TABLE_NAME_HTTP_HEADER = "Pinot-Table-Name";
    public static final String PREFIX_OF_CONFIG_OF_PINOT_CRYPTER = "pinot.controller.crypter";
  }

  public static class Minion {
    public static final String INSTANCE_PREFIX = "Minion_";
    public static final String INSTANCE_TYPE = "minion";
    public static final String UNTAGGED_INSTANCE = "minion_untagged";
    public static final String METRICS_PREFIX = "pinot.minion.";
    public static final String METADATA_EVENT_OBSERVER_PREFIX = "metadata.event.notifier";

    // Config keys
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

  public static class Metric {
    public static class Server {
      public static final String CURRENT_NUMBER_OF_SEGMENTS = "currentNumberOfSegments";
      public static final String CURRENT_NUMBER_OF_DOCUMENTS = "currentNumberOfDocuments";
      public static final String NUMBER_OF_DELETED_SEGMENTS = "numberOfDeletedSegments";
    }
  }

  public static class Segment {
    public static class Realtime {
      public enum Status {
        IN_PROGRESS,
        DONE
      }
      public static final String STATUS = "segment.realtime.status";
    }

    public static class Offline {
      public static final String DOWNLOAD_URL = "segment.offline.download.url";
      public static final String PUSH_TIME = "segment.offline.push.time";
      public static final String REFRESH_TIME = "segment.offline.refresh.time";
    }

    public static final String SEGMENT_NAME = "segment.name";
    public static final String TABLE_NAME = "segment.table.name";
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
      OFFLINE,
      REALTIME
    }
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
