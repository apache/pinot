/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.util.Collections;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.google.common.collect.Sets;


public class CommonConstants {
  public static class Helix {
    public static final String IS_SHUTDOWN_IN_PROGRESS = "shutdownInProgress";

    public static final String PREFIX_OF_BROKER_RESOURCE_TAG = "broker_";
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
        public static final String DROPPED = "DROPPED";
      }

      public static class RealtimeSegmentOnlineOfflineStateModel {
        public static final String ONLINE = "ONLINE";
        public static final String OFFLINE = "OFFLINE";
        public static final String ERROR = "ERROR";
        public static final String DROPPED = "DROPPED";
        public static final String CONSUMING = "CONSUMING";
        public static final String CONVERTING = "CONVERTING";
      }

      public static class BrokerOnlineOfflineStateModel {
        public static final String ONLINE = "ONLINE";
        public static final String OFFLINE = "OFFLINE";
        public static final String DROPPED = "DROPPED";
        public static final String ERROR = "ERROR";
      }
    }

    /**
     * Resources names that are not Pinot resources (such as broker resource)
     */
    public static final Set<String> NON_PINOT_RESOURCE_RESOURCE_NAMES =
        Collections.unmodifiableSet(Sets.newHashSet(BROKER_RESOURCE_INSTANCE));

    public static class DataSource {
      public static final String SCHEMA = "schema";
      public static final String KAFKA = "kafka";
      public static final String STREAM_PREFIX = "stream";
      public static enum SegmentAssignmentStrategyType {
        RandomAssignmentStrategy,
        BalanceNumSegmentAssignmentStrategy,
        BucketizedSegmentAssignmentStrategy;
      }

      public static class Schema {
        public static final String COLUMN_NAME = "columnName";
        public static final String DATA_TYPE = "dataType";
        public static final String DELIMETER = "delimeter";
        public static final String IS_SINGLE_VALUE = "isSingleValue";
        public static final String FIELD_TYPE = "fieldType";
        public static final String TIME_UNIT = "timeUnit";
      }

      public static class Realtime {
        public static final String STREAM_TYPE = "streamType";
        // Time threshold that will keep the realtime segment open for before we convert it into an offline segment
        public static final String REALTIME_SEGMENT_FLUSH_TIME = "realtime.segment.flush.threshold.time";
        // Time threshold that controller will wait for the segment to be built by the server
        public static final String SEGMENT_COMMIT_TIMEOUT_SECONDS = "realtime.segment.commit.timeoutSeconds";
        /**
         * Row count flush threshold for realtime segments. This behaves in a similar way for HLC and LLC. For HLC,
         * since there is only one consumer per server, this size is used as the size of the consumption buffer and
         * determines after how many rows we flush to disk. For example, if this threshold is set to two million rows,
         * then a high level consumer would have a buffer size of two million.
         *
         * For LLC, this size is divided across all the segments assigned to a given server and is set on a per segment
         * basis. Assuming a low level consumer server is assigned four Kafka partitions to consume from and a flush
         * size of two million, then each consuming segment would have a flush size of five hundred thousand rows, for a
         * total of two million rows in memory.
         *
         * Keep in mind that this NOT a hard threshold, as other tables can also be assigned to this server, and that in
         * certain conditions (eg. if the number of servers, replicas of Kafka partitions changes) where Kafka partition
         * to server assignment changes, it's possible to end up with more (or less) than this number of rows in memory.
         */
        public static final String REALTIME_SEGMENT_FLUSH_SIZE = "realtime.segment.flush.threshold.size";

        public static enum StreamType {
          kafka
        }

        public static class Kafka {

          public static enum ConsumerType {
            simple,
            highLevel
          }

          public static final String TOPIC_NAME = "kafka.topic.name";
          public static final String CONSUMER_TYPE = "kafka.consumer.type";
          public static final String DECODER_CLASS = "kafka.decoder.class.name";
          public static final String DECODER_PROPS_PREFIX = "kafka.decoder.prop";
          public static final String KAFKA_CONSUMER_PROPS_PREFIX = "kafka.consumer.prop";
          public static final String KAFKA_CONNECTION_TIMEOUT_MILLIS = "kafka.connection.timeout.ms";
          public static final String KAFKA_FETCH_TIMEOUT_MILLIS = "kafka.fetch.timeout.ms";
          public static final String ZK_BROKER_URL = "kafka.zk.broker.url";
          public static final String KAFKA_BROKER_LIST = "kafka.broker.list";

          // Consumer properties
          public static final String AUTO_OFFSET_RESET = "auto.offset.reset";

          public static String getDecoderPropertyKeyFor(String key) {
            return StringUtils.join(new String[] { DECODER_PROPS_PREFIX, key }, ".");
          }

          public static String getDecoderPropertyKey(String incoming) {
            return incoming.split(DECODER_PROPS_PREFIX + ".")[1];
          }

          public static String getConsumerPropertyKey(String incoming) {
            return incoming.split(KAFKA_CONSUMER_PROPS_PREFIX + ".")[1];
          }

          public static class HighLevelConsumer {
            public static final String ZK_CONNECTION_STRING = "kafka.hlc.zk.connect.string";
            public static final String GROUP_ID = "kafka.hlc.group.id";
          }
        }
      }

    }

    public static class Instance {
      public static final String INSTANCE_NAME = "instance.name";
      public static final String GROUP_ID_SUFFIX = "kafka.hlc.groupId";
      public static final String PARTITION_SUFFIX = "kafka.hlc.partition";
      public static final String ADMIN_PORT_KEY = "adminPort";
    }

    public static enum TableType {
      OFFLINE,
      REALTIME,
      HYBRID;

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

  public static class Server {
    public static final String CONFIG_OF_INSTANCE_DATA_DIR = "pinot.server.instance.dataDir";
    public static final String CONFIG_OF_INSTANCE_SEGMENT_TAR_DIR = "pinot.server.instance.segmentTarDir";
    public static final String CONFIG_OF_INSTANCE_READ_MODE = "pinot.server.instance.readMode";
    public static final String CONFIG_OF_INSTANCE_DATA_MANAGER_CLASS = "pinot.server.instance.data.manager.class";
    public static final String CONFIG_OF_INSTANCE_SEGMENT_METADATA_LOADER_CLASS =
        "pinot.server.instance.segment.metadata.loader.class";
    public static final String CONFIG_OF_QUERY_EXECUTOR_PRUNER_CLASS = "pinot.server.query.executor.pruner.class";
    public static final String CONFIG_OF_QUERY_EXECUTOR_TIMEOUT = "pinot.server.query.executor.timeout";
    public static final String CONFIG_OF_QUERY_EXECUTOR_CLASS = "pinot.server.query.executor.class";
    public static final String CONFIG_OF_REQUEST_HANDLER_FACTORY_CLASS = "pinot.server.requestHandlerFactory.class";
    public static final String CONFIG_OF_NETTY_PORT = "pinot.server.netty.port";
    public static final String CONFIG_OF_ADMIN_API_PORT = "pinot.server.adminapi.port";
    public static final String CONFIG_OF_SEGMENT_LOAD_MAX_RETRY_COUNT = "pinot.server.segment.loadMaxRetryCount";
    public static final String CONFIG_OF_SEGMENT_LOAD_MIN_RETRY_DELAY_MILLIS =
        "pinot.server.segment.minRetryDelayMillis";
    public static final String CONFIG_OF_SEGMENT_FORMAT_VERSION = "pinot.server.instance.segment.format.version";
    public static final String CONFIG_OF_ENABLE_DEFAULT_COLUMNS = "pinot.server.instance.enable.default.columns";

    public static final String DEFAULT_ADMIN_API_PORT = "8097";
    public static final String DEFAULT_READ_MODE = "heap";
    public static final String DEFAULT_INSTANCE_DATA_DIR = "/tmp/PinotServer/test/index";
    public static final String DEFAULT_INSTANCE_SEGMENT_TAR_DIR = "/tmp/PinotServer/test/segmentTar";
    public static final String DEFAULT_SEGMENT_METADATA_LOADER_CLASS =
        "com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentMetadataLoader";
    public static final String DEFAULT_DATA_MANAGER_CLASS =
        "com.linkedin.pinot.server.starter.helix.HelixInstanceDataManager";
    public static final String DEFAULT_QUERY_EXECUTOR_CLASS =
        "com.linkedin.pinot.core.query.executor.ServerQueryExecutorV1Impl";
    public static final String DEFAULT_QUERY_EXECUTOR_TIMEOUT = "150000";
    public static final String DEFAULT_REQUEST_HANDLER_FACTORY_CLASS =
        "com.linkedin.pinot.server.request.SimpleRequestHandlerFactory";
    public static final String DEFAULT_SEGMENT_LOAD_MAX_RETRY_COUNT = "5";
    public static final String DEFAULT_SEGMENT_LOAD_MIN_RETRY_DELAY_MILLIS = "60000";
    public static final String PREFIX_OF_CONFIG_OF_SEGMENT_FETCHER_FACTORY = "pinot.server.segment.fetcher";
    public static final String DEFAULT_SEGMENT_FORMAT_VERSION = "v3";
    public static final String DEFAULT_STAR_TREE_FORMAT_VERSION = "OFF_HEAP";
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
      public static enum Status {
        IN_PROGRESS,
        DONE
      }
      public static final String STATUS = "segment.realtime.status";
    }

    public static class Offline {
      public static final String DOWNLOAD_URL = "segment.offline.download.url";
      public static final String PUSH_TIME = "segment.offline.push.time";
      public static final String REFRESH_TIME = "segment.offline.refresh.time";;
    }

    public static final String SEGMENT_NAME = "segment.name";
    public static final String TABLE_NAME = "segment.table.name";
    public static final String SEGMENT_TYPE = "segment.type";
    public static final String INDEX_VERSION = "segment.index.version";
    public static final String START_TIME = "segment.start.time";
    public static final String END_TIME = "segment.end.time";
    public static final String TIME_UNIT = "segment.time.unit";
    public static final String TOTAL_DOCS = "segment.total.docs";
    public static final String CRC = "segment.crc";
    public static final String CREATION_TIME = "segment.creation.time";
    public static final String FLUSH_THRESHOLD_SIZE = "segment.flush.threshold.size";

    public static enum SegmentType {
      OFFLINE,
      REALTIME
    }
  }
}
