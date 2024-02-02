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
package org.apache.pinot.spi.utils;

import com.google.common.collect.ImmutableList;
import java.io.File;
import java.math.BigDecimal;
import java.util.List;
import org.apache.pinot.spi.config.instance.InstanceType;


public class CommonConstants {
  private CommonConstants() {
  }

  public static final String ENVIRONMENT_IDENTIFIER = "environment";
  public static final String INSTANCE_FAILURE_DOMAIN = "failureDomain";
  public static final String DEFAULT_FAILURE_DOMAIN = "No such domain";

  public static final String PREFIX_OF_SSL_SUBSET = "ssl";
  public static final String HTTP_PROTOCOL = "http";
  public static final String HTTPS_PROTOCOL = "https";

  public static final String KEY_OF_AUTH = "auth";

  public static final String TABLE_NAME = "tableName";

  public static final String UNKNOWN = "unknown";
  public static final String CONFIG_OF_METRICS_FACTORY_CLASS_NAME = "factory.className";
  public static final String CONFIG_OF_BROKER_EVENT_LISTENER_CLASS_NAME = "factory.className";
  public static final String CONFIG_OF_REQUEST_CONTEXT_TRACKED_HEADER_KEYS = "request.context.tracked.header.keys";
  public static final String DEFAULT_METRICS_FACTORY_CLASS_NAME =
      "org.apache.pinot.plugin.metrics.yammer.YammerMetricsFactory";
  public static final String DEFAULT_BROKER_EVENT_LISTENER_CLASS_NAME =
      "org.apache.pinot.spi.eventlistener.query.NoOpBrokerQueryEventListener";

  public static final String SWAGGER_AUTHORIZATION_KEY = "oauth";
  public static final String CONFIG_OF_SWAGGER_RESOURCES_PATH = "META-INF/resources/webjars/swagger-ui/5.1.0/";

  /**
   * The state of the consumer for a given segment
   */
  public enum ConsumerState {
    CONSUMING, NOT_CONSUMING // In error state
  }

  public static class Table {
    public static final String PUSH_FREQUENCY_HOURLY = "hourly";
    public static final String PUSH_FREQUENCY_DAILY = "daily";
    public static final String PUSH_FREQUENCY_WEEKLY = "weekly";
    public static final String PUSH_FREQUENCY_MONTHLY = "monthly";
  }

  public static class Helix {
    public static final String IS_SHUTDOWN_IN_PROGRESS = "shutdownInProgress";
    public static final String QUERIES_DISABLED = "queriesDisabled";
    public static final String QUERY_RATE_LIMIT_DISABLED = "queryRateLimitDisabled";

    public static final String INSTANCE_CONNECTED_METRIC_NAME = "helix.connected";

    public static final String PREFIX_OF_CONTROLLER_INSTANCE = "Controller_";
    public static final String PREFIX_OF_BROKER_INSTANCE = "Broker_";
    public static final String PREFIX_OF_SERVER_INSTANCE = "Server_";
    public static final String PREFIX_OF_MINION_INSTANCE = "Minion_";

    public static final int CONTROLLER_INSTANCE_PREFIX_LENGTH = PREFIX_OF_CONTROLLER_INSTANCE.length();
    public static final int BROKER_INSTANCE_PREFIX_LENGTH = PREFIX_OF_BROKER_INSTANCE.length();
    public static final int SERVER_INSTANCE_PREFIX_LENGTH = PREFIX_OF_SERVER_INSTANCE.length();
    public static final int MINION_INSTANCE_PREFIX_LENGTH = PREFIX_OF_MINION_INSTANCE.length();

    public static final String BROKER_RESOURCE_INSTANCE = "brokerResource";
    public static final String LEAD_CONTROLLER_RESOURCE_NAME = "leadControllerResource";

    public static final String LEAD_CONTROLLER_RESOURCE_ENABLED_KEY = "RESOURCE_ENABLED";

    public static final String ENABLE_CASE_INSENSITIVE_KEY = "enable.case.insensitive";
    public static final boolean DEFAULT_ENABLE_CASE_INSENSITIVE = true;
    public static final String ALLOW_TABLE_NAME_WITH_DATABASE = "allow.table.name.with.database";
    public static final boolean DEFAULT_ALLOW_TABLE_NAME_WITH_DATABASE = false;

    public static final String DEFAULT_HYPERLOGLOG_LOG2M_KEY = "default.hyperloglog.log2m";
    public static final int DEFAULT_HYPERLOGLOG_LOG2M = 8;
    public static final int DEFAULT_HYPERLOGLOG_PLUS_P = 14;
    public static final int DEFAULT_HYPERLOGLOG_PLUS_SP = 0;

    // 2 to the power of 14, for tradeoffs see datasketches library documentation:
    // https://datasketches.apache.org/docs/Theta/ThetaErrorTable.html
    public static final int DEFAULT_THETA_SKETCH_NOMINAL_ENTRIES = 16384;

    public static final int DEFAULT_TUPLE_SKETCH_LGK = 16;

    public static final int DEFAULT_CPC_SKETCH_LGK = 12;
    public static final int DEFAULT_ULTRALOGLOG_P = 12;

    // Whether to rewrite DistinctCount to DistinctCountBitmap
    public static final String ENABLE_DISTINCT_COUNT_BITMAP_OVERRIDE_KEY = "enable.distinct.count.bitmap.override";

    // More information on why these numbers are set can be found in the following doc:
    // https://cwiki.apache.org/confluence/display/PINOT/Controller+Separation+between+Helix+and+Pinot
    public static final int NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE = 24;
    public static final int LEAD_CONTROLLER_RESOURCE_REPLICA_COUNT = 1;
    public static final int MIN_ACTIVE_REPLICAS = 0;

    // Instance tags
    public static final String CONTROLLER_INSTANCE = "controller";
    public static final String UNTAGGED_BROKER_INSTANCE = "broker_untagged";
    public static final String UNTAGGED_SERVER_INSTANCE = "server_untagged";
    public static final String UNTAGGED_MINION_INSTANCE = "minion_untagged";

    public static class StateModel {
      public static class SegmentStateModel {
        public static final String ONLINE = "ONLINE";
        public static final String OFFLINE = "OFFLINE";
        public static final String ERROR = "ERROR";
        public static final String CONSUMING = "CONSUMING";
      }

      public static class BrokerResourceStateModel {
        public static final String ONLINE = "ONLINE";
        public static final String OFFLINE = "OFFLINE";
        public static final String ERROR = "ERROR";
      }
    }

    public static class ZkClient {
      public static final int DEFAULT_CONNECT_TIMEOUT_MS = 60_000;
      public static final int DEFAULT_SESSION_TIMEOUT_MS = 30_000;
      // Retry interval and count for ZK operations where we would rather fail than get an empty (wrong) result back
      public static final int RETRY_INTERVAL_MS = 50;
      public static final int RETRY_COUNT = 2;
      public static final String ZK_CLIENT_CONNECTION_TIMEOUT_MS_CONFIG = "zk.client.connection.timeout.ms";
      public static final String ZK_CLIENT_SESSION_TIMEOUT_MS_CONFIG = "zk.client.session.timeout.ms";
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
      @Deprecated
      public static final String INSTANCE_ID_KEY = "instanceId";
      public static final String DATA_DIR_KEY = "dataDir";
      public static final String ADMIN_PORT_KEY = "adminPort";
      public static final String ADMIN_HTTPS_PORT_KEY = "adminHttpsPort";
      public static final String GRPC_PORT_KEY = "grpcPort";
      public static final String NETTY_TLS_PORT_KEY = "nettyTlsPort";

      public static final String MULTI_STAGE_QUERY_ENGINE_SERVICE_PORT_KEY = "queryServerPort";
      public static final String MULTI_STAGE_QUERY_ENGINE_MAILBOX_PORT_KEY = "queryMailboxPort";

      public static final String SYSTEM_RESOURCE_INFO_KEY = "SYSTEM_RESOURCE_INFO";
    }

    public static final String SET_INSTANCE_ID_TO_HOSTNAME_KEY = "pinot.set.instance.id.to.hostname";

    public static final String KEY_OF_SERVER_NETTY_PORT = "pinot.server.netty.port";
    public static final int DEFAULT_SERVER_NETTY_PORT = 8098;
    public static final String KEY_OF_SERVER_NETTYTLS_PORT = Server.SERVER_NETTYTLS_PREFIX + ".port";
    public static final int DEFAULT_SERVER_NETTYTLS_PORT = 8091;
    public static final String KEY_OF_BROKER_QUERY_PORT = "pinot.broker.client.queryPort";
    public static final int DEFAULT_BROKER_QUERY_PORT = 8099;
    public static final String KEY_OF_SERVER_NETTY_HOST = "pinot.server.netty.host";
    public static final String KEY_OF_MINION_HOST = "pinot.minion.host";
    public static final String KEY_OF_MINION_PORT = "pinot.minion.port";

    // NOTE: Helix will disconnect the manager and disable the instance if it detects flapping (too frequent disconnect
    // from ZooKeeper). Setting flapping time window to a small value can avoid this from happening. Helix ignores the
    // non-positive value, so set the default value as 1.
    public static final String CONFIG_OF_CONTROLLER_FLAPPING_TIME_WINDOW_MS = "pinot.controller.flapping.timeWindowMs";
    public static final String CONFIG_OF_BROKER_FLAPPING_TIME_WINDOW_MS = "pinot.broker.flapping.timeWindowMs";
    public static final String CONFIG_OF_SERVER_FLAPPING_TIME_WINDOW_MS = "pinot.server.flapping.timeWindowMs";
    public static final String CONFIG_OF_MINION_FLAPPING_TIME_WINDOW_MS = "pinot.minion.flapping.timeWindowMs";
    public static final String CONFIG_OF_HELIX_INSTANCE_MAX_STATE_TRANSITIONS =
        "pinot.helix.instance.state.maxStateTransitions";
    public static final String DEFAULT_HELIX_INSTANCE_MAX_STATE_TRANSITIONS = "100000";
    public static final String DEFAULT_FLAPPING_TIME_WINDOW_MS = "1";
    public static final String PINOT_SERVICE_ROLE = "pinot.service.role";
    public static final String CONFIG_OF_CLUSTER_NAME = "pinot.cluster.name";
    public static final String CONFIG_OF_ZOOKEEPR_SERVER = "pinot.zk.server";

    public static final String CONFIG_OF_PINOT_CONTROLLER_STARTABLE_CLASS = "pinot.controller.startable.class";
    public static final String CONFIG_OF_PINOT_BROKER_STARTABLE_CLASS = "pinot.broker.startable.class";
    public static final String CONFIG_OF_PINOT_SERVER_STARTABLE_CLASS = "pinot.server.startable.class";
    public static final String CONFIG_OF_PINOT_MINION_STARTABLE_CLASS = "pinot.minion.startable.class";

    public static final String CONFIG_OF_MULTI_STAGE_ENGINE_ENABLED = "pinot.multistage.engine.enabled";
    public static final boolean DEFAULT_MULTI_STAGE_ENGINE_ENABLED = true;
  }

  public static class Broker {
    public static final String ROUTING_TABLE_CONFIG_PREFIX = "pinot.broker.routing.table";
    public static final String ACCESS_CONTROL_CONFIG_PREFIX = "pinot.broker.access.control";
    public static final String METRICS_CONFIG_PREFIX = "pinot.broker.metrics";
    public static final String EVENT_LISTENER_CONFIG_PREFIX = "pinot.broker.event.listener";
    public static final String CONFIG_OF_METRICS_NAME_PREFIX = "pinot.broker.metrics.prefix";
    public static final String DEFAULT_METRICS_NAME_PREFIX = "pinot.broker.";

    public static final String CONFIG_OF_DELAY_SHUTDOWN_TIME_MS = "pinot.broker.delayShutdownTimeMs";
    public static final long DEFAULT_DELAY_SHUTDOWN_TIME_MS = 10_000L;
    public static final String CONFIG_OF_ENABLE_TABLE_LEVEL_METRICS = "pinot.broker.enableTableLevelMetrics";
    public static final boolean DEFAULT_ENABLE_TABLE_LEVEL_METRICS = true;
    public static final String CONFIG_OF_ALLOWED_TABLES_FOR_EMITTING_METRICS =
        "pinot.broker.allowedTablesForEmittingMetrics";

    public static final String CONFIG_OF_BROKER_QUERY_REWRITER_CLASS_NAMES = "pinot.broker.query.rewriter.class.names";
    public static final String CONFIG_OF_BROKER_QUERY_RESPONSE_LIMIT = "pinot.broker.query.response.limit";
    public static final int DEFAULT_BROKER_QUERY_RESPONSE_LIMIT = Integer.MAX_VALUE;
    public static final String CONFIG_OF_BROKER_QUERY_LOG_LENGTH = "pinot.broker.query.log.length";
    public static final int DEFAULT_BROKER_QUERY_LOG_LENGTH = Integer.MAX_VALUE;
    public static final String CONFIG_OF_BROKER_QUERY_LOG_MAX_RATE_PER_SECOND =
        "pinot.broker.query.log.maxRatePerSecond";
    public static final String CONFIG_OF_BROKER_ENABLE_QUERY_CANCELLATION = "pinot.broker.enable.query.cancellation";
    public static final double DEFAULT_BROKER_QUERY_LOG_MAX_RATE_PER_SECOND = 10_000d;
    public static final String CONFIG_OF_BROKER_TIMEOUT_MS = "pinot.broker.timeoutMs";
    public static final long DEFAULT_BROKER_TIMEOUT_MS = 10_000L;
    public static final String CONFIG_OF_BROKER_ID = "pinot.broker.instance.id";
    public static final String CONFIG_OF_BROKER_INSTANCE_TAGS = "pinot.broker.instance.tags";
    public static final String CONFIG_OF_BROKER_HOSTNAME = "pinot.broker.hostname";
    public static final String CONFIG_OF_SWAGGER_USE_HTTPS = "pinot.broker.swagger.use.https";
    // Comma separated list of packages that contains javax service resources.
    public static final String BROKER_RESOURCE_PACKAGES = "broker.restlet.api.resource.packages";
    public static final String DEFAULT_BROKER_RESOURCE_PACKAGES = "org.apache.pinot.broker.api.resources";

    // Configuration to consider the broker ServiceStatus as being STARTED if the percent of resources (tables) that
    // are ONLINE for this broker has crossed the threshold percentage of the total number of tables
    // that it is expected to serve.
    public static final String CONFIG_OF_BROKER_MIN_RESOURCE_PERCENT_FOR_START =
        "pinot.broker.startup.minResourcePercent";
    public static final double DEFAULT_BROKER_MIN_RESOURCE_PERCENT_FOR_START = 100.0;
    public static final String CONFIG_OF_ENABLE_QUERY_LIMIT_OVERRIDE = "pinot.broker.enable.query.limit.override";

    // Config for number of threads to use for Broker reduce-phase.
    public static final String CONFIG_OF_MAX_REDUCE_THREADS_PER_QUERY = "pinot.broker.max.reduce.threads.per.query";
    public static final int DEFAULT_MAX_REDUCE_THREADS_PER_QUERY =
        Math.max(1, Math.min(10, Runtime.getRuntime().availableProcessors() / 2));
    // Same logic as CombineOperatorUtils

    // Config for Jersey ThreadPoolExecutorProvider.
    // By default, Jersey uses the default unbounded thread pool to process queries.
    // By enabling it, BrokerManagedAsyncExecutorProvider will be used to create a bounded thread pool.
    public static final String CONFIG_OF_ENABLE_BOUNDED_JERSEY_THREADPOOL_EXECUTOR =
        "pinot.broker.enable.bounded.jersey.threadpool.executor";
    public static final boolean DEFAULT_ENABLE_BOUNDED_JERSEY_THREADPOOL_EXECUTOR = false;
    // Default capacities for the bounded thread pool
    public static final String CONFIG_OF_JERSEY_THREADPOOL_EXECUTOR_MAX_POOL_SIZE =
        "pinot.broker.jersey.threadpool.executor.max.pool.size";
    public static final int DEFAULT_JERSEY_THREADPOOL_EXECUTOR_MAX_POOL_SIZE =
        Runtime.getRuntime().availableProcessors() * 2;
    public static final String CONFIG_OF_JERSEY_THREADPOOL_EXECUTOR_CORE_POOL_SIZE =
        "pinot.broker.jersey.threadpool.executor.core.pool.size";
    public static final int DEFAULT_JERSEY_THREADPOOL_EXECUTOR_CORE_POOL_SIZE =
        Runtime.getRuntime().availableProcessors() * 2;
    public static final String CONFIG_OF_JERSEY_THREADPOOL_EXECUTOR_QUEUE_SIZE =
        "pinot.broker.jersey.threadpool.executor.queue.size";
    public static final int DEFAULT_JERSEY_THREADPOOL_EXECUTOR_QUEUE_SIZE = Integer.MAX_VALUE;

    // used for SQL GROUP BY during broker reduce
    public static final String CONFIG_OF_BROKER_GROUPBY_TRIM_THRESHOLD = "pinot.broker.groupby.trim.threshold";
    public static final int DEFAULT_BROKER_GROUPBY_TRIM_THRESHOLD = 1_000_000;
    public static final String CONFIG_OF_BROKER_MIN_GROUP_TRIM_SIZE = "pinot.broker.min.group.trim.size";
    public static final int DEFAULT_BROKER_MIN_GROUP_TRIM_SIZE = 5000;

    // Configure the request handler type used by broker to handler inbound query request.
    // NOTE: the request handler type refers to the communication between Broker and Server.
    public static final String BROKER_REQUEST_HANDLER_TYPE = "pinot.broker.request.handler.type";
    public static final String NETTY_BROKER_REQUEST_HANDLER_TYPE = "netty";
    public static final String GRPC_BROKER_REQUEST_HANDLER_TYPE = "grpc";
    public static final String MULTI_STAGE_BROKER_REQUEST_HANDLER_TYPE = "multistage";
    public static final String DEFAULT_BROKER_REQUEST_HANDLER_TYPE = NETTY_BROKER_REQUEST_HANDLER_TYPE;

    public static final String BROKER_TLS_PREFIX = "pinot.broker.tls";
    public static final String BROKER_NETTY_PREFIX = "pinot.broker.netty";
    public static final String BROKER_NETTYTLS_ENABLED = "pinot.broker.nettytls.enabled";
    //Set to true to load all services tagged and compiled with hk2-metadata-generator. Default to False
    public static final String BROKER_SERVICE_AUTO_DISCOVERY = "pinot.broker.service.auto.discovery";

    public static final String DISABLE_GROOVY = "pinot.broker.disable.query.groovy";
    public static final boolean DEFAULT_DISABLE_GROOVY = true;
    // Rewrite potential expensive functions to their approximation counterparts
    // - DISTINCT_COUNT -> DISTINCT_COUNT_SMART_HLL
    // - PERCENTILE -> PERCENTILE_SMART_TDIGEST
    public static final String USE_APPROXIMATE_FUNCTION = "pinot.broker.use.approximate.function";

    public static final String CONTROLLER_URL = "pinot.broker.controller.url";

    public static final String CONFIG_OF_BROKER_REQUEST_CLIENT_IP_LOGGING = "pinot.broker.request.client.ip.logging";

    // TODO: Support populating clientIp for GrpcRequestIdentity.
    public static final boolean DEFAULT_BROKER_REQUEST_CLIENT_IP_LOGGING = false;

    public static final String CONFIG_OF_LOGGER_ROOT_DIR = "pinot.broker.logger.root.dir";
    public static final String CONFIG_OF_SWAGGER_BROKER_ENABLED = "pinot.broker.swagger.enabled";
    public static final boolean DEFAULT_SWAGGER_BROKER_ENABLED = true;
    public static final String CONFIG_OF_ENABLE_THREAD_CPU_TIME_MEASUREMENT =
        "pinot.broker.instance.enableThreadCpuTimeMeasurement";
    public static final String CONFIG_OF_ENABLE_THREAD_ALLOCATED_BYTES_MEASUREMENT =
        "pinot.broker.instance.enableThreadAllocatedBytesMeasurement";
    public static final boolean DEFAULT_ENABLE_THREAD_CPU_TIME_MEASUREMENT = false;
    public static final boolean DEFAULT_THREAD_ALLOCATED_BYTES_MEASUREMENT = false;
    public static final String CONFIG_OF_BROKER_RESULT_REWRITER_CLASS_NAMES =
        "pinot.broker.result.rewriter.class.names";

    public static final String CONFIG_OF_ENABLE_PARTITION_METADATA_MANAGER =
        "pinot.broker.enable.partition.metadata.manager";
    public static final boolean DEFAULT_ENABLE_PARTITION_METADATA_MANAGER = false;

    public static final String CONFIG_OF_USE_FIXED_REPLICA = "pinot.broker.use.fixed.replica";
    public static final boolean DEFAULT_USE_FIXED_REPLICA = false;

    // Broker config indicating the maximum serialized response size across all servers for a query. This value is
    // equally divided across all servers processing the query.
    public static final String CONFIG_OF_MAX_QUERY_RESPONSE_SIZE_BYTES = "pinot.broker.max.query.response.size.bytes";

    // Broker config indicating the maximum length of the serialized response per server for a query.
    public static final String CONFIG_OF_MAX_SERVER_RESPONSE_SIZE_BYTES = "pinot.broker.max.server.response.size.bytes";


    public static class Request {
      public static final String SQL = "sql";
      public static final String TRACE = "trace";
      public static final String DEBUG_OPTIONS = "debugOptions";
      public static final String QUERY_OPTIONS = "queryOptions";

      public static class QueryOptionKey {
        public static final String TIMEOUT_MS = "timeoutMs";
        public static final String SKIP_UPSERT = "skipUpsert";
        public static final String USE_STAR_TREE = "useStarTree";
        public static final String SCAN_STAR_TREE_NODES = "scanStarTreeNodes";
        public static final String ROUTING_OPTIONS = "routingOptions";
        public static final String USE_SCAN_REORDER_OPTIMIZATION = "useScanReorderOpt";
        public static final String MAX_EXECUTION_THREADS = "maxExecutionThreads";
        public static final String MIN_SEGMENT_GROUP_TRIM_SIZE = "minSegmentGroupTrimSize";
        public static final String MIN_SERVER_GROUP_TRIM_SIZE = "minServerGroupTrimSize";
        public static final String MIN_BROKER_GROUP_TRIM_SIZE = "minBrokerGroupTrimSize";
        public static final String NUM_REPLICA_GROUPS_TO_QUERY = "numReplicaGroupsToQuery";
        public static final String USE_FIXED_REPLICA = "useFixedReplica";
        public static final String EXPLAIN_PLAN_VERBOSE = "explainPlanVerbose";
        public static final String USE_MULTISTAGE_ENGINE = "useMultistageEngine";
        public static final String ENABLE_NULL_HANDLING = "enableNullHandling";
        public static final String SERVER_RETURN_FINAL_RESULT = "serverReturnFinalResult";
        // Reorder scan based predicates based on cardinality and number of selected values
        public static final String AND_SCAN_REORDERING = "AndScanReordering";

        public static final String ORDER_BY_ALGORITHM = "orderByAlgorithm";

        public static final String MULTI_STAGE_LEAF_LIMIT = "multiStageLeafLimit";
        public static final String NUM_GROUPS_LIMIT = "numGroupsLimit";
        public static final String MAX_INITIAL_RESULT_HOLDER_CAPACITY = "maxInitialResultHolderCapacity";
        public static final String GROUP_TRIM_THRESHOLD = "groupTrimThreshold";
        public static final String STAGE_PARALLELISM = "stageParallelism";

        public static final String IN_PREDICATE_PRE_SORTED = "inPredicatePreSorted";
        public static final String IN_PREDICATE_LOOKUP_ALGORITHM = "inPredicateLookupAlgorithm";

        public static final String DROP_RESULTS = "dropResults";

        // Maximum number of pending results blocks allowed in the streaming operator
        public static final String MAX_STREAMING_PENDING_BLOCKS = "maxStreamingPendingBlocks";

        // Handle JOIN Overflow
        public static final String MAX_ROWS_IN_JOIN = "maxRowsInJoin";
        public static final String JOIN_OVERFLOW_MODE = "joinOverflowMode";

        // Indicates the maximum length of the serialized response per server for a query.
        public static final String MAX_SERVER_RESPONSE_SIZE_BYTES = "maxServerResponseSizeBytes";

        // Indicates the maximum length of serialized response across all servers for a query. This value is equally
        // divided across all servers processing the query.
        public static final String MAX_QUERY_RESPONSE_SIZE_BYTES = "maxQueryResponseSizeBytes";

        // TODO: Remove these keys (only apply to PQL) after releasing 0.11.0
        @Deprecated
        public static final String PRESERVE_TYPE = "preserveType";
        @Deprecated
        public static final String RESPONSE_FORMAT = "responseFormat";
        @Deprecated
        public static final String GROUP_BY_MODE = "groupByMode";
      }

      public static class QueryOptionValue {
        public static final int DEFAULT_MAX_STREAMING_PENDING_BLOCKS = 100;
      }
    }

    public static class FailureDetector {
      public enum Type {
        // Do not detect any failure
        NO_OP,

        // Detect connection failures
        CONNECTION,

        // Use the custom failure detector of the configured class name
        CUSTOM
      }

      public static final String CONFIG_OF_TYPE = "pinot.broker.failure.detector.type";
      public static final String DEFAULT_TYPE = Type.NO_OP.name();
      public static final String CONFIG_OF_CLASS_NAME = "pinot.broker.failure.detector.class";

      // Exponential backoff delay of retrying an unhealthy server when a failure is detected
      public static final String CONFIG_OF_RETRY_INITIAL_DELAY_MS =
          "pinot.broker.failure.detector.retry.initial.delay.ms";
      public static final long DEFAULT_RETRY_INITIAL_DELAY_MS = 5_000L;
      public static final String CONFIG_OF_RETRY_DELAY_FACTOR = "pinot.broker.failure.detector.retry.delay.factor";
      public static final double DEFAULT_RETRY_DELAY_FACTOR = 2.0;
      public static final String CONFIG_OF_MAX_RETRIES = "pinot.broker.failure.detector.max.retries";
      public static final int DEFAULT_MAX_RETIRES = 10;
    }

    // Configs related to AdaptiveServerSelection.
    public static class AdaptiveServerSelector {
      /**
       * Adaptive Server Selection feature has 2 parts:
       * 1. Stats Collection
       * 2. Routing Strategy
       *
       * Stats Collection is controlled by the config CONFIG_OF_ENABLE_STATS_COLLECTION.
       * Routing Strategy is controlled by the config CONFIG_OF_TYPE.
       *
       *
       *
       * Stats Collection: Enabling/Disabling stats collection will dictate whether stats (like latency, # of inflight
       *                   requests) will be collected when queries are routed to/received from servers. It does not
       *                   have any impact on the Server Selection Strategy used.
       *
       * Routing Strategy: Decides what strategy should be used to pick a server. Note that this
       *                   routing strategy complements the existing Balanced/ReplicaGroup/StrictReplicaGroup
       *                   strategies and is not a replacement.The available strategies are as follows:
       *                   1. NO_OP: Uses the default behavior offered by Balanced/ReplicaGroup/StrictReplicaGroup
       *                   instance selectors. Does NOT require Stats Collection to be enabled.
       *                   2. NUM_INFLIGHT_REQ: Picks the best server based on the number of inflight requests for
       *                   each server. Requires Stats Collection to be enabled.
       *                   3. LATENCY: Picks the best server based on the Exponential Weighted Moving Averge of Latency
       *                   for each server. Requires Stats Collection to be enabled.
       *                   4. HYBRID: Picks the best server by computing a custom hybrid score based on both latency
       *                   and # inflight requests. This is based on the approach described in the paper
       *                   https://www.usenix.org/system/files/conference/nsdi15/nsdi15-paper-suresh.pdf. Requires Stats
       *                   Collection to be enabled.
       */

      public enum Type {
        NO_OP,

        NUM_INFLIGHT_REQ,

        LATENCY,

        HYBRID
      }

      private static final String CONFIG_PREFIX = "pinot.broker.adaptive.server.selector";

      // Determines the type of AdaptiveServerSelector to use.
      public static final String CONFIG_OF_TYPE = CONFIG_PREFIX + ".type";
      public static final String DEFAULT_TYPE = Type.NO_OP.name();

      // Determines whether stats collection is enabled. This can be enabled independent of CONFIG_OF_TYPE. This is
      // so that users have an option to just enable stats collection and analyze them before deciding and enabling
      // adaptive server selection.
      public static final String CONFIG_OF_ENABLE_STATS_COLLECTION = CONFIG_PREFIX + ".enable.stats.collection";
      public static final boolean DEFAULT_ENABLE_STATS_COLLECTION = false;

      // Parameters to tune exponential moving average.

      // The weightage to be given for a new incoming value. For example, alpha=0.30 will give 30% weightage to the
      // new value and 70% weightage to the existing value in the Exponential Weighted Moving Average calculation.
      public static final String CONFIG_OF_EWMA_ALPHA = CONFIG_PREFIX + ".ewma.alpha";
      public static final double DEFAULT_EWMA_ALPHA = 0.666;

      // If the EMA average has not been updated during a specified time window (defined by this property), the
      // EMA average value is automatically decayed by an incoming value of zero. This is required to bring a server
      // back to healthy state gracefully after it has experienced some form of slowness.
      public static final String CONFIG_OF_AUTODECAY_WINDOW_MS = CONFIG_PREFIX + ".autodecay.window.ms";
      public static final long DEFAULT_AUTODECAY_WINDOW_MS = 10 * 1000;

      // Determines the initial duration during which incoming values are skipped in the Exponential Moving Average
      // calculation. Until this duration has elapsed, average returned will be equal to AVG_INITIALIZATION_VAL.
      public static final String CONFIG_OF_WARMUP_DURATION_MS = CONFIG_PREFIX + ".warmup.duration";
      public static final long DEFAULT_WARMUP_DURATION_MS = 0;

      // Determines the initialization value for Exponential Moving Average.
      public static final String CONFIG_OF_AVG_INITIALIZATION_VAL = CONFIG_PREFIX + ".avg.initialization.val";
      public static final double DEFAULT_AVG_INITIALIZATION_VAL = 1.0;

      // Parameters related to Hybrid score.
      public static final String CONFIG_OF_HYBRID_SCORE_EXPONENT = CONFIG_PREFIX + ".hybrid.score.exponent";
      public static final int DEFAULT_HYBRID_SCORE_EXPONENT = 3;

      // Threadpool size of ServerRoutingStatsManager. This controls the number of threads available to update routing
      // stats for servers upon query submission and response arrival.
      public static final String CONFIG_OF_STATS_MANAGER_THREADPOOL_SIZE =
          CONFIG_PREFIX + ".stats.manager.threadpool.size";
      public static final int DEFAULT_STATS_MANAGER_THREADPOOL_SIZE = 2;
    }
  }

  public static class Server {
    public static final String INSTANCE_DATA_MANAGER_CONFIG_PREFIX = "pinot.server.instance";
    public static final String QUERY_EXECUTOR_CONFIG_PREFIX = "pinot.server.query.executor";
    public static final String METRICS_CONFIG_PREFIX = "pinot.server.metrics";

    public static final String CONFIG_OF_INSTANCE_ID = "pinot.server.instance.id";
    public static final String CONFIG_OF_INSTANCE_DATA_DIR = "pinot.server.instance.dataDir";
    public static final String CONFIG_OF_CONSUMER_DIR = "pinot.server.instance.consumerDir";
    public static final String CONFIG_OF_INSTANCE_SEGMENT_TAR_DIR = "pinot.server.instance.segmentTarDir";
    public static final String CONFIG_OF_INSTANCE_READ_MODE = "pinot.server.instance.readMode";
    public static final String CONFIG_OF_INSTANCE_RELOAD_CONSUMING_SEGMENT =
        "pinot.server.instance.reload.consumingSegment";
    public static final String CONFIG_OF_INSTANCE_DATA_MANAGER_CLASS = "pinot.server.instance.data.manager.class";

    // Query logger related configs
    public static final String CONFIG_OF_QUERY_LOG_MAX_RATE = "pinot.server.query.log.maxRatePerSecond";
    @Deprecated
    public static final String DEPRECATED_CONFIG_OF_QUERY_LOG_MAX_RATE =
        "pinot.query.scheduler.query.log.maxRatePerSecond";
    public static final double DEFAULT_QUERY_LOG_MAX_RATE = 10_000;
    public static final String CONFIG_OF_QUERY_LOG_DROPPED_REPORT_MAX_RATE =
        "pinot.server.query.log.droppedReportMaxRatePerSecond";
    public static final double DEFAULT_QUERY_LOG_DROPPED_REPORT_MAX_RATE = 1;

    // Query executor related configs
    public static final String CONFIG_OF_QUERY_EXECUTOR_CLASS = "pinot.server.query.executor.class";
    public static final String CONFIG_OF_QUERY_EXECUTOR_PRUNER_CLASS = "pinot.server.query.executor.pruner.class";
    public static final String CONFIG_OF_QUERY_EXECUTOR_PLAN_MAKER_CLASS =
        "pinot.server.query.executor.plan.maker.class";
    public static final String CONFIG_OF_QUERY_EXECUTOR_TIMEOUT = "pinot.server.query.executor.timeout";
    public static final String CONFIG_OF_QUERY_EXECUTOR_NUM_GROUPS_LIMIT =
        "pinot.server.query.executor.num.groups.limit";
    public static final String CONFIG_OF_QUERY_EXECUTOR_MAX_INITIAL_RESULT_HOLDER_CAPACITY =
        "pinot.server.query.executor.max.init.group.holder.capacity";

    public static final String CONFIG_OF_TRANSFORM_FUNCTIONS = "pinot.server.transforms";
    public static final String CONFIG_OF_SERVER_QUERY_REWRITER_CLASS_NAMES = "pinot.server.query.rewriter.class.names";
    public static final String CONFIG_OF_ENABLE_QUERY_CANCELLATION = "pinot.server.enable.query.cancellation";
    public static final String CONFIG_OF_NETTY_SERVER_ENABLED = "pinot.server.netty.enabled";
    public static final boolean DEFAULT_NETTY_SERVER_ENABLED = true;
    public static final String CONFIG_OF_ENABLE_GRPC_SERVER = "pinot.server.grpc.enable";
    public static final boolean DEFAULT_ENABLE_GRPC_SERVER = true;
    public static final String CONFIG_OF_GRPC_PORT = "pinot.server.grpc.port";
    public static final int DEFAULT_GRPC_PORT = 8090;
    public static final String CONFIG_OF_GRPCTLS_SERVER_ENABLED = "pinot.server.grpctls.enabled";
    public static final boolean DEFAULT_GRPCTLS_SERVER_ENABLED = false;
    public static final String CONFIG_OF_NETTYTLS_SERVER_ENABLED = "pinot.server.nettytls.enabled";
    public static final boolean DEFAULT_NETTYTLS_SERVER_ENABLED = false;
    public static final String CONFIG_OF_SWAGGER_SERVER_ENABLED = "pinot.server.swagger.enabled";
    public static final boolean DEFAULT_SWAGGER_SERVER_ENABLED = true;
    public static final String CONFIG_OF_SWAGGER_USE_HTTPS = "pinot.server.swagger.use.https";
    public static final String CONFIG_OF_ADMIN_API_PORT = "pinot.server.adminapi.port";
    public static final int DEFAULT_ADMIN_API_PORT = 8097;
    public static final String CONFIG_OF_SERVER_RESOURCE_PACKAGES = "server.restlet.api.resource.packages";
    public static final String DEFAULT_SERVER_RESOURCE_PACKAGES = "org.apache.pinot.server.api.resources";

    public static final String CONFIG_OF_SEGMENT_FORMAT_VERSION = "pinot.server.instance.segment.format.version";
    public static final String CONFIG_OF_REALTIME_OFFHEAP_ALLOCATION = "pinot.server.instance.realtime.alloc.offheap";
    public static final String CONFIG_OF_REALTIME_OFFHEAP_DIRECT_ALLOCATION =
        "pinot.server.instance.realtime.alloc.offheap.direct";
    public static final String PREFIX_OF_CONFIG_OF_PINOT_FS_FACTORY = "pinot.server.storage.factory";
    public static final String PREFIX_OF_CONFIG_OF_PINOT_CRYPTER = "pinot.server.crypter";
    public static final String CONFIG_OF_VALUE_PRUNER_IN_PREDICATE_THRESHOLD =
        "pinot.server.query.executor.pruner.columnvaluesegmentpruner.inpredicate.threshold";
    public static final int DEFAULT_VALUE_PRUNER_IN_PREDICATE_THRESHOLD = 10;

    /**
     * Service token for accessing protected controller APIs.
     * E.g. null (auth disabled), "Basic abcdef..." (basic auth), "Bearer 123def..." (oauth2)
     */
    public static final String CONFIG_OF_AUTH = KEY_OF_AUTH;

    // Configuration to consider the server ServiceStatus as being STARTED if the percent of resources (tables) that
    // are ONLINE for this this server has crossed the threshold percentage of the total number of tables
    // that it is expected to serve.
    public static final String CONFIG_OF_SERVER_MIN_RESOURCE_PERCENT_FOR_START =
        "pinot.server.startup.minResourcePercent";
    public static final double DEFAULT_SERVER_MIN_RESOURCE_PERCENT_FOR_START = 100.0;
    public static final String CONFIG_OF_STARTUP_REALTIME_CONSUMPTION_CATCHUP_WAIT_MS =
        "pinot.server.starter.realtimeConsumptionCatchupWaitMs";
    public static final int DEFAULT_STARTUP_REALTIME_CONSUMPTION_CATCHUP_WAIT_MS = 0;
    public static final String CONFIG_OF_ENABLE_REALTIME_OFFSET_BASED_CONSUMPTION_STATUS_CHECKER =
        "pinot.server.starter.enableRealtimeOffsetBasedConsumptionStatusChecker";
    public static final boolean DEFAULT_ENABLE_REALTIME_OFFSET_BASED_CONSUMPTION_STATUS_CHECKER = false;

    public static final String CONFIG_OF_ENABLE_REALTIME_FRESHNESS_BASED_CONSUMPTION_STATUS_CHECKER =
        "pinot.server.starter.enableRealtimeFreshnessBasedConsumptionStatusChecker";
    public static final boolean DEFAULT_ENABLE_REALTIME_FRESHNESS_BASED_CONSUMPTION_STATUS_CHECKER = false;
    // This configuration is in place to avoid servers getting stuck checking for freshness in
    // cases where they will never be able to reach the freshness threshold or the latest offset.
    // The only current case where we have seen this is low volume streams using read_committed
    // because of transactional publishes where the last message in the stream is an
    // un-consumable kafka control message, and it is impossible to tell if the consumer is stuck
    // or some offsets will never be consumable.
    //
    // When in doubt, do not enable this configuration as it can cause a lagged server to start
    // serving queries.
    public static final String CONFIG_OF_REALTIME_FRESHNESS_IDLE_TIMEOUT_MS =
        "pinot.server.starter.realtimeFreshnessIdleTimeoutMs";
    public static final int DEFAULT_REALTIME_FRESHNESS_IDLE_TIMEOUT_MS = 0;
    public static final String CONFIG_OF_STARTUP_REALTIME_MIN_FRESHNESS_MS =
        "pinot.server.starter.realtimeMinFreshnessMs";
    // Use 10 seconds by default so high volume stream are able to catch up.
    // This is also the default in the case a user misconfigures this by setting to <= 0.
    public static final int DEFAULT_STARTUP_REALTIME_MIN_FRESHNESS_MS = 10000;

    // Config for realtime consumption message rate limit
    public static final String CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT = "pinot.server.consumption.rate.limit";
    // Default to 0.0 (no limit)
    public static final double DEFAULT_SERVER_CONSUMPTION_RATE_LIMIT = 0.0;

    public static final String DEFAULT_READ_MODE = "mmap";
    // Whether to reload consuming segment on scheme update
    public static final boolean DEFAULT_RELOAD_CONSUMING_SEGMENT = true;
    public static final String DEFAULT_INSTANCE_BASE_DIR =
        System.getProperty("java.io.tmpdir") + File.separator + "PinotServer";
    public static final String DEFAULT_INSTANCE_DATA_DIR = DEFAULT_INSTANCE_BASE_DIR + File.separator + "index";
    public static final String DEFAULT_INSTANCE_SEGMENT_TAR_DIR =
        DEFAULT_INSTANCE_BASE_DIR + File.separator + "segmentTar";
    public static final String DEFAULT_DATA_MANAGER_CLASS =
        "org.apache.pinot.server.starter.helix.HelixInstanceDataManager";
    public static final String DEFAULT_QUERY_EXECUTOR_CLASS =
        "org.apache.pinot.core.query.executor.ServerQueryExecutorV1Impl";
    // The order of the pruners matters. Pruning with segment metadata ahead of those using segment data like bloom
    // filters to reduce the required data access.
    public static final List<String> DEFAULT_QUERY_EXECUTOR_PRUNER_CLASS =
        ImmutableList.of("ColumnValueSegmentPruner", "BloomFilterSegmentPruner", "SelectionQuerySegmentPruner");
    public static final String DEFAULT_QUERY_EXECUTOR_PLAN_MAKER_CLASS =
        "org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2";
    public static final long DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS = 15_000L;
    public static final String PREFIX_OF_CONFIG_OF_SEGMENT_FETCHER_FACTORY = "pinot.server.segment.fetcher";

    // Configs for server starter startup/shutdown checks
    // Startup: timeout for the startup checks
    public static final String CONFIG_OF_STARTUP_TIMEOUT_MS = "pinot.server.startup.timeoutMs";
    public static final long DEFAULT_STARTUP_TIMEOUT_MS = 600_000L;
    // Startup: enable service status check before claiming server up
    public static final String CONFIG_OF_STARTUP_ENABLE_SERVICE_STATUS_CHECK =
        "pinot.server.startup.enableServiceStatusCheck";
    public static final boolean DEFAULT_STARTUP_ENABLE_SERVICE_STATUS_CHECK = true;
    // The timeouts above determine how long servers will poll their status before giving up.
    // This configuration determines what we do when we give up. By default, we will mark the
    // server as healthy and start the query server. If this is set to true, we instead throw
    // an exception and exit the server. This is useful if you want to ensure that the server
    // is always fully ready before accepting queries. But note that this can cause the server
    // to never be healthy if there is some reason that it can never reach a GOOD status.
    public static final String CONFIG_OF_EXIT_ON_SERVICE_STATUS_CHECK_FAILURE =
        "pinot.server.startup.exitOnServiceStatusCheckFailure";
    public static final boolean DEFAULT_EXIT_ON_SERVICE_STATUS_CHECK_FAILURE = false;
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

    public static final String DEFAULT_COLUMN_MIN_MAX_VALUE_GENERATOR_MODE = "ALL";

    public static final String PINOT_SERVER_METRICS_PREFIX = "pinot.server.metrics.prefix";

    public static final String SERVER_TLS_PREFIX = "pinot.server.tls";
    public static final String SERVER_NETTYTLS_PREFIX = "pinot.server.nettytls";
    public static final String SERVER_GRPCTLS_PREFIX = "pinot.server.grpctls";
    public static final String SERVER_NETTY_PREFIX = "pinot.server.netty";

    // The complete config key is pinot.server.instance.segment.store.uri
    public static final String CONFIG_OF_SEGMENT_STORE_URI = "segment.store.uri";
    public static final String CONFIG_OF_LOGGER_ROOT_DIR = "pinot.server.logger.root.dir";

    public static class SegmentCompletionProtocol {
      public static final String PREFIX_OF_CONFIG_OF_SEGMENT_UPLOADER = "pinot.server.segment.uploader";

      /**
       * Deprecated. Enable legacy https configs for segment upload.
       * Use server-wide TLS configs instead.
       */
      @Deprecated
      public static final String CONFIG_OF_CONTROLLER_HTTPS_ENABLED = "enabled";

      /**
       * Deprecated. Set the legacy https port for segment upload.
       * Use server-wide TLS configs instead.
       */
      @Deprecated
      public static final String CONFIG_OF_CONTROLLER_HTTPS_PORT = "controller.port";

      public static final String CONFIG_OF_SEGMENT_UPLOAD_REQUEST_TIMEOUT_MS = "upload.request.timeout.ms";

      /**
       * Specify connection scheme to use for controller upload connections. Defaults to "http"
       */
      public static final String CONFIG_OF_PROTOCOL = "protocol";

      /**
       * Service token for accessing protected controller APIs.
       * E.g. null (auth disabled), "Basic abcdef..." (basic auth), "Bearer 123def..." (oauth2)
       */
      public static final String CONFIG_OF_SEGMENT_UPLOADER_AUTH = KEY_OF_AUTH;

      public static final int DEFAULT_SEGMENT_UPLOAD_REQUEST_TIMEOUT_MS = 300_000;
      public static final int DEFAULT_OTHER_REQUESTS_TIMEOUT = 10_000;
    }

    public static final String DEFAULT_METRICS_PREFIX = "pinot.server.";
    public static final String CONFIG_OF_ENABLE_TABLE_LEVEL_METRICS = "pinot.server.enableTableLevelMetrics";
    public static final boolean DEFAULT_ENABLE_TABLE_LEVEL_METRICS = true;
    public static final String CONFIG_OF_ALLOWED_TABLES_FOR_EMITTING_METRICS =
        "pinot.server.allowedTablesForEmittingMetrics";
    public static final String ACCESS_CONTROL_FACTORY_CLASS = "pinot.server.admin.access.control.factory.class";
    public static final String DEFAULT_ACCESS_CONTROL_FACTORY_CLASS =
        "org.apache.pinot.server.access.AllowAllAccessFactory";
    public static final String PREFIX_OF_CONFIG_OF_ACCESS_CONTROL = "pinot.server.admin.access.control";

    public static final String CONFIG_OF_ENABLE_THREAD_CPU_TIME_MEASUREMENT =
        "pinot.server.instance.enableThreadCpuTimeMeasurement";
    public static final String CONFIG_OF_ENABLE_THREAD_ALLOCATED_BYTES_MEASUREMENT =
        "pinot.server.instance.enableThreadAllocatedBytesMeasurement";
    public static final boolean DEFAULT_ENABLE_THREAD_CPU_TIME_MEASUREMENT = false;
    public static final boolean DEFAULT_THREAD_ALLOCATED_BYTES_MEASUREMENT = false;

    public static final String CONFIG_OF_CURRENT_DATA_TABLE_VERSION = "pinot.server.instance.currentDataTableVersion";

    // Environment Provider Configs
    public static final String PREFIX_OF_CONFIG_OF_ENVIRONMENT_PROVIDER_FACTORY =
        "pinot.server.environmentProvider.factory";
    public static final String ENVIRONMENT_PROVIDER_CLASS_NAME = "pinot.server.environmentProvider.className";
  }

  public static class Controller {
    public static final String PREFIX_OF_CONFIG_OF_SEGMENT_FETCHER_FACTORY = "pinot.controller.segment.fetcher";
    public static final String PREFIX_OF_CONFIG_OF_PINOT_FS_FACTORY = "pinot.controller.storage.factory";
    public static final String HOST_HTTP_HEADER = "Pinot-Controller-Host";
    public static final String VERSION_HTTP_HEADER = "Pinot-Controller-Version";
    public static final String SEGMENT_NAME_HTTP_HEADER = "Pinot-Segment-Name";
    public static final String TABLE_NAME_HTTP_HEADER = "Pinot-Table-Name";
    public static final String INGESTION_DESCRIPTOR = "Pinot-Ingestion-Descriptor";
    public static final String PREFIX_OF_CONFIG_OF_PINOT_CRYPTER = "pinot.controller.crypter";

    public static final String CONFIG_OF_CONTROLLER_METRICS_PREFIX = "controller.metrics.prefix";
    // FYI this is incorrect as it generate metrics named without a dot after pinot.controller part,
    // but we keep this default for backward compatibility in case someone relies on this format
    // see Server or Broker class for correct prefix format you should use
    public static final String DEFAULT_METRICS_PREFIX = "pinot.controller.";

    public static final String CONFIG_OF_INSTANCE_ID = "pinot.controller.instance.id";
    public static final String CONFIG_OF_CONTROLLER_QUERY_REWRITER_CLASS_NAMES =
        "pinot.controller.query.rewriter.class.names";
    //Set to true to load all services tagged and compiled with hk2-metadata-generator. Default to False
    public static final String CONTROLLER_SERVICE_AUTO_DISCOVERY = "pinot.controller.service.auto.discovery";
    public static final String CONFIG_OF_LOGGER_ROOT_DIR = "pinot.controller.logger.root.dir";
  }

  public static class Minion {
    public static final String CONFIG_OF_METRICS_PREFIX = "pinot.minion.";
    public static final String CONFIG_OF_MINION_ID = "pinot.minion.instance.id";
    public static final String METADATA_EVENT_OBSERVER_PREFIX = "metadata.event.notifier";

    // Config keys
    public static final String CONFIG_OF_SWAGGER_USE_HTTPS = "pinot.minion.swagger.use.https";
    public static final String CONFIG_OF_METRICS_PREFIX_KEY = "pinot.minion.metrics.prefix";
    @Deprecated
    public static final String DEPRECATED_CONFIG_OF_METRICS_PREFIX_KEY = "metricsPrefix";
    public static final String METRICS_REGISTRY_REGISTRATION_LISTENERS_KEY = "metricsRegistryRegistrationListeners";
    public static final String METRICS_CONFIG_PREFIX = "pinot.minion.metrics";

    // Default settings
    public static final int DEFAULT_HELIX_PORT = 9514;
    public static final String DEFAULT_INSTANCE_BASE_DIR =
        System.getProperty("java.io.tmpdir") + File.separator + "PinotMinion";
    public static final String DEFAULT_INSTANCE_DATA_DIR = DEFAULT_INSTANCE_BASE_DIR + File.separator + "data";

    // Add pinot.minion prefix on those configs to be consistent with configs of controller and server.
    public static final String PREFIX_OF_CONFIG_OF_PINOT_FS_FACTORY = "pinot.minion.storage.factory";
    public static final String PREFIX_OF_CONFIG_OF_SEGMENT_FETCHER_FACTORY = "pinot.minion.segment.fetcher";
    public static final String PREFIX_OF_CONFIG_OF_SEGMENT_UPLOADER = "pinot.minion.segment.uploader";
    public static final String PREFIX_OF_CONFIG_OF_PINOT_CRYPTER = "pinot.minion.crypter";
    @Deprecated
    public static final String DEPRECATED_PREFIX_OF_CONFIG_OF_PINOT_FS_FACTORY = "storage.factory";
    @Deprecated
    public static final String DEPRECATED_PREFIX_OF_CONFIG_OF_SEGMENT_FETCHER_FACTORY = "segment.fetcher";
    @Deprecated
    public static final String DEPRECATED_PREFIX_OF_CONFIG_OF_SEGMENT_UPLOADER = "segment.uploader";
    @Deprecated
    public static final String DEPRECATED_PREFIX_OF_CONFIG_OF_PINOT_CRYPTER = "crypter";

    /**
     * Service token for accessing protected controller APIs.
     * E.g. null (auth disabled), "Basic abcdef..." (basic auth), "Bearer 123def..." (oauth2)
     */
    public static final String CONFIG_TASK_AUTH_NAMESPACE = "task.auth";
    public static final String MINION_TLS_PREFIX = "pinot.minion.tls";
    public static final String CONFIG_OF_MINION_QUERY_REWRITER_CLASS_NAMES = "pinot.minion.query.rewriter.class.names";
    public static final String CONFIG_OF_LOGGER_ROOT_DIR = "pinot.minion.logger.root.dir";
    public static final String CONFIG_OF_EVENT_OBSERVER_CLEANUP_DELAY_IN_SEC =
        "pinot.minion.event.observer.cleanupDelayInSec";
    public static final char TASK_LIST_SEPARATOR = ',';
  }

  public static class ControllerJob {
    /**
     * Controller job ZK props
     */
    public static final String JOB_TYPE = "jobType";
    public static final String TABLE_NAME_WITH_TYPE = "tableName";
    public static final String TENANT_NAME = "tenantName";
    public static final String JOB_ID = "jobId";
    public static final String SUBMISSION_TIME_MS = "submissionTimeMs";
    public static final String MESSAGE_COUNT = "messageCount";

    public static final Integer MAXIMUM_CONTROLLER_JOBS_IN_ZK = 100;
    /**
     * Segment reload job ZK props
     */
    public static final String SEGMENT_RELOAD_JOB_SEGMENT_NAME = "segmentName";
    // Force commit job ZK props
    public static final String CONSUMING_SEGMENTS_FORCE_COMMITTED_LIST = "segmentsForceCommitted";
  }

  // prefix for scheduler related features, e.g. query accountant
  public static final String PINOT_QUERY_SCHEDULER_PREFIX = "pinot.query.scheduler";

  public static class Accounting {
    public static final int ANCHOR_TASK_ID = -1;
    public static final String CONFIG_OF_FACTORY_NAME = "accounting.factory.name";

    public static final String CONFIG_OF_ENABLE_THREAD_CPU_SAMPLING = "accounting.enable.thread.cpu.sampling";
    public static final Boolean DEFAULT_ENABLE_THREAD_CPU_SAMPLING = false;

    public static final String CONFIG_OF_ENABLE_THREAD_MEMORY_SAMPLING = "accounting.enable.thread.memory.sampling";
    public static final Boolean DEFAULT_ENABLE_THREAD_MEMORY_SAMPLING = false;

    public static final String CONFIG_OF_OOM_PROTECTION_KILLING_QUERY = "accounting.oom.enable.killing.query";
    public static final boolean DEFAULT_ENABLE_OOM_PROTECTION_KILLING_QUERY = false;

    public static final String CONFIG_OF_PUBLISHING_JVM_USAGE = "accounting.publishing.jvm.heap.usage";
    public static final boolean DEFAULT_PUBLISHING_JVM_USAGE = false;

    public static final String CONFIG_OF_CPU_TIME_BASED_KILLING_ENABLED = "accounting.cpu.time.based.killing.enabled";
    public static final boolean DEFAULT_CPU_TIME_BASED_KILLING_ENABLED = false;

    public static final String CONFIG_OF_CPU_TIME_BASED_KILLING_THRESHOLD_MS =
        "accounting.cpu.time.based.killing.threshold.ms";
    public static final int DEFAULT_CPU_TIME_BASED_KILLING_THRESHOLD_MS = 30_000;

    public static final String CONFIG_OF_PANIC_LEVEL_HEAP_USAGE_RATIO = "accounting.oom.panic.heap.usage.ratio";
    public static final float DFAULT_PANIC_LEVEL_HEAP_USAGE_RATIO = 0.99f;

    public static final String CONFIG_OF_CRITICAL_LEVEL_HEAP_USAGE_RATIO = "accounting.oom.critical.heap.usage.ratio";
    public static final float DEFAULT_CRITICAL_LEVEL_HEAP_USAGE_RATIO = 0.96f;

    public static final String CONFIG_OF_CRITICAL_LEVEL_HEAP_USAGE_RATIO_DELTA_AFTER_GC =
        "accounting.oom.critical.heap.usage.ratio.delta.after.gc";
    public static final float DEFAULT_CONFIG_OF_CRITICAL_LEVEL_HEAP_USAGE_RATIO_DELTA_AFTER_GC = 0.15f;

    public static final String CONFIG_OF_ALARMING_LEVEL_HEAP_USAGE_RATIO = "accounting.oom.alarming.usage.ratio";
    public static final float DEFAULT_ALARMING_LEVEL_HEAP_USAGE_RATIO = 0.75f;

    public static final String CONFIG_OF_HEAP_USAGE_PUBLISHING_PERIOD_MS = "accounting.heap.usage.publishing.period.ms";
    public static final int DEFAULT_HEAP_USAGE_PUBLISH_PERIOD = 5000;

    public static final String CONFIG_OF_SLEEP_TIME_MS = "accounting.sleep.ms";
    public static final int DEFAULT_SLEEP_TIME_MS = 30;

    public static final String CONFIG_OF_SLEEP_TIME_DENOMINATOR = "accounting.sleep.time.denominator";
    public static final int DEFAULT_SLEEP_TIME_DENOMINATOR = 3;

    public static final String CONFIG_OF_MIN_MEMORY_FOOTPRINT_TO_KILL_RATIO =
        "accounting.min.memory.footprint.to.kill.ratio";
    public static final double DEFAULT_MEMORY_FOOTPRINT_TO_KILL_RATIO = 0.025;

    public static final String CONFIG_OF_GC_BACKOFF_COUNT = "accounting.gc.backoff.count";
    public static final int DEFAULT_GC_BACKOFF_COUNT = 5;

    public static final String CONFIG_OF_INSTANCE_TYPE = "accounting.instance.type";
    public static final InstanceType DEFAULT_CONFIG_OF_INSTANCE_TYPE = InstanceType.SERVER;

    public static final String CONFIG_OF_GC_WAIT_TIME_MS = "accounting.gc.wait.time.ms";
    public static final int DEFAULT_CONFIG_OF_GC_WAIT_TIME_MS = 0;

    public static final String CONFIG_OF_QUERY_KILLED_METRIC_ENABLED = "accounting.query.killed.metric.enabled";
    public static final boolean DEFAULT_QUERY_KILLED_METRIC_ENABLED = false;
  }

  public static class ExecutorService {
    public static final String PINOT_QUERY_RUNNER_NAME_PREFIX = "pqr-";
    public static final String PINOT_QUERY_RUNNER_NAME_FORMAT = PINOT_QUERY_RUNNER_NAME_PREFIX + "%d";
    public static final String PINOT_QUERY_WORKER_NAME_PREFIX = "pqw-";
    public static final String PINOT_QUERY_WORKER_NAME_FORMAT = PINOT_QUERY_WORKER_NAME_PREFIX + "%d";
  }

  public static class Segment {
    public static class Realtime {
      public enum Status {
        IN_PROGRESS, // The segment is still consuming data
        DONE, // The segment has finished consumption and has been committed to the segment store
        UPLOADED; // The segment is uploaded by an external party

        /**
         * Returns {@code true} if the segment is completed (DONE/UPLOADED), {@code false} otherwise.
         */
        public boolean isCompleted() {
          return this != IN_PROGRESS;
        }
      }

      /**
       * During realtime segment completion, the value of this enum decides how  non-winner servers should replace
       * the completed segment.
       */
      public enum CompletionMode {
        // default behavior - if the in memory segment in the non-winner server is equivalent to the committed
        // segment, then build and replace, else download
        DEFAULT, // non-winner servers always download the segment, never build it
        DOWNLOAD
      }

      public static final String STATUS = "segment.realtime.status";
      public static final String START_OFFSET = "segment.realtime.startOffset";
      public static final String END_OFFSET = "segment.realtime.endOffset";
      public static final String NUM_REPLICAS = "segment.realtime.numReplicas";
      public static final String FLUSH_THRESHOLD_SIZE = "segment.flush.threshold.size";
      public static final String FLUSH_THRESHOLD_TIME = "segment.flush.threshold.time";

      // Deprecated, but kept for backward-compatibility of reading old segments' ZK metadata
      @Deprecated
      public static final String DOWNLOAD_URL = "segment.realtime.download.url";
    }

    // Deprecated, but kept for backward-compatibility of reading old segments' ZK metadata
    @Deprecated
    public static class Offline {
      public static final String DOWNLOAD_URL = "segment.offline.download.url";
      public static final String PUSH_TIME = "segment.offline.push.time";
      public static final String REFRESH_TIME = "segment.offline.refresh.time";
    }

    public static final String START_TIME = "segment.start.time";
    public static final String END_TIME = "segment.end.time";
    public static final String RAW_START_TIME = "segment.start.time.raw";
    public static final String RAW_END_TIME = "segment.end.time.raw";
    public static final String TIME_UNIT = "segment.time.unit";
    public static final String INDEX_VERSION = "segment.index.version";
    public static final String TOTAL_DOCS = "segment.total.docs";
    public static final String CRC = "segment.crc";
    public static final String TIER = "segment.tier";
    public static final String CREATION_TIME = "segment.creation.time";
    public static final String PUSH_TIME = "segment.push.time";
    public static final String REFRESH_TIME = "segment.refresh.time";
    public static final String DOWNLOAD_URL = "segment.download.url";
    public static final String CRYPTER_NAME = "segment.crypter";
    public static final String PARTITION_METADATA = "segment.partition.metadata";
    public static final String CUSTOM_MAP = "custom.map";
    public static final String SIZE_IN_BYTES = "segment.size.in.bytes";

    /**
     * This field is used for parallel push protection to lock the segment globally.
     * We put the segment upload start timestamp so that if the previous push failed without unlock the segment, the
     * next upload won't be blocked forever.
     */
    public static final String SEGMENT_UPLOAD_START_TIME = "segment.upload.start.time";

    public static final String SEGMENT_BACKUP_DIR_SUFFIX = ".segment.bak";
    public static final String SEGMENT_TEMP_DIR_SUFFIX = ".segment.tmp";

    public static final String LOCAL_SEGMENT_SCHEME = "file";
    public static final String PEER_SEGMENT_DOWNLOAD_SCHEME = "peer://";
    public static final String METADATA_URI_FOR_PEER_DOWNLOAD = "";

    public static class AssignmentStrategy {
      public static final String BALANCE_NUM_SEGMENT_ASSIGNMENT_STRATEGY = "balanced";
      public static final String REPLICA_GROUP_SEGMENT_ASSIGNMENT_STRATEGY = "replicagroup";
      public static final String DIM_TABLE_SEGMENT_ASSIGNMENT_STRATEGY = "allservers";
    }

    public static class BuiltInVirtualColumn {
      public static final String DOCID = "$docId";
      public static final String HOSTNAME = "$hostName";
      public static final String SEGMENTNAME = "$segmentName";
    }
  }

  public static class Tier {
    public static final String BACKEND_PROP_DATA_DIR = "dataDir";
  }

  public static class Query {
    public static class Request {
      public static class MetadataKeys {
        public static final String REQUEST_ID = "requestId";
        public static final String BROKER_ID = "brokerId";
        public static final String ENABLE_TRACE = "enableTrace";
        public static final String ENABLE_STREAMING = "enableStreaming";
        public static final String PAYLOAD_TYPE = "payloadType";
      }

      public static class PayloadType {
        public static final String SQL = "sql";
        public static final String BROKER_REQUEST = "brokerRequest";
      }
    }

    public static class Response {
      public static class MetadataKeys {
        public static final String RESPONSE_TYPE = "responseType";
      }

      public static class ResponseType {
        // For streaming response, multiple (could be 0 if no data should be returned, or query encounters exception)
        // data responses will be returned, followed by one single metadata response
        public static final String DATA = "data";
        public static final String METADATA = "metadata";
        // For non-streaming response
        public static final String NON_STREAMING = "nonStreaming";
      }

      /**
       * Configuration keys for {@link org.apache.pinot.common.proto.Worker.QueryResponse} extra metadata.
       */
      public static class ServerResponseStatus {
        public static final String STATUS_ERROR = "ERROR";
        public static final String STATUS_OK = "OK";
      }
    }

    public static class OptimizationConstants {
      public static final int DEFAULT_AVG_MV_ENTRIES_DENOMINATOR = 2;
    }

    public static class Range {
      public static final char DELIMITER = '\0';
      public static final char LOWER_EXCLUSIVE = '(';
      public static final char LOWER_INCLUSIVE = '[';
      public static final char UPPER_EXCLUSIVE = ')';
      public static final char UPPER_INCLUSIVE = ']';
      public static final String UNBOUNDED = "*";
      public static final String LOWER_UNBOUNDED = LOWER_EXCLUSIVE + UNBOUNDED + DELIMITER;
      public static final String UPPER_UNBOUNDED = DELIMITER + UNBOUNDED + UPPER_EXCLUSIVE;
    }
  }

  public static class IdealState {
    public static final String HYBRID_TABLE_TIME_BOUNDARY = "HYBRID_TABLE_TIME_BOUNDARY";
  }

  public static class RewriterConstants {
    public static final String PARENT_AGGREGATION_NAME_PREFIX = "parent";
    public static final String CHILD_AGGREGATION_NAME_PREFIX = "child";
    public static final String CHILD_AGGREGATION_SEPERATOR = "@";
    public static final String CHILD_KEY_SEPERATOR = "_";
  }

  /**
   * Configuration for setting up multi-stage query runner, this service could be running on either broker or server.
   */
  public static class MultiStageQueryRunner {
    /**
     * Configuration for mailbox data block size
     */
    public static final String KEY_OF_MAX_INBOUND_QUERY_DATA_BLOCK_SIZE_BYTES = "pinot.query.runner.max.msg.size.bytes";
    public static final int DEFAULT_MAX_INBOUND_QUERY_DATA_BLOCK_SIZE_BYTES = 16 * 1024 * 1024;

    /**
     * Configuration for server port, port that opens and accepts
     * {@link org.apache.pinot.query.runtime.plan.DistributedStagePlan} and start executing query stages.
     */
    public static final String KEY_OF_QUERY_SERVER_PORT = "pinot.query.server.port";
    public static final int DEFAULT_QUERY_SERVER_PORT = 0;

    /**
     * Configuration for mailbox hostname and port, this hostname and port opens streaming channel to receive
     * {@link org.apache.pinot.common.datablock.DataBlock}.
     */
    public static final String KEY_OF_QUERY_RUNNER_HOSTNAME = "pinot.query.runner.hostname";
    public static final String KEY_OF_QUERY_RUNNER_PORT = "pinot.query.runner.port";
    public static final int DEFAULT_QUERY_RUNNER_PORT = 0;

    /**
     * Configuration for join overflow.
     */
    public static final String KEY_OF_MAX_ROWS_IN_JOIN = "pinot.query.join.max.rows";
    public static final String KEY_OF_JOIN_OVERFLOW_MODE = "pinot.query.join.overflow.mode";

    public enum JoinOverFlowMode {
      THROW, BREAK
    }
  }

  public static class NullValuePlaceHolder {
    public static final int INT = 0;
    public static final long LONG = 0L;
    public static final float FLOAT = 0f;
    public static final double DOUBLE = 0d;
    public static final BigDecimal BIG_DECIMAL = BigDecimal.ZERO;
    public static final String STRING = "";
    public static final byte[] BYTES = new byte[0];
    public static final ByteArray INTERNAL_BYTES = new ByteArray(BYTES);
    public static final int[] INT_ARRAY = new int[0];
    public static final long[] LONG_ARRAY = new long[0];
    public static final float[] FLOAT_ARRAY = new float[0];
    public static final double[] DOUBLE_ARRAY = new double[0];
    public static final String[] STRING_ARRAY = new String[0];
    public static final byte[][] BYTES_ARRAY = new byte[0][];
  }
}
