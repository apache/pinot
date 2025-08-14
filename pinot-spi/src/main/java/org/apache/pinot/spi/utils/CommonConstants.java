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

import java.io.File;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.query.QueryThreadContext;


public interface CommonConstants {

  String ENVIRONMENT_IDENTIFIER = "environment";
  String INSTANCE_FAILURE_DOMAIN = "failureDomain";
  String DEFAULT_FAILURE_DOMAIN = "No such domain";

  String PREFIX_OF_SSL_SUBSET = "ssl";
  String HTTP_PROTOCOL = "http";
  String HTTPS_PROTOCOL = "https";

  String KEY_OF_AUTH = "auth";

  String TABLE_NAME = "tableName";

  String UNKNOWN = "unknown";
  String CONFIG_OF_METRICS_FACTORY_CLASS_NAME = "factory.className";
  String CONFIG_OF_BROKER_EVENT_LISTENER_CLASS_NAME = "factory.className";
  String CONFIG_OF_REQUEST_CONTEXT_TRACKED_HEADER_KEYS = "request.context.tracked.header.keys";
  String DEFAULT_METRICS_FACTORY_CLASS_NAME =
      //"org.apache.pinot.plugin.metrics.compound.CompoundPinotMetricsFactory";
      "org.apache.pinot.plugin.metrics.yammer.YammerMetricsFactory";
  //"org.apache.pinot.plugin.metrics.dropwizard.DropwizardMetricsFactory";
  String DEFAULT_BROKER_EVENT_LISTENER_CLASS_NAME =
      "org.apache.pinot.spi.eventlistener.query.NoOpBrokerQueryEventListener";

  String SWAGGER_AUTHORIZATION_KEY = "oauth";
  String SWAGGER_POM_PROPERTIES_PATH = "META-INF/maven/org.webjars/swagger-ui/pom.properties";
  String CONFIG_OF_SWAGGER_RESOURCES_PATH = "META-INF/resources/webjars/swagger-ui/";
  String CONFIG_OF_TIMEZONE = "pinot.timezone";

  String DATABASE = "database";
  String DEFAULT_DATABASE = "default";
  String CONFIG_OF_PINOT_INSECURE_MODE = "pinot.insecure.mode";
  @Deprecated
  String DEFAULT_PINOT_INSECURE_MODE = "false";

  String CONFIG_OF_EXECUTORS_FIXED_NUM_THREADS = "pinot.executors.fixed.default.numThreads";
  String DEFAULT_EXECUTORS_FIXED_NUM_THREADS = "-1";

  String CONFIG_OF_PINOT_TAR_COMPRESSION_CODEC_NAME = "pinot.tar.compression.codec.name";
  String QUERY_WORKLOAD = "queryWorkload";

  interface Lucene {
    String CONFIG_OF_LUCENE_MAX_CLAUSE_COUNT = "pinot.lucene.max.clause.count";
    int DEFAULT_LUCENE_MAX_CLAUSE_COUNT = 1024;
  }
  String JFR = "pinot.jfr";

  String RLS_FILTERS = "rlsFilters";

  /**
   * The state of the consumer for a given segment
   */
  enum ConsumerState {
    CONSUMING, NOT_CONSUMING // In error state
  }

  enum TaskTriggers {
    CRON_TRIGGER, MANUAL_TRIGGER, ADHOC_TRIGGER, UNKNOWN
  }

  interface Table {
    String PUSH_FREQUENCY_HOURLY = "hourly";
    String PUSH_FREQUENCY_DAILY = "daily";
    String PUSH_FREQUENCY_WEEKLY = "weekly";
    String PUSH_FREQUENCY_MONTHLY = "monthly";
  }

  interface Helix {
    String IS_SHUTDOWN_IN_PROGRESS = "shutdownInProgress";
    String QUERIES_DISABLED = "queriesDisabled";
    String QUERY_RATE_LIMIT_DISABLED = "queryRateLimitDisabled";
    String DATABASE_MAX_QUERIES_PER_SECOND = "databaseMaxQueriesPerSecond";
    String APPLICATION_MAX_QUERIES_PER_SECOND = "applicationMaxQueriesPerSecond";

    String INSTANCE_CONNECTED_METRIC_NAME = "helix.connected";

    String PREFIX_OF_CONTROLLER_INSTANCE = "Controller_";
    String PREFIX_OF_BROKER_INSTANCE = "Broker_";
    String PREFIX_OF_SERVER_INSTANCE = "Server_";
    String PREFIX_OF_MINION_INSTANCE = "Minion_";

    int CONTROLLER_INSTANCE_PREFIX_LENGTH = PREFIX_OF_CONTROLLER_INSTANCE.length();
    int BROKER_INSTANCE_PREFIX_LENGTH = PREFIX_OF_BROKER_INSTANCE.length();
    int SERVER_INSTANCE_PREFIX_LENGTH = PREFIX_OF_SERVER_INSTANCE.length();
    int MINION_INSTANCE_PREFIX_LENGTH = PREFIX_OF_MINION_INSTANCE.length();

    String BROKER_RESOURCE_INSTANCE = "brokerResource";
    String LEAD_CONTROLLER_RESOURCE_NAME = "leadControllerResource";

    String LEAD_CONTROLLER_RESOURCE_ENABLED_KEY = "RESOURCE_ENABLED";

    String ENABLE_CASE_INSENSITIVE_KEY = "enable.case.insensitive";
    boolean DEFAULT_ENABLE_CASE_INSENSITIVE = true;

    String DEFAULT_HYPERLOGLOG_LOG2M_KEY = "default.hyperloglog.log2m";
    int DEFAULT_HYPERLOGLOG_LOG2M = 8;
    int DEFAULT_HYPERLOGLOG_PLUS_P = 14;
    int DEFAULT_HYPERLOGLOG_PLUS_SP = 0;

    // 2 to the power of 14, for tradeoffs see datasketches library documentation:
    // https://datasketches.apache.org/docs/Theta/ThetaErrorTable.html
    int DEFAULT_THETA_SKETCH_NOMINAL_ENTRIES = 16384;

    // 2 to the power of 14, for tradeoffs see datasketches library documentation:
    // https://datasketches.apache.org/docs/Theta/ThetaErrorTable.html
    int DEFAULT_TUPLE_SKETCH_LGK = 14;

    int DEFAULT_CPC_SKETCH_LGK = 12;
    int DEFAULT_ULTRALOGLOG_P = 12;

    // K is set to 200, for tradeoffs see datasketches library documentation:
    // https://datasketches.apache.org/docs/KLL/KLLAccuracyAndSize.html#:~:
    int DEFAULT_KLL_SKETCH_K = 200;

    // Whether to rewrite DistinctCount to DistinctCountBitmap
    String ENABLE_DISTINCT_COUNT_BITMAP_OVERRIDE_KEY = "enable.distinct.count.bitmap.override";

    // More information on why these numbers are set can be found in the following doc:
    // https://cwiki.apache.org/confluence/display/PINOT/Controller+Separation+between+Helix+and+Pinot
    int NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE = 24;
    int LEAD_CONTROLLER_RESOURCE_REPLICA_COUNT = 1;
    int MIN_ACTIVE_REPLICAS = 0;

    // Instance tags
    String CONTROLLER_INSTANCE = "controller";
    String UNTAGGED_BROKER_INSTANCE = "broker_untagged";
    String UNTAGGED_SERVER_INSTANCE = "server_untagged";
    String UNTAGGED_MINION_INSTANCE = "minion_untagged";

    interface StateModel {
      interface SegmentStateModel {
        String ONLINE = "ONLINE";
        String OFFLINE = "OFFLINE";
        String ERROR = "ERROR";
        String CONSUMING = "CONSUMING";
      }

      interface DisplaySegmentStatus {
        String BAD = "BAD";
        String GOOD = "GOOD";
        String UPDATING = "UPDATING";
      }

      interface BrokerResourceStateModel {
        String ONLINE = "ONLINE";
        String OFFLINE = "OFFLINE";
        String ERROR = "ERROR";
      }
    }

    interface ZkClient {
      int DEFAULT_CONNECT_TIMEOUT_MS = 60_000;
      int DEFAULT_SESSION_TIMEOUT_MS = 30_000;
      // Retry interval and count for ZK operations where we would rather fail than get an empty (wrong) result back
      int RETRY_INTERVAL_MS = 50;
      int RETRY_COUNT = 2;
      String ZK_CLIENT_CONNECTION_TIMEOUT_MS_CONFIG = "zk.client.connection.timeout.ms";
      String ZK_CLIENT_SESSION_TIMEOUT_MS_CONFIG = "zk.client.session.timeout.ms";
    }

    interface DataSource {
      enum SegmentAssignmentStrategyType {
        RandomAssignmentStrategy,
        BalanceNumSegmentAssignmentStrategy,
        BucketizedSegmentAssignmentStrategy,
        ReplicaGroupSegmentAssignmentStrategy
      }
    }

    interface Instance {
      @Deprecated
      String INSTANCE_ID_KEY = "instanceId";
      String DATA_DIR_KEY = "dataDir";
      String ADMIN_PORT_KEY = "adminPort";
      String ADMIN_HTTPS_PORT_KEY = "adminHttpsPort";
      String GRPC_PORT_KEY = "grpcPort";
      String NETTY_TLS_PORT_KEY = "nettyTlsPort";

      String MULTI_STAGE_QUERY_ENGINE_SERVICE_PORT_KEY = "queryServerPort";
      String MULTI_STAGE_QUERY_ENGINE_MAILBOX_PORT_KEY = "queryMailboxPort";

      String SYSTEM_RESOURCE_INFO_KEY = "SYSTEM_RESOURCE_INFO";
      String PINOT_VERSION_KEY = "pinotVersion";
    }

    String SET_INSTANCE_ID_TO_HOSTNAME_KEY = "pinot.set.instance.id.to.hostname";

    String KEY_OF_SERVER_NETTY_PORT = "pinot.server.netty.port";
    int DEFAULT_SERVER_NETTY_PORT = 8098;
    String KEY_OF_SERVER_NETTYTLS_PORT = Server.SERVER_NETTYTLS_PREFIX + ".port";
    int DEFAULT_SERVER_NETTYTLS_PORT = 8091;
    String KEY_OF_BROKER_QUERY_PORT = "pinot.broker.client.queryPort";
    int DEFAULT_BROKER_QUERY_PORT = 8099;
    String KEY_OF_SERVER_NETTY_HOST = "pinot.server.netty.host";
    String KEY_OF_MINION_HOST = "pinot.minion.host";
    String KEY_OF_MINION_PORT = "pinot.minion.port";

    // NOTE: Helix will disconnect the manager and disable the instance if it detects flapping (too frequent disconnect
    // from ZooKeeper). Setting flapping time window to a small value can avoid this from happening. Helix ignores the
    // non-positive value, so set the default value as 1.
    String CONFIG_OF_CONTROLLER_FLAPPING_TIME_WINDOW_MS = "pinot.controller.flapping.timeWindowMs";
    String CONFIG_OF_BROKER_FLAPPING_TIME_WINDOW_MS = "pinot.broker.flapping.timeWindowMs";
    String CONFIG_OF_SERVER_FLAPPING_TIME_WINDOW_MS = "pinot.server.flapping.timeWindowMs";
    String CONFIG_OF_MINION_FLAPPING_TIME_WINDOW_MS = "pinot.minion.flapping.timeWindowMs";
    String CONFIG_OF_HELIX_INSTANCE_MAX_STATE_TRANSITIONS =
        "pinot.helix.instance.state.maxStateTransitions";
    String DEFAULT_HELIX_INSTANCE_MAX_STATE_TRANSITIONS = "100000";
    String DEFAULT_FLAPPING_TIME_WINDOW_MS = "1";
    String PINOT_SERVICE_ROLE = "pinot.service.role";
    String CONFIG_OF_CLUSTER_NAME = "pinot.cluster.name";
    String CONFIG_OF_ZOOKEEPER_SERVER = "pinot.zk.server";
    @Deprecated(since = "1.5.0", forRemoval = true)
    String CONFIG_OF_ZOOKEEPR_SERVER = "pinot.zk.server";

    String CONFIG_OF_PINOT_CONTROLLER_STARTABLE_CLASS = "pinot.controller.startable.class";
    String CONFIG_OF_PINOT_BROKER_STARTABLE_CLASS = "pinot.broker.startable.class";
    String CONFIG_OF_PINOT_SERVER_STARTABLE_CLASS = "pinot.server.startable.class";
    String CONFIG_OF_PINOT_MINION_STARTABLE_CLASS = "pinot.minion.startable.class";

    String CONFIG_OF_MULTI_STAGE_ENGINE_ENABLED = "pinot.multistage.engine.enabled";
    boolean DEFAULT_MULTI_STAGE_ENGINE_ENABLED = true;

    String CONFIG_OF_MULTI_STAGE_ENGINE_TLS_ENABLED = "pinot.multistage.engine.tls.enabled";
    boolean DEFAULT_MULTI_STAGE_ENGINE_TLS_ENABLED = false;

    // This is a "beta" config and can be changed or even removed in future releases.
    String CONFIG_OF_MULTI_STAGE_ENGINE_MAX_SERVER_QUERY_THREADS =
        "pinot.beta.multistage.engine.max.server.query.threads";
    String DEFAULT_MULTI_STAGE_ENGINE_MAX_SERVER_QUERY_THREADS = "-1";
    String CONFIG_OF_MULTI_STAGE_ENGINE_MAX_SERVER_QUERY_HARDLIMIT_FACTOR =
        "pinot.beta.multistage.engine.max.server.query.threads.hardlimit.factor";
    String DEFAULT_MULTI_STAGE_ENGINE_MAX_SERVER_QUERY_HARDLIMIT_FACTOR = "4";

    // Preprocess throttle configs
    String CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM =
        "pinot.server.max.segment.preprocess.parallelism";
    // Setting to Integer.MAX_VALUE to effectively disable throttling by default
    String DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM = String.valueOf(Integer.MAX_VALUE);
    // Before serving queries is enabled, we should use a higher preprocess parallelism to process segments faster
    String CONFIG_OF_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES =
        "pinot.server.max.segment.preprocess.parallelism.before.serving.queries";
    // Setting the before serving queries to Integer.MAX_VALUE to effectively disable throttling by default
    String DEFAULT_MAX_SEGMENT_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES =
        String.valueOf(Integer.MAX_VALUE);

    // Preprocess throttle config specifically for StarTree index rebuild
    String CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM =
        "pinot.server.max.segment.startree.preprocess.parallelism";
    // Setting to Integer.MAX_VALUE to effectively disable throttling by default
    String DEFAULT_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM = String.valueOf(Integer.MAX_VALUE);
    String CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES =
        "pinot.server.max.segment.startree.preprocess.parallelism.before.serving.queries";
    // Setting the before serving queries to Integer.MAX_VALUE to effectively disable throttling by default
    String DEFAULT_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES =
        String.valueOf(Integer.MAX_VALUE);

    // Preprocess throttle config specifically for StarTree index rebuild
    String CONFIG_OF_MAX_SEGMENT_MULTICOL_TEXT_INDEX_PREPROCESS_PARALLELISM =
        "pinot.server.max.segment.multicol.text.index.preprocess.parallelism";
    // Setting to Integer.MAX_VALUE to effectively disable throttling by default
    String DEFAULT_MAX_SEGMENT_MULTICOL_TEXT_INDEX_PREPROCESS_PARALLELISM =
        String.valueOf(Integer.MAX_VALUE);
    String CONFIG_OF_MAX_SEGMENT_MULTICOL_TEXT_INDEX_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES =
        "pinot.server.max.segment.multicol.text.index.preprocess.parallelism.before.serving.queries";
    // Setting the before serving queries to Integer.MAX_VALUE to effectively disable throttling by default
    String DEFAULT_MAX_SEGMENT_MULTICOL_TEXT_INDEX_PREPROCESS_PARALLELISM_BEFORE_SERVING_QUERIES =
        String.valueOf(Integer.MAX_VALUE);

    // Download throttle config
    String CONFIG_OF_MAX_SEGMENT_DOWNLOAD_PARALLELISM =
        "pinot.server.max.segment.download.parallelism";
    // Setting to Integer.MAX_VALUE to effectively disable throttling by default
    String DEFAULT_MAX_SEGMENT_DOWNLOAD_PARALLELISM = String.valueOf(Integer.MAX_VALUE);
    String CONFIG_OF_MAX_SEGMENT_DOWNLOAD_PARALLELISM_BEFORE_SERVING_QUERIES =
        "pinot.server.max.segment.download.parallelism.before.serving.queries";
    // Setting the before serving queries to Integer.MAX_VALUE to effectively disable throttling by default
    String DEFAULT_MAX_SEGMENT_DOWNLOAD_PARALLELISM_BEFORE_SERVING_QUERIES =
        String.valueOf(Integer.MAX_VALUE);
  }

  interface Broker {
    String ROUTING_TABLE_CONFIG_PREFIX = "pinot.broker.routing.table";
    String ACCESS_CONTROL_CONFIG_PREFIX = "pinot.broker.access.control";
    String METRICS_CONFIG_PREFIX = "pinot.broker.metrics";
    String EVENT_LISTENER_CONFIG_PREFIX = "pinot.broker.event.listener";
    String CONFIG_OF_METRICS_NAME_PREFIX = "pinot.broker.metrics.prefix";
    String DEFAULT_METRICS_NAME_PREFIX = "pinot.broker.";

    String CONFIG_OF_DELAY_SHUTDOWN_TIME_MS = "pinot.broker.delayShutdownTimeMs";
    long DEFAULT_DELAY_SHUTDOWN_TIME_MS = 10_000L;
    String CONFIG_OF_ENABLE_TABLE_LEVEL_METRICS = "pinot.broker.enableTableLevelMetrics";
    boolean DEFAULT_ENABLE_TABLE_LEVEL_METRICS = true;
    String CONFIG_OF_ALLOWED_TABLES_FOR_EMITTING_METRICS =
        "pinot.broker.allowedTablesForEmittingMetrics";

    String CONFIG_OF_BROKER_QUERY_REWRITER_CLASS_NAMES = "pinot.broker.query.rewriter.class.names";
    String CONFIG_OF_BROKER_QUERY_RESPONSE_LIMIT = "pinot.broker.query.response.limit";
    String CONFIG_OF_BROKER_DEFAULT_QUERY_LIMIT = "pinot.broker.default.query.limit";

    int DEFAULT_BROKER_QUERY_RESPONSE_LIMIT = Integer.MAX_VALUE;

    // -1 means no limit; value of 10 aligns limit with PinotQuery's defaults.
    int DEFAULT_BROKER_QUERY_LIMIT = 10;

    String CONFIG_OF_BROKER_QUERY_LOG_LENGTH = "pinot.broker.query.log.length";
    int DEFAULT_BROKER_QUERY_LOG_LENGTH = Integer.MAX_VALUE;
    String CONFIG_OF_BROKER_QUERY_LOG_MAX_RATE_PER_SECOND =
        "pinot.broker.query.log.maxRatePerSecond";
    String CONFIG_OF_BROKER_QUERY_LOG_BEFORE_PROCESSING =
        "pinot.broker.query.log.logBeforeProcessing";
    boolean DEFAULT_BROKER_QUERY_LOG_BEFORE_PROCESSING = true;
    String CONFIG_OF_BROKER_QUERY_ENABLE_NULL_HANDLING = "pinot.broker.query.enable.null.handling";
    String CONFIG_OF_BROKER_ENABLE_QUERY_CANCELLATION = "pinot.broker.enable.query.cancellation";
    double DEFAULT_BROKER_QUERY_LOG_MAX_RATE_PER_SECOND = 10_000d;
    String CONFIG_OF_BROKER_TIMEOUT_MS = "pinot.broker.timeoutMs";
    long DEFAULT_BROKER_TIMEOUT_MS = 10_000L;
    String CONFIG_OF_BROKER_ENABLE_ROW_COLUMN_LEVEL_AUTH =
        "pinot.broker.enable.row.column.level.auth";
    boolean DEFAULT_BROKER_ENABLE_ROW_COLUMN_LEVEL_AUTH = false;
    String CONFIG_OF_EXTRA_PASSIVE_TIMEOUT_MS = "pinot.broker.extraPassiveTimeoutMs";
    long DEFAULT_EXTRA_PASSIVE_TIMEOUT_MS = 100L;
    String CONFIG_OF_BROKER_ID = "pinot.broker.instance.id";
    String CONFIG_OF_BROKER_INSTANCE_TAGS = "pinot.broker.instance.tags";
    String CONFIG_OF_BROKER_HOSTNAME = "pinot.broker.hostname";
    String CONFIG_OF_SWAGGER_USE_HTTPS = "pinot.broker.swagger.use.https";
    // Comma separated list of packages that contains javax service resources.
    String BROKER_RESOURCE_PACKAGES = "broker.restlet.api.resource.packages";
    String DEFAULT_BROKER_RESOURCE_PACKAGES = "org.apache.pinot.broker.api.resources";

    // Configuration to consider the broker ServiceStatus as being STARTED if the percent of resources (tables) that
    // are ONLINE for this broker has crossed the threshold percentage of the total number of tables
    // that it is expected to serve.
    String CONFIG_OF_BROKER_MIN_RESOURCE_PERCENT_FOR_START =
        "pinot.broker.startup.minResourcePercent";
    double DEFAULT_BROKER_MIN_RESOURCE_PERCENT_FOR_START = 100.0;
    String CONFIG_OF_ENABLE_QUERY_LIMIT_OVERRIDE = "pinot.broker.enable.query.limit.override";

    // Config for number of threads to use for Broker reduce-phase.
    String CONFIG_OF_MAX_REDUCE_THREADS_PER_QUERY = "pinot.broker.max.reduce.threads.per.query";
    int DEFAULT_MAX_REDUCE_THREADS_PER_QUERY =
        Math.max(1, Math.min(10, Runtime.getRuntime().availableProcessors() / 2));
    // Same logic as CombineOperatorUtils

    // Config for Jersey ThreadPoolExecutorProvider.
    // By default, Jersey uses the default unbounded thread pool to process queries.
    // By enabling it, BrokerManagedAsyncExecutorProvider will be used to create a bounded thread pool.
    String CONFIG_OF_ENABLE_BOUNDED_JERSEY_THREADPOOL_EXECUTOR =
        "pinot.broker.enable.bounded.jersey.threadpool.executor";
    boolean DEFAULT_ENABLE_BOUNDED_JERSEY_THREADPOOL_EXECUTOR = false;
    // Default capacities for the bounded thread pool
    String CONFIG_OF_JERSEY_THREADPOOL_EXECUTOR_MAX_POOL_SIZE =
        "pinot.broker.jersey.threadpool.executor.max.pool.size";
    int DEFAULT_JERSEY_THREADPOOL_EXECUTOR_MAX_POOL_SIZE =
        Runtime.getRuntime().availableProcessors() * 2;
    String CONFIG_OF_JERSEY_THREADPOOL_EXECUTOR_CORE_POOL_SIZE =
        "pinot.broker.jersey.threadpool.executor.core.pool.size";
    int DEFAULT_JERSEY_THREADPOOL_EXECUTOR_CORE_POOL_SIZE =
        Runtime.getRuntime().availableProcessors() * 2;
    String CONFIG_OF_JERSEY_THREADPOOL_EXECUTOR_QUEUE_SIZE =
        "pinot.broker.jersey.threadpool.executor.queue.size";
    int DEFAULT_JERSEY_THREADPOOL_EXECUTOR_QUEUE_SIZE = Integer.MAX_VALUE;

    // Configs for broker reduce on group-by queries (only apply to SSE)
    String CONFIG_OF_BROKER_GROUPBY_TRIM_THRESHOLD = "pinot.broker.groupby.trim.threshold";
    int DEFAULT_BROKER_GROUPBY_TRIM_THRESHOLD = 1_000_000;
    String CONFIG_OF_BROKER_MIN_GROUP_TRIM_SIZE = "pinot.broker.min.group.trim.size";
    int DEFAULT_BROKER_MIN_GROUP_TRIM_SIZE = 5000;
    String CONFIG_OF_BROKER_MIN_INITIAL_INDEXED_TABLE_CAPACITY =
        "pinot.broker.min.init.indexed.table.capacity";
    int DEFAULT_BROKER_MIN_INITIAL_INDEXED_TABLE_CAPACITY = 128;

    // Config for enabling group trim for MSE group-by queries. When group trim is enabled, there are 3 levels of
    // trimming: segment level (shared with SSE, disabled by default), leaf stage level (shared with SSE, enabled by
    // default), intermediate stage level (enabled by default). The group trim behavior for each level is configured on
    // the server side.
    String CONFIG_OF_MSE_ENABLE_GROUP_TRIM = "pinot.broker.mse.enable.group.trim";
    boolean DEFAULT_MSE_ENABLE_GROUP_TRIM = false;

    String CONFIG_OF_MSE_MAX_SERVER_QUERY_THREADS = "pinot.broker.mse.max.server.query.threads";
    int DEFAULT_MSE_MAX_SERVER_QUERY_THREADS = -1;
    String CONFIG_OF_MSE_MAX_SERVER_QUERY_THREADS_EXCEED_STRATEGY =
        "pinot.broker.mse.max.server.query.threads.exceed.strategy";
    String DEFAULT_MSE_MAX_SERVER_QUERY_THREADS_EXCEED_STRATEGY = "WAIT";

    // Configure the request handler type used by broker to handler inbound query request.
    // NOTE: the request handler type refers to the communication between Broker and Server.
    String BROKER_REQUEST_HANDLER_TYPE = "pinot.broker.request.handler.type";
    String NETTY_BROKER_REQUEST_HANDLER_TYPE = "netty";
    String GRPC_BROKER_REQUEST_HANDLER_TYPE = "grpc";
    String MULTI_STAGE_BROKER_REQUEST_HANDLER_TYPE = "multistage";
    String DEFAULT_BROKER_REQUEST_HANDLER_TYPE = NETTY_BROKER_REQUEST_HANDLER_TYPE;

    String BROKER_TLS_PREFIX = "pinot.broker.tls";
    String BROKER_NETTY_PREFIX = "pinot.broker.netty";
    String BROKER_NETTYTLS_ENABLED = "pinot.broker.nettytls.enabled";
    //Set to true to load all services tagged and compiled with hk2-metadata-generator. Default to False
    String BROKER_SERVICE_AUTO_DISCOVERY = "pinot.broker.service.auto.discovery";

    String DISABLE_GROOVY = "pinot.broker.disable.query.groovy";
    boolean DEFAULT_DISABLE_GROOVY = true;
    // Rewrite potential expensive functions to their approximation counterparts
    // - DISTINCT_COUNT -> DISTINCT_COUNT_SMART_HLL
    // - PERCENTILE -> PERCENTILE_SMART_TDIGEST
    String USE_APPROXIMATE_FUNCTION = "pinot.broker.use.approximate.function";

    String CONTROLLER_URL = "pinot.broker.controller.url";

    String CONFIG_OF_BROKER_REQUEST_CLIENT_IP_LOGGING = "pinot.broker.request.client.ip.logging";

    // TODO: Support populating clientIp for GrpcRequestIdentity.
    boolean DEFAULT_BROKER_REQUEST_CLIENT_IP_LOGGING = false;

    String CONFIG_OF_LOGGER_ROOT_DIR = "pinot.broker.logger.root.dir";
    String CONFIG_OF_SWAGGER_BROKER_ENABLED = "pinot.broker.swagger.enabled";
    boolean DEFAULT_SWAGGER_BROKER_ENABLED = true;
    String CONFIG_OF_ENABLE_THREAD_CPU_TIME_MEASUREMENT =
        "pinot.broker.instance.enableThreadCpuTimeMeasurement";
    String CONFIG_OF_ENABLE_THREAD_ALLOCATED_BYTES_MEASUREMENT =
        "pinot.broker.instance.enableThreadAllocatedBytesMeasurement";
    boolean DEFAULT_ENABLE_THREAD_CPU_TIME_MEASUREMENT = false;
    boolean DEFAULT_THREAD_ALLOCATED_BYTES_MEASUREMENT = false;
    String CONFIG_OF_BROKER_RESULT_REWRITER_CLASS_NAMES =
        "pinot.broker.result.rewriter.class.names";

    String CONFIG_OF_ENABLE_PARTITION_METADATA_MANAGER =
        "pinot.broker.enable.partition.metadata.manager";
    boolean DEFAULT_ENABLE_PARTITION_METADATA_MANAGER = true;
    // Whether to infer partition hint by default or not.
    // This value can always be overridden by INFER_PARTITION_HINT query option
    String CONFIG_OF_INFER_PARTITION_HINT = "pinot.broker.multistage.infer.partition.hint";
    boolean DEFAULT_INFER_PARTITION_HINT = false;

    /**
     * Whether to use spools in multistage query engine by default.
     * This value can always be overridden by {@link Request.QueryOptionKey#USE_SPOOLS} query option
     */
    String CONFIG_OF_SPOOLS = "pinot.broker.multistage.spools";
    boolean DEFAULT_OF_SPOOLS = false;

    /// Whether to only use servers for leaf stages as the workers for the intermediate stages.
    /// This value can always be overridden by [Request.QueryOptionKey#USE_LEAF_SERVER_FOR_INTERMEDIATE_STAGE].
    String CONFIG_OF_USE_LEAF_SERVER_FOR_INTERMEDIATE_STAGE =
        "pinot.broker.mse.use.leaf.server.for.intermediate.stage";
    boolean DEFAULT_USE_LEAF_SERVER_FOR_INTERMEDIATE_STAGE = false;

    String CONFIG_OF_USE_FIXED_REPLICA = "pinot.broker.use.fixed.replica";
    boolean DEFAULT_USE_FIXED_REPLICA = false;

    // Broker config indicating the maximum serialized response size across all servers for a query. This value is
    // equally divided across all servers processing the query.
    // The value can be in human readable format (e.g. '200K', '200KB', '0.2MB') or in raw bytes (e.g. '200000').
    String CONFIG_OF_MAX_QUERY_RESPONSE_SIZE_BYTES = "pinot.broker.max.query.response.size.bytes";

    // Broker config indicating the maximum length of the serialized response per server for a query.
    // If both "server.response.size" and "query.response.size" are set, then the "server.response.size" takes
    // precedence over "query.response.size" (i.e., "query.response.size" will be ignored).
    String CONFIG_OF_MAX_SERVER_RESPONSE_SIZE_BYTES = "pinot.broker.max.server.response.size.bytes";

    String CONFIG_OF_NEW_SEGMENT_EXPIRATION_SECONDS = "pinot.broker.new.segment.expiration.seconds";
    long DEFAULT_VALUE_OF_NEW_SEGMENT_EXPIRATION_SECONDS = TimeUnit.MINUTES.toSeconds(5);

    // If this config is set to true, the broker will check every query executed using the v1 query engine and attempt
    // to determine whether the query could have successfully been run on the v2 / multi-stage query engine. If not,
    // a counter metric will be incremented - if this counter remains 0 during regular query workload execution, it
    // signals that users can potentially migrate their query workload to the multistage query engine.
    String CONFIG_OF_BROKER_ENABLE_MULTISTAGE_MIGRATION_METRIC =
        "pinot.broker.enable.multistage.migration.metric";
    boolean DEFAULT_ENABLE_MULTISTAGE_MIGRATION_METRIC = false;
    String CONFIG_OF_BROKER_ENABLE_DYNAMIC_FILTERING_SEMI_JOIN =
        "pinot.broker.enable.dynamic.filtering.semijoin";
    boolean DEFAULT_ENABLE_DYNAMIC_FILTERING_SEMI_JOIN = true;

    /**
     * Whether to use physical optimizer by default.
     * This value can always be overridden by {@link Request.QueryOptionKey#USE_PHYSICAL_OPTIMIZER} query option
     */
    String CONFIG_OF_USE_PHYSICAL_OPTIMIZER = "pinot.broker.multistage.use.physical.optimizer";
    boolean DEFAULT_USE_PHYSICAL_OPTIMIZER = false;

    /**
     * Whether to use lite mode by default.
     * This value can always be overridden by {@link Request.QueryOptionKey#USE_LITE_MODE} query option
     */
    String CONFIG_OF_USE_LITE_MODE = "pinot.broker.multistage.use.lite.mode";
    boolean DEFAULT_USE_LITE_MODE = false;

    /**
     * Whether to run in broker by default.
     * This value can always be overridden by {@link Request.QueryOptionKey#RUN_IN_BROKER} query option
     */
    String CONFIG_OF_RUN_IN_BROKER = "pinot.broker.multistage.run.in.broker";
    boolean DEFAULT_RUN_IN_BROKER = true;

    /**
     * Whether to use broker pruning by default.
     * This value can always be overridden by {@link Request.QueryOptionKey#USE_BROKER_PRUNING} query option
     */
    String CONFIG_OF_USE_BROKER_PRUNING = "pinot.broker.multistage.use.broker.pruning";
    boolean DEFAULT_USE_BROKER_PRUNING = true;

    /**
     * Default server stage limit for lite mode queries.
     * This value can always be overridden by {@link Request.QueryOptionKey#LITE_MODE_SERVER_STAGE_LIMIT} query option
     */
    String CONFIG_OF_LITE_MODE_LEAF_STAGE_LIMIT =
        "pinot.broker.multistage.lite.mode.leaf.stage.limit";
    int DEFAULT_LITE_MODE_LEAF_STAGE_LIMIT = 100_000;

    // Config for default hash function used in KeySelector for data shuffling
    String CONFIG_OF_BROKER_DEFAULT_HASH_FUNCTION = "pinot.broker.multistage.default.hash.function";
    String DEFAULT_BROKER_DEFAULT_HASH_FUNCTION = "absHashCode";

    // When the server instance's pool field is null or the pool contains multi distinguished group value, the broker
    // would set the pool to -1 in the routing table for that server.
    int FALLBACK_POOL_ID = -1;
    // keep the variable to pass the compability test
    @Deprecated
    int FALLBACK_REPLICA_GROUP_ID = -1;

    interface Request {
      String SQL = "sql";
      String SQL_V1 = "sqlV1";
      String SQL_V2 = "sqlV2";
      String TRACE = "trace";
      String QUERY_OPTIONS = "queryOptions";
      String LANGUAGE = "language";
      String QUERY = "query";

      interface QueryOptionKey {
        String TIMEOUT_MS = "timeoutMs";
        String EXTRA_PASSIVE_TIMEOUT_MS = "extraPassiveTimeoutMs";
        String SKIP_UPSERT = "skipUpsert";
        String SKIP_UPSERT_VIEW = "skipUpsertView";
        String UPSERT_VIEW_FRESHNESS_MS = "upsertViewFreshnessMs";
        String USE_STAR_TREE = "useStarTree";
        String SCAN_STAR_TREE_NODES = "scanStarTreeNodes";
        String ROUTING_OPTIONS = "routingOptions";
        String USE_SCAN_REORDER_OPTIMIZATION = "useScanReorderOpt";
        String MAX_EXECUTION_THREADS = "maxExecutionThreads";

        // For group-by queries with order-by clause, the tail groups are trimmed off to reduce the memory footprint. To
        // ensure the accuracy of the result, {@code max(limit * 5, minTrimSize)} groups are retained. When
        // {@code minTrimSize} is non-positive, trim is disabled.
        //
        // Caution:
        // Setting trim size to non-positive value (disable trim) or large value gives more accurate result, but can
        // potentially cause higher memory pressure.
        //
        // Trim can be applied in the following stages:
        // - Segment level: after getting the segment level results, before merging them into server level results.
        // - Server level: while merging the segment level results into server level results.
        // - Broker level: while merging the server level results into broker level results. (SSE only)
        // - MSE intermediate stage (MSE only)

        String MIN_SEGMENT_GROUP_TRIM_SIZE = "minSegmentGroupTrimSize";
        String MIN_SERVER_GROUP_TRIM_SIZE = "minServerGroupTrimSize";
        String MIN_BROKER_GROUP_TRIM_SIZE = "minBrokerGroupTrimSize";
        String MSE_MIN_GROUP_TRIM_SIZE = "mseMinGroupTrimSize";

        /**
         * This will help in getting accurate and correct result for queries
         * with group by and limit but  without order by
         */
        String ACCURATE_GROUP_BY_WITHOUT_ORDER_BY = "accurateGroupByWithoutOrderBy";

        /** Number of threads used in the final reduce.
         * This is useful for expensive aggregation functions. E.g. Funnel queries are considered as expensive
         * aggregation functions. */
        String NUM_THREADS_EXTRACT_FINAL_RESULT = "numThreadsExtractFinalResult";

        /** Number of threads used in the final reduce at broker level. */
        String CHUNK_SIZE_EXTRACT_FINAL_RESULT = "chunkSizeExtractFinalResult";

        String NUM_REPLICA_GROUPS_TO_QUERY = "numReplicaGroupsToQuery";

        @Deprecated
        String ORDERED_PREFERRED_REPLICAS = "orderedPreferredReplicas";
        String ORDERED_PREFERRED_POOLS = "orderedPreferredPools";
        String USE_FIXED_REPLICA = "useFixedReplica";
        String EXPLAIN_PLAN_VERBOSE = "explainPlanVerbose";
        String USE_MULTISTAGE_ENGINE = "useMultistageEngine";
        String INFER_PARTITION_HINT = "inferPartitionHint";
        String ENABLE_NULL_HANDLING = "enableNullHandling";
        String APPLICATION_NAME = "applicationName";
        String USE_SPOOLS = "useSpools";
        String USE_PHYSICAL_OPTIMIZER = "usePhysicalOptimizer";
        /**
         * If set, changes the explain behavior in multi-stage engine.
         *
         * {@code true} means to ask servers for the physical plan while false means to just use logical plan.
         *
         * Use false in order to mimic behavior of Pinot 1.2.0 and previous.
         */
        String EXPLAIN_ASKING_SERVERS = "explainAskingServers";

        // Can be applied to aggregation and group-by queries to ask servers to directly return final results instead of
        // intermediate results for aggregations.
        String SERVER_RETURN_FINAL_RESULT = "serverReturnFinalResult";
        // Can be applied to group-by queries to ask servers to directly return final results instead of intermediate
        // results for aggregations. Different from SERVER_RETURN_FINAL_RESULT, this option should be used when the
        // group key is not server partitioned, but the aggregated values are server partitioned. When this option is
        // used, server will return final results, but won't directly trim the result to the query limit.
        String SERVER_RETURN_FINAL_RESULT_KEY_UNPARTITIONED =
            "serverReturnFinalResultKeyUnpartitioned";

        // Reorder scan based predicates based on cardinality and number of selected values
        String AND_SCAN_REORDERING = "AndScanReordering";
        String SKIP_INDEXES = "skipIndexes";

        // Query option key used to skip a given set of rules
        String SKIP_PLANNER_RULES = "skipPlannerRules";

        // Query option key used to enable a given set of defaultly disabled rules
        String USE_PLANNER_RULES = "usePlannerRules";

        String ORDER_BY_ALGORITHM = "orderByAlgorithm";

        String MULTI_STAGE_LEAF_LIMIT = "multiStageLeafLimit";

        // TODO: Apply this to SSE as well
        /** Throw an exception on reaching num_groups_limit instead of just setting a flag. */
        String ERROR_ON_NUM_GROUPS_LIMIT = "errorOnNumGroupsLimit";

        String NUM_GROUPS_LIMIT = "numGroupsLimit";
        // Not actually accepted as Query Option but faked as one during MSE
        String NUM_GROUPS_WARNING_LIMIT = "numGroupsWarningLimit";
        String MAX_INITIAL_RESULT_HOLDER_CAPACITY = "maxInitialResultHolderCapacity";
        String MIN_INITIAL_INDEXED_TABLE_CAPACITY = "minInitialIndexedTableCapacity";
        String MSE_MAX_INITIAL_RESULT_HOLDER_CAPACITY = "mseMaxInitialResultHolderCapacity";
        String GROUP_TRIM_THRESHOLD = "groupTrimThreshold";
        String STAGE_PARALLELISM = "stageParallelism";

        String IN_PREDICATE_PRE_SORTED = "inPredicatePreSorted";
        String IN_PREDICATE_LOOKUP_ALGORITHM = "inPredicateLookupAlgorithm";

        String DROP_RESULTS = "dropResults";

        // Maximum number of pending results blocks allowed in the streaming operator
        String MAX_STREAMING_PENDING_BLOCKS = "maxStreamingPendingBlocks";

        // Handle JOIN Overflow
        String MAX_ROWS_IN_JOIN = "maxRowsInJoin";
        String JOIN_OVERFLOW_MODE = "joinOverflowMode";

        // Handle WINDOW Overflow
        String MAX_ROWS_IN_WINDOW = "maxRowsInWindow";
        String WINDOW_OVERFLOW_MODE = "windowOverflowMode";

        // Indicates the maximum length of the serialized response per server for a query.
        String MAX_SERVER_RESPONSE_SIZE_BYTES = "maxServerResponseSizeBytes";

        // Indicates the maximum length of serialized response across all servers for a query. This value is equally
        // divided across all servers processing the query.
        String MAX_QUERY_RESPONSE_SIZE_BYTES = "maxQueryResponseSizeBytes";

        // If query submission causes an exception, still continue to submit the query to other servers
        String SKIP_UNAVAILABLE_SERVERS = "skipUnavailableServers";

        // Indicates that a query belongs to a secondary workload when using the BinaryWorkloadScheduler. The
        // BinaryWorkloadScheduler divides queries into two workloads, primary and secondary. Primary workloads are
        // executed in an  Unbounded FCFS fashion. However, secondary workloads are executed in a constrainted FCFS
        // fashion with limited compute.des queries into two workloads, primary and secondary. Primary workloads are
        // executed in an  Unbounded FCFS fashion. However, secondary workloads are executed in a constrainted FCFS
        // fashion with limited compute.
        String IS_SECONDARY_WORKLOAD = "isSecondaryWorkload";

        // For group by queries with only filtered aggregations (and no non-filtered aggregations), the default behavior
        // is to compute all groups over the rows matching the main query filter. This ensures SQL compliant results,
        // since empty groups are also expected to be returned in such queries. However, this could be quite inefficient
        // if the main query does not have a filter (since a scan would be required to compute all groups). In case
        // users are okay with skipping empty groups - i.e., only the groups matching at least one aggregation filter
        // will be returned - this query option can be set. This is useful for performance, since indexes can be used
        // for the aggregation filters and a full scan can be avoided.
        String FILTERED_AGGREGATIONS_SKIP_EMPTY_GROUPS = "filteredAggregationsSkipEmptyGroups";

        // When set to true, the max initial result holder capacity will be optimized based on the query. Rather than
        // using the default value. This is best-effort for now and returns the default value if the optimization is not
        // possible.
        String OPTIMIZE_MAX_INITIAL_RESULT_HOLDER_CAPACITY =
            "optimizeMaxInitialResultHolderCapacity";

        // Set to true if a cursor should be returned instead of the complete result set
        String GET_CURSOR = "getCursor";
        // Number of rows that the cursor should contain
        String CURSOR_NUM_ROWS = "cursorNumRows";

        // Custom Query ID provided by the client
        String CLIENT_QUERY_ID = "clientQueryId";

        // Use MSE compiler when trying to fill a response with no schema metadata
        // (overrides the "pinot.broker.use.mse.to.fill.empty.response.schema" broker conf)
        String USE_MSE_TO_FILL_EMPTY_RESPONSE_SCHEMA = "useMSEToFillEmptyResponseSchema";

        // Used by the MSE Engine when auto-inferring data partitioning. Realtime streams can often incorrectly assign
        // records to stream partitions, which can make a segment have multiple partitions. The scale of this is
        // usually low, and this query option allows the MSE Optimizer to infer the partition of a segment based on its
        // name, when that segment has multiple partitions in its columnPartitionMap.
        @Deprecated
        String INFER_INVALID_SEGMENT_PARTITION = "inferInvalidSegmentPartition";
        // For realtime tables, this infers the segment partition for all segments. The partition column, function,
        // and number of partitions still rely on the Table's segmentPartitionConfig. This is useful if you have
        // scenarios where the stream doesn't guarantee 100% accuracy for stream partition assignment. In such
        // scenarios, if you don't have upsert compaction enabled, inferInvalidSegmentPartition will suffice. But when
        // you have compaction enabled, it's possible that after compaction you are only left with invalid partition
        // records, which can change the partition of a segment from something like [1, 3, 5] to [5], for a segment
        // that was supposed to be in partition-1.
        String INFER_REALTIME_SEGMENT_PARTITION = "inferRealtimeSegmentPartition";
        String USE_LITE_MODE = "useLiteMode";
        // Server stage limit for lite mode queries.
        String LITE_MODE_SERVER_STAGE_LIMIT = "liteModeServerStageLimit";
        // Used by the MSE Engine to determine whether to use the broker pruning logic. Only supported by the
        // new MSE query optimizer.
        // TODO(mse-physical): Consider removing this query option and making this the default, since there's already
        //   a table config to enable broker pruning (it is disabled by default).
        String USE_BROKER_PRUNING = "useBrokerPruning";
        // When lite mode is enabled, if this flag is set, we will run all the non-leaf stage operators within the
        // broker itself. That way, the MSE queries will model the scatter gather pattern used by the V1 Engine.
        String RUN_IN_BROKER = "runInBroker";

        /// For MSE queries, when this option is set to true, only use servers for leaf stages as the workers for the
        /// intermediate stages. This is useful to control the fanout of the query and reduce data shuffling.
        String USE_LEAF_SERVER_FOR_INTERMEDIATE_STAGE = "useLeafServerForIntermediateStage";

        // Option denoting the workloadName to which the query belongs. This is used to enforce resource budgets for
        // each workload if "Query Workload Isolation" feature enabled.
        String WORKLOAD_NAME = "workloadName";
      }

      interface QueryOptionValue {
        int DEFAULT_MAX_STREAMING_PENDING_BLOCKS = 100;
      }
    }

    /**
     * Calcite and Pinot rule names / descriptions
     * used for enable and disabling of rules, this will be iterated through in PlannerContext
     * to check if rule is disabled.
     */
    interface PlannerRuleNames {
      String FILTER_INTO_JOIN = "FilterIntoJoin";
      String FILTER_AGGREGATE_TRANSPOSE = "FilterAggregateTranspose";
      String FILTER_SET_OP_TRANSPOSE = "FilterSetOpTranspose";
      String PROJECT_JOIN_TRANSPOSE = "ProjectJoinTranspose";
      String PROJECT_SET_OP_TRANSPOSE = "ProjectSetOpTranspose";
      String FILTER_PROJECT_TRANSPOSE = "FilterProjectTranspose";
      String JOIN_CONDITION_PUSH = "JoinConditionPush";
      String JOIN_PUSH_TRANSITIVE_PREDICATES = "JoinPushTransitivePredicates";
      String PROJECT_REMOVE = "ProjectRemove";
      String PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW = "ProjectToLogicalProjectAndWindow";
      String PROJECT_WINDOW_TRANSPOSE = "ProjectWindowTranspose";
      String EVALUATE_LITERAL_PROJECT = "EvaluateProjectLiteral";
      String EVALUATE_LITERAL_FILTER = "EvaluateFilterLiteral";
      String JOIN_PUSH_EXPRESSIONS = "JoinPushExpressions";
      String PROJECT_TO_SEMI_JOIN = "ProjectToSemiJoin";
      String SEMI_JOIN_DISTINCT_PROJECT = "SemiJoinDistinctProject";
      String UNION_TO_DISTINCT = "UnionToDistinct";
      String AGGREGATE_REMOVE = "AggregateRemove";
      String AGGREGATE_JOIN_TRANSPOSE = "AggregateJoinTranspose";
      String AGGREGATE_UNION_AGGREGATE = "AggregateUnionAggregate";
      String AGGREGATE_REDUCE_FUNCTIONS = "AggregateReduceFunctions";
      String AGGREGATE_CASE_TO_FILTER = "AggregateCaseToFilter";
      String PROJECT_FILTER_TRANSPOSE = "ProjectFilterTranspose";
      String PROJECT_MERGE = "ProjectMerge";
      String AGGREGATE_PROJECT_MERGE = "AggregateProjectMerge";
      String FILTER_MERGE = "FilterMerge";
      String SORT_REMOVE = "SortRemove";
      String SORT_JOIN_TRANSPOSE = "SortJoinTranspose";
      String SORT_JOIN_COPY = "SortJoinCopy";
      String AGGREGATE_JOIN_TRANSPOSE_EXTENDED = "AggregateJoinTransposeExtended";
      String PRUNE_EMPTY_AGGREGATE = "PruneEmptyAggregate";
      String PRUNE_EMPTY_FILTER = "PruneEmptyFilter";
      String PRUNE_EMPTY_PROJECT = "PruneEmptyProject";
      String PRUNE_EMPTY_SORT = "PruneEmptySort";
      String PRUNE_EMPTY_UNION = "PruneEmptyUnion";
      String PRUNE_EMPTY_CORRELATE_LEFT = "PruneEmptyCorrelateLeft";
      String PRUNE_EMPTY_CORRELATE_RIGHT = "PruneEmptyCorrelateRight";
      String PRUNE_EMPTY_JOIN_LEFT = "PruneEmptyJoinLeft";
      String PRUNE_EMPTY_JOIN_RIGHT = "PruneEmptyJoinRight";
      String JOIN_TO_ENRICHED_JOIN = "JoinToEnrichedJoin";
    }

    /**
     * Set of planner rules that will be disabled by default
     * and could be enabled by setting
     * {@link CommonConstants.Broker.Request.QueryOptionKey#USE_PLANNER_RULES}.
     *
     * If a rule is enabled and disabled at the same time,
     * it will be disabled
     */
    Set<String> DEFAULT_DISABLED_RULES = Set.of(
        PlannerRuleNames.AGGREGATE_JOIN_TRANSPOSE_EXTENDED,
        PlannerRuleNames.SORT_JOIN_TRANSPOSE,
        PlannerRuleNames.SORT_JOIN_COPY,
        PlannerRuleNames.AGGREGATE_UNION_AGGREGATE,
        PlannerRuleNames.JOIN_TO_ENRICHED_JOIN
    );

    interface FailureDetector {
      enum Type {
        // Do not detect any failure
        NO_OP,

        // Detect connection failures
        CONNECTION,

        // Use the custom failure detector of the configured class name
        CUSTOM
      }

      String CONFIG_OF_TYPE = "pinot.broker.failure.detector.type";
      String DEFAULT_TYPE = Type.NO_OP.name();
      String CONFIG_OF_CLASS_NAME = "pinot.broker.failure.detector.class";

      // Exponential backoff delay of retrying an unhealthy server when a failure is detected
      String CONFIG_OF_RETRY_INITIAL_DELAY_MS =
          "pinot.broker.failure.detector.retry.initial.delay.ms";
      long DEFAULT_RETRY_INITIAL_DELAY_MS = 5_000L;
      String CONFIG_OF_RETRY_DELAY_FACTOR = "pinot.broker.failure.detector.retry.delay.factor";
      double DEFAULT_RETRY_DELAY_FACTOR = 2.0;
      String CONFIG_OF_MAX_RETRIES = "pinot.broker.failure.detector.max.retries";
      int DEFAULT_MAX_RETRIES = 10;
    }

    // Configs related to AdaptiveServerSelection.
    interface AdaptiveServerSelector {
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

      enum Type {
        NO_OP,

        NUM_INFLIGHT_REQ,

        LATENCY,

        HYBRID
      }

      String CONFIG_PREFIX = "pinot.broker.adaptive.server.selector";

      // Determines the type of AdaptiveServerSelector to use.
      String CONFIG_OF_TYPE = CONFIG_PREFIX + ".type";
      String DEFAULT_TYPE = Type.NO_OP.name();

      // Determines whether stats collection is enabled. This can be enabled independent of CONFIG_OF_TYPE. This is
      // so that users have an option to just enable stats collection and analyze them before deciding and enabling
      // adaptive server selection.
      String CONFIG_OF_ENABLE_STATS_COLLECTION = CONFIG_PREFIX + ".enable.stats.collection";
      boolean DEFAULT_ENABLE_STATS_COLLECTION = false;

      // Parameters to tune exponential moving average.

      // The weightage to be given for a new incoming value. For example, alpha=0.30 will give 30% weightage to the
      // new value and 70% weightage to the existing value in the Exponential Weighted Moving Average calculation.
      String CONFIG_OF_EWMA_ALPHA = CONFIG_PREFIX + ".ewma.alpha";
      double DEFAULT_EWMA_ALPHA = 0.666;

      // If the EMA average has not been updated during a specified time window (defined by this property), the
      // EMA average value is automatically decayed by an incoming value of zero. This is required to bring a server
      // back to healthy state gracefully after it has experienced some form of slowness.
      String CONFIG_OF_AUTODECAY_WINDOW_MS = CONFIG_PREFIX + ".autodecay.window.ms";
      long DEFAULT_AUTODECAY_WINDOW_MS = 10 * 1000;

      // Determines the initial duration during which incoming values are skipped in the Exponential Moving Average
      // calculation. Until this duration has elapsed, average returned will be equal to AVG_INITIALIZATION_VAL.
      String CONFIG_OF_WARMUP_DURATION_MS = CONFIG_PREFIX + ".warmup.duration";
      long DEFAULT_WARMUP_DURATION_MS = 0;

      // Determines the initialization value for Exponential Moving Average.
      String CONFIG_OF_AVG_INITIALIZATION_VAL = CONFIG_PREFIX + ".avg.initialization.val";
      double DEFAULT_AVG_INITIALIZATION_VAL = 1.0;

      // Parameters related to Hybrid score.
      String CONFIG_OF_HYBRID_SCORE_EXPONENT = CONFIG_PREFIX + ".hybrid.score.exponent";
      int DEFAULT_HYBRID_SCORE_EXPONENT = 3;

      // Threadpool size of ServerRoutingStatsManager. This controls the number of threads available to update routing
      // stats for servers upon query submission and response arrival.
      String CONFIG_OF_STATS_MANAGER_THREADPOOL_SIZE =
          CONFIG_PREFIX + ".stats.manager.threadpool.size";
      int DEFAULT_STATS_MANAGER_THREADPOOL_SIZE = 2;
    }

    interface Grpc {
      String KEY_OF_GRPC_PORT = "pinot.broker.grpc.port";
      String KEY_OF_GRPC_TLS_ENABLED = "pinot.broker.grpc.tls.enabled";
      String KEY_OF_GRPC_TLS_PORT = "pinot.broker.grpc.tls.port";
      String KEY_OF_GRPC_TLS_PREFIX = "pinot.broker.grpctls";

      String BLOCK_ROW_SIZE = "blockRowSize";
      int DEFAULT_BLOCK_ROW_SIZE = 10_000;
      String COMPRESSION = "compression";
      String DEFAULT_COMPRESSION = "ZSTD";
      String ENCODING = "encoding";
      String DEFAULT_ENCODING = "JSON";
    }

    String PREFIX_OF_CONFIG_OF_PINOT_FS_FACTORY = "pinot.broker.storage.factory";

    String USE_MSE_TO_FILL_EMPTY_RESPONSE_SCHEMA =
        "pinot.broker.use.mse.to.fill.empty.response.schema";
    boolean DEFAULT_USE_MSE_TO_FILL_EMPTY_RESPONSE_SCHEMA = false;
  }

  interface Server {
    String INSTANCE_DATA_MANAGER_CONFIG_PREFIX = "pinot.server.instance";
    String QUERY_EXECUTOR_CONFIG_PREFIX = "pinot.server.query.executor";
    String METRICS_CONFIG_PREFIX = "pinot.server.metrics";

    String CONFIG_OF_INSTANCE_DATA_MANAGER_CLASS = "pinot.server.instance.data.manager.class";
    String DEFAULT_INSTANCE_DATA_MANAGER_CLASS =
        "org.apache.pinot.server.starter.helix.HelixInstanceDataManager";
    // Following configs are used in HelixInstanceDataManagerConfig, where the config prefix is trimmed. We keep the
    // full config for reference purpose.
    String INSTANCE_ID = "id";
    String CONFIG_OF_INSTANCE_ID = INSTANCE_DATA_MANAGER_CONFIG_PREFIX + "." + INSTANCE_ID;
    String INSTANCE_DATA_DIR = "dataDir";
    String CONFIG_OF_INSTANCE_DATA_DIR =
        INSTANCE_DATA_MANAGER_CONFIG_PREFIX + "." + INSTANCE_DATA_DIR;
    String DEFAULT_INSTANCE_BASE_DIR =
        FileUtils.getTempDirectoryPath() + File.separator + "PinotServer";
    String DEFAULT_INSTANCE_DATA_DIR = DEFAULT_INSTANCE_BASE_DIR + File.separator + "index";
    String CONSUMER_DIR = "consumerDir";
    String CONFIG_OF_CONSUMER_DIR = INSTANCE_DATA_MANAGER_CONFIG_PREFIX + "." + CONSUMER_DIR;
    String INSTANCE_SEGMENT_TAR_DIR = "segmentTarDir";
    String CONFIG_OF_INSTANCE_SEGMENT_TAR_DIR =
        INSTANCE_DATA_MANAGER_CONFIG_PREFIX + "." + INSTANCE_SEGMENT_TAR_DIR;
    String DEFAULT_INSTANCE_SEGMENT_TAR_DIR =
        DEFAULT_INSTANCE_BASE_DIR + File.separator + "segmentTar";
    String CONSUMER_CLIENT_ID_SUFFIX = "consumer.client.id.suffix";
    String CONFIG_OF_CONSUMER_CLIENT_ID_SUFFIX =
        INSTANCE_DATA_MANAGER_CONFIG_PREFIX + "." + CONSUMER_CLIENT_ID_SUFFIX;
    String SEGMENT_STORE_URI = "segment.store.uri";
    String CONFIG_OF_SEGMENT_STORE_URI =
        INSTANCE_DATA_MANAGER_CONFIG_PREFIX + "." + SEGMENT_STORE_URI;
    String TABLE_DATA_MANAGER_PROVIDER_CLASS = "table.data.manager.provider.class";
    String CONFIG_OF_TABLE_DATA_MANAGER_PROVIDER_CLASS =
        INSTANCE_DATA_MANAGER_CONFIG_PREFIX + "." + TABLE_DATA_MANAGER_PROVIDER_CLASS;
    String DEFAULT_TABLE_DATA_MANAGER_PROVIDER_CLASS =
        "org.apache.pinot.core.data.manager.provider.DefaultTableDataManagerProvider";
    String READ_MODE = "readMode";
    String CONFIG_OF_READ_MODE = INSTANCE_DATA_MANAGER_CONFIG_PREFIX + "." + READ_MODE;
    String DEFAULT_READ_MODE = "mmap";
    String SEGMENT_FORMAT_VERSION = "segment.format.version";
    String CONFIG_OF_SEGMENT_FORMAT_VERSION =
        INSTANCE_DATA_MANAGER_CONFIG_PREFIX + "." + SEGMENT_FORMAT_VERSION;
    String REALTIME_OFFHEAP_ALLOCATION = "realtime.alloc.offheap";
    String CONFIG_OF_REALTIME_OFFHEAP_ALLOCATION =
        INSTANCE_DATA_MANAGER_CONFIG_PREFIX + "." + REALTIME_OFFHEAP_ALLOCATION;
    boolean DEFAULT_REALTIME_OFFHEAP_ALLOCATION = true;
    String REALTIME_OFFHEAP_DIRECT_ALLOCATION = "realtime.alloc.offheap.direct";
    String CONFIG_OF_REALTIME_OFFHEAP_DIRECT_ALLOCATION =
        INSTANCE_DATA_MANAGER_CONFIG_PREFIX + "." + REALTIME_OFFHEAP_DIRECT_ALLOCATION;
    boolean DEFAULT_REALTIME_OFFHEAP_DIRECT_ALLOCATION = false;
    String RELOAD_CONSUMING_SEGMENT = "reload.consumingSegment";
    String CONFIG_OF_RELOAD_CONSUMING_SEGMENT =
        INSTANCE_DATA_MANAGER_CONFIG_PREFIX + "." + RELOAD_CONSUMING_SEGMENT;
    boolean DEFAULT_RELOAD_CONSUMING_SEGMENT = true;

    // Query logger related configs
    String CONFIG_OF_QUERY_LOG_MAX_RATE = "pinot.server.query.log.maxRatePerSecond";
    @Deprecated
    String DEPRECATED_CONFIG_OF_QUERY_LOG_MAX_RATE =
        "pinot.query.scheduler.query.log.maxRatePerSecond";
    double DEFAULT_QUERY_LOG_MAX_RATE = 10_000;
    String CONFIG_OF_QUERY_LOG_DROPPED_REPORT_MAX_RATE =
        "pinot.server.query.log.droppedReportMaxRatePerSecond";
    double DEFAULT_QUERY_LOG_DROPPED_REPORT_MAX_RATE = 1;

    /* Start of query executor related configs */

    String CLASS = "class";
    String CONFIG_OF_QUERY_EXECUTOR_CLASS = QUERY_EXECUTOR_CONFIG_PREFIX + "." + CLASS;
    String DEFAULT_QUERY_EXECUTOR_CLASS =
        "org.apache.pinot.core.query.executor.ServerQueryExecutorV1Impl";
    String PRUNER = "pruner";
    String QUERY_EXECUTOR_PRUNER_CONFIG_PREFIX = QUERY_EXECUTOR_CONFIG_PREFIX + "." + PRUNER;
    String CONFIG_OF_QUERY_EXECUTOR_PRUNER_CLASS =
        QUERY_EXECUTOR_PRUNER_CONFIG_PREFIX + "." + CLASS;
    // The order of the pruners matters. Pruning with segment metadata ahead of those using segment data like bloom
    // filters to reduce the required data access.
    List<String> DEFAULT_QUERY_EXECUTOR_PRUNER_CLASS =
        List.of("ColumnValueSegmentPruner", "BloomFilterSegmentPruner", "SelectionQuerySegmentPruner");
    String PLAN_MAKER_CLASS = "plan.maker.class";
    String CONFIG_OF_QUERY_EXECUTOR_PLAN_MAKER_CLASS =
        QUERY_EXECUTOR_CONFIG_PREFIX + "." + PLAN_MAKER_CLASS;
    String DEFAULT_QUERY_EXECUTOR_PLAN_MAKER_CLASS =
        "org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2";
    String TIMEOUT = "timeout";
    String CONFIG_OF_QUERY_EXECUTOR_TIMEOUT = QUERY_EXECUTOR_CONFIG_PREFIX + "." + TIMEOUT;
    long DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS = 15_000L;
    String MAX_EXECUTION_THREADS = "max.execution.threads";
    String CONFIG_OF_QUERY_EXECUTOR_MAX_EXECUTION_THREADS =
        QUERY_EXECUTOR_CONFIG_PREFIX + "." + MAX_EXECUTION_THREADS;
    int DEFAULT_QUERY_EXECUTOR_MAX_EXECUTION_THREADS = -1;  // Use number of CPU cores

    // Group-by query related configs
    String NUM_GROUPS_LIMIT = "num.groups.limit";
    String CONFIG_OF_QUERY_EXECUTOR_NUM_GROUPS_LIMIT =
        QUERY_EXECUTOR_CONFIG_PREFIX + "." + NUM_GROUPS_LIMIT;
    int DEFAULT_QUERY_EXECUTOR_NUM_GROUPS_LIMIT = 100_000;
    String NUM_GROUPS_WARN_LIMIT = "num.groups.warn.limit";
    String CONFIG_OF_QUERY_EXECUTOR_NUM_GROUPS_WARN_LIMIT =
        QUERY_EXECUTOR_CONFIG_PREFIX + "." + NUM_GROUPS_WARN_LIMIT;
    int DEFAULT_QUERY_EXECUTOR_NUM_GROUPS_WARN_LIMIT = 150_000;
    String MAX_INITIAL_RESULT_HOLDER_CAPACITY = "max.init.group.holder.capacity";
    String CONFIG_OF_QUERY_EXECUTOR_MAX_INITIAL_RESULT_HOLDER_CAPACITY =
        QUERY_EXECUTOR_CONFIG_PREFIX + "." + MAX_INITIAL_RESULT_HOLDER_CAPACITY;
    int DEFAULT_QUERY_EXECUTOR_MAX_INITIAL_RESULT_HOLDER_CAPACITY = 10_000;
    String MIN_INITIAL_INDEXED_TABLE_CAPACITY = "min.init.indexed.table.capacity";
    String CONFIG_OF_QUERY_EXECUTOR_MIN_INITIAL_INDEXED_TABLE_CAPACITY =
        QUERY_EXECUTOR_CONFIG_PREFIX + "." + MIN_INITIAL_INDEXED_TABLE_CAPACITY;
    int DEFAULT_QUERY_EXECUTOR_MIN_INITIAL_INDEXED_TABLE_CAPACITY = 128;
    String MSE = "mse";
    String MSE_CONFIG_PREFIX = QUERY_EXECUTOR_CONFIG_PREFIX + "." + MSE;
    String CONFIG_OF_MSE_MAX_INITIAL_RESULT_HOLDER_CAPACITY =
        MSE_CONFIG_PREFIX + "." + MAX_INITIAL_RESULT_HOLDER_CAPACITY;
    String CONFIG_OF_MSE_MAX_EXECUTION_THREADS =
        MSE_CONFIG_PREFIX + "." + MAX_EXECUTION_THREADS;
    int DEFAULT_MSE_MAX_EXECUTION_THREADS = -1;
    String CONFIG_OF_MSE_MAX_EXECUTION_THREADS_EXCEED_STRATEGY =
        MSE_CONFIG_PREFIX + "." + MAX_EXECUTION_THREADS + ".exceed.strategy";
    String DEFAULT_MSE_MAX_EXECUTION_THREADS_EXCEED_STRATEGY = "ERROR";

    // For group-by queries with order-by clause, the tail groups are trimmed off to reduce the memory footprint. To
    // ensure the accuracy of the result, {@code max(limit * 5, minTrimSize)} groups are retained. When
    // {@code minTrimSize} is non-positive, trim is disabled.
    //
    // Caution:
    // Setting trim size to non-positive value (disable trim) or large value gives more accurate result, but can
    // potentially cause higher memory pressure.
    //
    // Trim can be applied in the following stages:
    // - Segment level: after getting the segment level results, before merging them into server level results.
    // - Server level: while merging the segment level results into server level results.
    // - Broker level: while merging the server level results into broker level results. (SSE only)
    // - MSE intermediate stage (MSE only)
    String MIN_SEGMENT_GROUP_TRIM_SIZE = "min.segment.group.trim.size";
    String CONFIG_OF_QUERY_EXECUTOR_MIN_SEGMENT_GROUP_TRIM_SIZE =
        QUERY_EXECUTOR_CONFIG_PREFIX + "." + MIN_SEGMENT_GROUP_TRIM_SIZE;
    int DEFAULT_QUERY_EXECUTOR_MIN_SEGMENT_GROUP_TRIM_SIZE = -1;
    String MIN_SERVER_GROUP_TRIM_SIZE = "min.server.group.trim.size";
    String CONFIG_OF_QUERY_EXECUTOR_MIN_SERVER_GROUP_TRIM_SIZE =
        QUERY_EXECUTOR_CONFIG_PREFIX + "." + MIN_SERVER_GROUP_TRIM_SIZE;
    // Match the value of GroupByUtils.DEFAULT_MIN_NUM_GROUPS
    int DEFAULT_QUERY_EXECUTOR_MIN_SERVER_GROUP_TRIM_SIZE = 5000;
    String GROUPBY_TRIM_THRESHOLD = "groupby.trim.threshold";
    String CONFIG_OF_QUERY_EXECUTOR_GROUPBY_TRIM_THRESHOLD =
        QUERY_EXECUTOR_CONFIG_PREFIX + "." + GROUPBY_TRIM_THRESHOLD;
    int DEFAULT_QUERY_EXECUTOR_GROUPBY_TRIM_THRESHOLD = 1_000_000;
    String CONFIG_OF_MSE_MIN_GROUP_TRIM_SIZE = MSE_CONFIG_PREFIX + ".min.group.trim.size";
    // Match the value of GroupByUtils.DEFAULT_MIN_NUM_GROUPS
    int DEFAULT_MSE_MIN_GROUP_TRIM_SIZE = 5000;

    // TODO: Merge this with "mse"
    /**
     * The ExecutorServiceProvider to use for execution threads, which are the ones that execute
     * MultiStageOperators (and SSE operators in the leaf stages).
     *
     * It is recommended to use cached. In case fixed is used, it should use a large enough number of threads or
     * parent operators may consume all threads.
     * In Java 21 or newer, virtual threads are a good solution. Although Apache Pinot doesn't include this option yet,
     * it is trivial to implement that plugin.
     *
     * See QueryRunner
     */
    String MULTISTAGE_EXECUTOR = "multistage.executor";
    String MULTISTAGE_EXECUTOR_CONFIG_PREFIX =
        QUERY_EXECUTOR_CONFIG_PREFIX + "." + MULTISTAGE_EXECUTOR;
    String DEFAULT_MULTISTAGE_EXECUTOR_TYPE = "cached";
    /**
     * The ExecutorServiceProvider to be used for submission threads, which are the ones
     * that receive requests in protobuf and transform them into MultiStageOperators.
     *
     * It is recommended to use a fixed thread pool, given submission code should not block.
     *
     * See QueryServer
     */
    String MULTISTAGE_SUBMISSION_EXEC_CONFIG_PREFIX =
        QUERY_EXECUTOR_CONFIG_PREFIX + "." + "multistage.submission";
    String DEFAULT_MULTISTAGE_SUBMISSION_EXEC_TYPE = "fixed";
    @Deprecated
    String CONFIG_OF_QUERY_EXECUTOR_OPCHAIN_EXECUTOR = MULTISTAGE_EXECUTOR_CONFIG_PREFIX;
    @Deprecated
    String DEFAULT_QUERY_EXECUTOR_OPCHAIN_EXECUTOR = DEFAULT_MULTISTAGE_EXECUTOR_TYPE;

    // Enable SSE & MSE task throttling on critical heap usage.
    String CONFIG_OF_ENABLE_QUERY_SCHEDULER_THROTTLING_ON_HEAP_USAGE =
        QUERY_EXECUTOR_CONFIG_PREFIX + ".enableThrottlingOnHeapUsage";
    boolean DEFAULT_ENABLE_QUERY_SCHEDULER_THROTTLING_ON_HEAP_USAGE = false;

    /**
     * The ExecutorServiceProvider to be used for timeseries threads.
     *
     * It is recommended to use a cached thread pool, given timeseries endpoints are blocking.
     *
     * See QueryServer
     */
    String MULTISTAGE_TIMESERIES_EXEC_CONFIG_PREFIX =
        QUERY_EXECUTOR_CONFIG_PREFIX + "." + "timeseries";
    String DEFAULT_TIMESERIES_EXEC_CONFIG_PREFIX = "cached";
    /* End of query executor related configs */

    String CONFIG_OF_TRANSFORM_FUNCTIONS = "pinot.server.transforms";
    String CONFIG_OF_SERVER_QUERY_REWRITER_CLASS_NAMES = "pinot.server.query.rewriter.class.names";
    String CONFIG_OF_SERVER_QUERY_REGEX_CLASS = "pinot.server.query.regex.class";
    String DEFAULT_SERVER_QUERY_REGEX_CLASS = "JAVA_UTIL";
    String CONFIG_OF_ENABLE_QUERY_CANCELLATION = "pinot.server.enable.query.cancellation";
    String CONFIG_OF_NETTY_SERVER_ENABLED = "pinot.server.netty.enabled";
    boolean DEFAULT_NETTY_SERVER_ENABLED = true;
    String CONFIG_OF_ENABLE_GRPC_SERVER = "pinot.server.grpc.enable";
    boolean DEFAULT_ENABLE_GRPC_SERVER = true;
    String CONFIG_OF_GRPC_PORT = "pinot.server.grpc.port";
    int DEFAULT_GRPC_PORT = 8090;
    String CONFIG_OF_GRPCTLS_SERVER_ENABLED = "pinot.server.grpctls.enabled";
    boolean DEFAULT_GRPCTLS_SERVER_ENABLED = false;
    String CONFIG_OF_NETTYTLS_SERVER_ENABLED = "pinot.server.nettytls.enabled";
    boolean DEFAULT_NETTYTLS_SERVER_ENABLED = false;
    String CONFIG_OF_SWAGGER_SERVER_ENABLED = "pinot.server.swagger.enabled";
    boolean DEFAULT_SWAGGER_SERVER_ENABLED = true;
    String CONFIG_OF_SWAGGER_USE_HTTPS = "pinot.server.swagger.use.https";
    String CONFIG_OF_ADMIN_API_PORT = "pinot.server.adminapi.port";
    int DEFAULT_ADMIN_API_PORT = 8097;
    String CONFIG_OF_SERVER_RESOURCE_PACKAGES = "server.restlet.api.resource.packages";
    String DEFAULT_SERVER_RESOURCE_PACKAGES = "org.apache.pinot.server.api.resources";

    String PREFIX_OF_CONFIG_OF_PINOT_FS_FACTORY = "pinot.server.storage.factory";
    String PREFIX_OF_CONFIG_OF_PINOT_CRYPTER = "pinot.server.crypter";
    String CONFIG_OF_VALUE_PRUNER_IN_PREDICATE_THRESHOLD =
        "pinot.server.query.executor.pruner.columnvaluesegmentpruner.inpredicate.threshold";
    int DEFAULT_VALUE_PRUNER_IN_PREDICATE_THRESHOLD = 10;

    /**
     * Service token for accessing protected controller APIs.
     * E.g. null (auth disabled), "Basic abcdef..." (basic auth), "Bearer 123def..." (oauth2)
     */
    String CONFIG_OF_AUTH = KEY_OF_AUTH;

    // Configuration to consider the server ServiceStatus as being STARTED if the percent of resources (tables) that
    // are ONLINE for this this server has crossed the threshold percentage of the total number of tables
    // that it is expected to serve.
    String CONFIG_OF_SERVER_MIN_RESOURCE_PERCENT_FOR_START =
        "pinot.server.startup.minResourcePercent";
    double DEFAULT_SERVER_MIN_RESOURCE_PERCENT_FOR_START = 100.0;
    String CONFIG_OF_STARTUP_REALTIME_CONSUMPTION_CATCHUP_WAIT_MS =
        "pinot.server.starter.realtimeConsumptionCatchupWaitMs";
    int DEFAULT_STARTUP_REALTIME_CONSUMPTION_CATCHUP_WAIT_MS = 0;
    String CONFIG_OF_ENABLE_REALTIME_OFFSET_BASED_CONSUMPTION_STATUS_CHECKER =
        "pinot.server.starter.enableRealtimeOffsetBasedConsumptionStatusChecker";
    boolean DEFAULT_ENABLE_REALTIME_OFFSET_BASED_CONSUMPTION_STATUS_CHECKER = false;

    String CONFIG_OF_ENABLE_REALTIME_FRESHNESS_BASED_CONSUMPTION_STATUS_CHECKER =
        "pinot.server.starter.enableRealtimeFreshnessBasedConsumptionStatusChecker";
    boolean DEFAULT_ENABLE_REALTIME_FRESHNESS_BASED_CONSUMPTION_STATUS_CHECKER = false;
    // This configuration is in place to avoid servers getting stuck checking for freshness in
    // cases where they will never be able to reach the freshness threshold or the latest offset.
    // The only current case where we have seen this is low volume streams using read_committed
    // because of transactional publishes where the last message in the stream is an
    // un-consumable kafka control message, and it is impossible to tell if the consumer is stuck
    // or some offsets will never be consumable.
    //
    // When in doubt, do not enable this configuration as it can cause a lagged server to start
    // serving queries.
    String CONFIG_OF_REALTIME_FRESHNESS_IDLE_TIMEOUT_MS =
        "pinot.server.starter.realtimeFreshnessIdleTimeoutMs";
    int DEFAULT_REALTIME_FRESHNESS_IDLE_TIMEOUT_MS = 0;
    String CONFIG_OF_STARTUP_REALTIME_MIN_FRESHNESS_MS =
        "pinot.server.starter.realtimeMinFreshnessMs";
    // Use 10 seconds by default so high volume stream are able to catch up.
    // This is also the default in the case a user misconfigures this by setting to <= 0.
    int DEFAULT_STARTUP_REALTIME_MIN_FRESHNESS_MS = 10000;

    // Config for realtime consumption message rate limit
    String CONFIG_OF_SERVER_CONSUMPTION_RATE_LIMIT = "pinot.server.consumption.rate.limit";
    // Default to 0.0 (no limit)
    double DEFAULT_SERVER_CONSUMPTION_RATE_LIMIT = 0.0;

    String CONFIG_OF_MMAP_DEFAULT_ADVICE = "pinot.server.mmap.advice.default";
    String PREFIX_OF_CONFIG_OF_SEGMENT_FETCHER_FACTORY = "pinot.server.segment.fetcher";

    // Configs for server starter startup/shutdown checks
    // Startup: timeout for the startup checks
    String CONFIG_OF_STARTUP_TIMEOUT_MS = "pinot.server.startup.timeoutMs";
    long DEFAULT_STARTUP_TIMEOUT_MS = 600_000L;
    // Startup: enable service status check before claiming server up
    String CONFIG_OF_STARTUP_ENABLE_SERVICE_STATUS_CHECK =
        "pinot.server.startup.enableServiceStatusCheck";
    boolean DEFAULT_STARTUP_ENABLE_SERVICE_STATUS_CHECK = true;
    // The timeouts above determine how long servers will poll their status before giving up.
    // This configuration determines what we do when we give up. By default, we will mark the
    // server as healthy and start the query server. If this is set to true, we instead throw
    // an exception and exit the server. This is useful if you want to ensure that the server
    // is always fully ready before accepting queries. But note that this can cause the server
    // to never be healthy if there is some reason that it can never reach a GOOD status.
    String CONFIG_OF_EXIT_ON_SERVICE_STATUS_CHECK_FAILURE =
        "pinot.server.startup.exitOnServiceStatusCheckFailure";
    boolean DEFAULT_EXIT_ON_SERVICE_STATUS_CHECK_FAILURE = false;
    String CONFIG_OF_STARTUP_SERVICE_STATUS_CHECK_INTERVAL_MS =
        "pinot.server.startup.serviceStatusCheckIntervalMs";
    long DEFAULT_STARTUP_SERVICE_STATUS_CHECK_INTERVAL_MS = 10_000L;
    // Shutdown: timeout for the shutdown checks
    String CONFIG_OF_SHUTDOWN_TIMEOUT_MS = "pinot.server.shutdown.timeoutMs";
    long DEFAULT_SHUTDOWN_TIMEOUT_MS = 600_000L;
    // Shutdown: enable query check before shutting down the server
    //           Will drain queries (no incoming queries and all existing queries finished)
    String CONFIG_OF_SHUTDOWN_ENABLE_QUERY_CHECK = "pinot.server.shutdown.enableQueryCheck";
    boolean DEFAULT_SHUTDOWN_ENABLE_QUERY_CHECK = true;
    // Shutdown: threshold to mark that there is no incoming queries, use max query time as the default threshold
    String CONFIG_OF_SHUTDOWN_NO_QUERY_THRESHOLD_MS = "pinot.server.shutdown.noQueryThresholdMs";
    // Shutdown: enable resource check before shutting down the server
    //           Will wait until all the resources in the external view are neither ONLINE nor CONSUMING
    //           No need to enable this check if startup service status check is enabled
    String CONFIG_OF_SHUTDOWN_ENABLE_RESOURCE_CHECK = "pinot.server.shutdown.enableResourceCheck";
    boolean DEFAULT_SHUTDOWN_ENABLE_RESOURCE_CHECK = false;
    String CONFIG_OF_SHUTDOWN_RESOURCE_CHECK_INTERVAL_MS =
        "pinot.server.shutdown.resourceCheckIntervalMs";
    long DEFAULT_SHUTDOWN_RESOURCE_CHECK_INTERVAL_MS = 10_000L;

    String DEFAULT_COLUMN_MIN_MAX_VALUE_GENERATOR_MODE = "ALL";

    String PINOT_SERVER_METRICS_PREFIX = "pinot.server.metrics.prefix";

    String SERVER_TLS_PREFIX = "pinot.server.tls";
    String SERVER_NETTYTLS_PREFIX = "pinot.server.nettytls";
    String SERVER_GRPCTLS_PREFIX = "pinot.server.grpctls";
    String SERVER_NETTY_PREFIX = "pinot.server.netty";

    String CONFIG_OF_LOGGER_ROOT_DIR = "pinot.server.logger.root.dir";

    String LUCENE_MAX_REFRESH_THREADS = "pinot.server.lucene.max.refresh.threads";
    int DEFAULT_LUCENE_MAX_REFRESH_THREADS = 1;
    String LUCENE_MIN_REFRESH_INTERVAL_MS = "pinot.server.lucene.min.refresh.interval.ms";
    int DEFAULT_LUCENE_MIN_REFRESH_INTERVAL_MS = 10;

    String CONFIG_OF_MESSAGES_COUNT_REFRESH_INTERVAL_SECONDS =
        "pinot.server.messagesCount.refreshIntervalSeconds";
    int DEFAULT_MESSAGES_COUNT_REFRESH_INTERVAL_SECONDS = 30;

    interface SegmentCompletionProtocol {
      String PREFIX_OF_CONFIG_OF_SEGMENT_UPLOADER = "pinot.server.segment.uploader";

      /**
       * Deprecated. Enable legacy https configs for segment upload.
       * Use server-wide TLS configs instead.
       */
      @Deprecated
      String CONFIG_OF_CONTROLLER_HTTPS_ENABLED = "enabled";

      /**
       * Deprecated. Set the legacy https port for segment upload.
       * Use server-wide TLS configs instead.
       */
      @Deprecated
      String CONFIG_OF_CONTROLLER_HTTPS_PORT = "controller.port";

      String CONFIG_OF_SEGMENT_UPLOAD_REQUEST_TIMEOUT_MS = "upload.request.timeout.ms";

      /**
       * Specify connection scheme to use for controller upload connections. Defaults to "http"
       */
      String CONFIG_OF_PROTOCOL = "protocol";

      /**
       * Service token for accessing protected controller APIs.
       * E.g. null (auth disabled), "Basic abcdef..." (basic auth), "Bearer 123def..." (oauth2)
       */
      String CONFIG_OF_SEGMENT_UPLOADER_AUTH = KEY_OF_AUTH;

      int DEFAULT_SEGMENT_UPLOAD_REQUEST_TIMEOUT_MS = 300_000;
      int DEFAULT_OTHER_REQUESTS_TIMEOUT = 10_000;
    }

    String DEFAULT_METRICS_PREFIX = "pinot.server.";
    String CONFIG_OF_ENABLE_TABLE_LEVEL_METRICS = "pinot.server.enableTableLevelMetrics";
    boolean DEFAULT_ENABLE_TABLE_LEVEL_METRICS = true;
    String CONFIG_OF_ALLOWED_TABLES_FOR_EMITTING_METRICS =
        "pinot.server.allowedTablesForEmittingMetrics";
    String ACCESS_CONTROL_FACTORY_CLASS = "pinot.server.admin.access.control.factory.class";
    String DEFAULT_ACCESS_CONTROL_FACTORY_CLASS =
        "org.apache.pinot.server.access.AllowAllAccessFactory";
    String PREFIX_OF_CONFIG_OF_ACCESS_CONTROL = "pinot.server.admin.access.control";

    String CONFIG_OF_ENABLE_THREAD_CPU_TIME_MEASUREMENT =
        "pinot.server.instance.enableThreadCpuTimeMeasurement";
    String CONFIG_OF_ENABLE_THREAD_ALLOCATED_BYTES_MEASUREMENT =
        "pinot.server.instance.enableThreadAllocatedBytesMeasurement";
    boolean DEFAULT_ENABLE_THREAD_CPU_TIME_MEASUREMENT = false;
    boolean DEFAULT_THREAD_ALLOCATED_BYTES_MEASUREMENT = false;

    String CONFIG_OF_CURRENT_DATA_TABLE_VERSION = "pinot.server.instance.currentDataTableVersion";

    // Environment Provider Configs
    String PREFIX_OF_CONFIG_OF_ENVIRONMENT_PROVIDER_FACTORY =
        "pinot.server.environmentProvider.factory";
    String ENVIRONMENT_PROVIDER_CLASS_NAME = "pinot.server.environmentProvider.className";

    /// All the keys should be prefixed with {@link #INSTANCE_DATA_MANAGER_CONFIG_PREFIX}
    interface Upsert {
      String CONFIG_PREFIX = "upsert";
      String DEFAULT_METADATA_MANAGER_CLASS = "default.metadata.manager.class";
      String DEFAULT_ENABLE_SNAPSHOT = "default.enable.snapshot";
      String DEFAULT_ENABLE_PRELOAD = "default.enable.preload";

      /// @deprecated use {@link org.apache.pinot.spi.config.table.ingestion.ParallelSegmentConsumptionPolicy)} instead.
      @Deprecated
      String DEFAULT_ALLOW_PARTIAL_UPSERT_CONSUMPTION_DURING_COMMIT =
          "default.allow.partial.upsert.consumption.during.commit";
    }

    /// All the keys should be prefixed with {@link #INSTANCE_DATA_MANAGER_CONFIG_PREFIX}
    interface Dedup {
      String CONFIG_PREFIX = "dedup";
      String DEFAULT_METADATA_MANAGER_CLASS = "default.metadata.manager.class";
      String DEFAULT_ENABLE_PRELOAD = "default.enable.preload";
      String DEFAULT_IGNORE_NON_DEFAULT_TIERS = "default.ignore.non.default.tiers";

      /// @deprecated use {@link org.apache.pinot.spi.config.table.ingestion.ParallelSegmentConsumptionPolicy)} instead.
      @Deprecated
      String DEFAULT_ALLOW_DEDUP_CONSUMPTION_DURING_COMMIT =
          "default.allow.dedup.consumption.during.commit";
    }
  }

  interface Controller {
    String PREFIX_OF_CONFIG_OF_SEGMENT_FETCHER_FACTORY = "pinot.controller.segment.fetcher";
    String PREFIX_OF_CONFIG_OF_PINOT_FS_FACTORY = "pinot.controller.storage.factory";
    String HOST_HTTP_HEADER = "Pinot-Controller-Host";
    String VERSION_HTTP_HEADER = "Pinot-Controller-Version";
    String SEGMENT_NAME_HTTP_HEADER = "Pinot-Segment-Name";
    String TABLE_NAME_HTTP_HEADER = "Pinot-Table-Name";
    String PINOT_QUERY_ERROR_CODE_HEADER = "X-Pinot-Error-Code";
    String INGESTION_DESCRIPTOR = "Pinot-Ingestion-Descriptor";
    String PREFIX_OF_CONFIG_OF_PINOT_CRYPTER = "pinot.controller.crypter";

    String CONFIG_OF_CONTROLLER_METRICS_PREFIX = "controller.metrics.prefix";
    // FYI this is incorrect as it generate metrics named without a dot after pinot.controller part,
    // but we keep this default for backward compatibility in case someone relies on this format
    // see Server or Broker class for correct prefix format you should use
    String DEFAULT_METRICS_PREFIX = "pinot.controller.";

    String CONFIG_OF_INSTANCE_ID = "pinot.controller.instance.id";
    String CONFIG_OF_CONTROLLER_QUERY_REWRITER_CLASS_NAMES =
        "pinot.controller.query.rewriter.class.names";

    // Task Manager configuration
    String CONFIG_OF_TASK_MANAGER_CLASS = "pinot.controller.task.manager.class";
    String DEFAULT_TASK_MANAGER_CLASS =
        "org.apache.pinot.controller.helix.core.minion.PinotTaskManager";

    //Set to true to load all services tagged and compiled with hk2-metadata-generator. Default to False
    String CONTROLLER_SERVICE_AUTO_DISCOVERY = "pinot.controller.service.auto.discovery";
    String CONFIG_OF_LOGGER_ROOT_DIR = "pinot.controller.logger.root.dir";
    String PREFIX_OF_PINOT_CONTROLLER_SEGMENT_COMPLETION = "pinot.controller.segment.completion";
  }

  interface Minion {
    String CONFIG_OF_METRICS_PREFIX = "pinot.minion.";
    String CONFIG_OF_MINION_ID = "pinot.minion.instance.id";
    String METADATA_EVENT_OBSERVER_PREFIX = "metadata.event.notifier";

    // Config keys
    String CONFIG_OF_SWAGGER_USE_HTTPS = "pinot.minion.swagger.use.https";
    String CONFIG_OF_METRICS_PREFIX_KEY = "pinot.minion.metrics.prefix";
    @Deprecated
    String DEPRECATED_CONFIG_OF_METRICS_PREFIX_KEY = "metricsPrefix";
    String METRICS_REGISTRY_REGISTRATION_LISTENERS_KEY = "metricsRegistryRegistrationListeners";
    String METRICS_CONFIG_PREFIX = "pinot.minion.metrics";

    // Default settings
    int DEFAULT_HELIX_PORT = 9514;
    String DEFAULT_INSTANCE_BASE_DIR =
        System.getProperty("java.io.tmpdir") + File.separator + "PinotMinion";
    String DEFAULT_INSTANCE_DATA_DIR = DEFAULT_INSTANCE_BASE_DIR + File.separator + "data";

    // Add pinot.minion prefix on those configs to be consistent with configs of controller and server.
    String PREFIX_OF_CONFIG_OF_PINOT_FS_FACTORY = "pinot.minion.storage.factory";
    String PREFIX_OF_CONFIG_OF_SEGMENT_FETCHER_FACTORY = "pinot.minion.segment.fetcher";
    String PREFIX_OF_CONFIG_OF_SEGMENT_UPLOADER = "pinot.minion.segment.uploader";
    String PREFIX_OF_CONFIG_OF_PINOT_CRYPTER = "pinot.minion.crypter";
    @Deprecated
    String DEPRECATED_PREFIX_OF_CONFIG_OF_PINOT_FS_FACTORY = "storage.factory";
    @Deprecated
    String DEPRECATED_PREFIX_OF_CONFIG_OF_SEGMENT_FETCHER_FACTORY = "segment.fetcher";
    @Deprecated
    String DEPRECATED_PREFIX_OF_CONFIG_OF_SEGMENT_UPLOADER = "segment.uploader";
    @Deprecated
    String DEPRECATED_PREFIX_OF_CONFIG_OF_PINOT_CRYPTER = "crypter";

    /**
     * Service token for accessing protected controller APIs.
     * E.g. null (auth disabled), "Basic abcdef..." (basic auth), "Bearer 123def..." (oauth2)
     */
    String CONFIG_TASK_AUTH_NAMESPACE = "task.auth";
    String MINION_TLS_PREFIX = "pinot.minion.tls";
    String CONFIG_OF_MINION_QUERY_REWRITER_CLASS_NAMES = "pinot.minion.query.rewriter.class.names";
    String CONFIG_OF_LOGGER_ROOT_DIR = "pinot.minion.logger.root.dir";
    String CONFIG_OF_EVENT_OBSERVER_CLEANUP_DELAY_IN_SEC =
        "pinot.minion.event.observer.cleanupDelayInSec";
    char TASK_LIST_SEPARATOR = ',';
    String CONFIG_OF_ALLOW_DOWNLOAD_FROM_SERVER = "pinot.minion.task.allow.download.from.server";
    String DEFAULT_ALLOW_DOWNLOAD_FROM_SERVER = "false";
  }

  interface ControllerJob {
    /**
     * Controller job ZK props
     */
    String JOB_TYPE = "jobType";
    String TABLE_NAME_WITH_TYPE = "tableName";
    String TENANT_NAME = "tenantName";
    String JOB_ID = "jobId";
    String SUBMISSION_TIME_MS = "submissionTimeMs";
    String MESSAGE_COUNT = "messageCount";

    Integer DEFAULT_MAXIMUM_CONTROLLER_JOBS_IN_ZK = 100;

    /**
     * Segment reload job ZK props
     */
    String SEGMENT_RELOAD_JOB_SEGMENT_NAME = "segmentName";
    String SEGMENT_RELOAD_JOB_INSTANCE_NAME = "instanceName";
    // Force commit job ZK props
    String CONSUMING_SEGMENTS_FORCE_COMMITTED_LIST = "segmentsForceCommitted";
    String CONSUMING_SEGMENTS_YET_TO_BE_COMMITTED_LIST = "segmentsYetToBeCommitted";
    String NUM_CONSUMING_SEGMENTS_YET_TO_BE_COMMITTED = "numberOfSegmentsYetToBeCommitted";
  }

  // prefix for scheduler related features, e.g. query accountant
  String PINOT_QUERY_SCHEDULER_PREFIX = "pinot.query.scheduler";

  interface Accounting {
    int ANCHOR_TASK_ID = -1;
    String CONFIG_OF_FACTORY_NAME = "accounting.factory.name";

    String CONFIG_OF_ENABLE_THREAD_CPU_SAMPLING = "accounting.enable.thread.cpu.sampling";
    Boolean DEFAULT_ENABLE_THREAD_CPU_SAMPLING = false;

    String CONFIG_OF_ENABLE_THREAD_MEMORY_SAMPLING = "accounting.enable.thread.memory.sampling";
    Boolean DEFAULT_ENABLE_THREAD_MEMORY_SAMPLING = false;

    String CONFIG_OF_OOM_PROTECTION_KILLING_QUERY = "accounting.oom.enable.killing.query";
    boolean DEFAULT_ENABLE_OOM_PROTECTION_KILLING_QUERY = false;

    String CONFIG_OF_PUBLISHING_JVM_USAGE = "accounting.publishing.jvm.heap.usage";
    boolean DEFAULT_PUBLISHING_JVM_USAGE = false;

    String CONFIG_OF_CPU_TIME_BASED_KILLING_ENABLED = "accounting.cpu.time.based.killing.enabled";
    boolean DEFAULT_CPU_TIME_BASED_KILLING_ENABLED = false;

    String CONFIG_OF_CPU_TIME_BASED_KILLING_THRESHOLD_MS =
        "accounting.cpu.time.based.killing.threshold.ms";
    int DEFAULT_CPU_TIME_BASED_KILLING_THRESHOLD_MS = 30_000;

    String CONFIG_OF_PANIC_LEVEL_HEAP_USAGE_RATIO = "accounting.oom.panic.heap.usage.ratio";
    float DFAULT_PANIC_LEVEL_HEAP_USAGE_RATIO = 0.99f;

    String CONFIG_OF_CRITICAL_LEVEL_HEAP_USAGE_RATIO = "accounting.oom.critical.heap.usage.ratio";
    float DEFAULT_CRITICAL_LEVEL_HEAP_USAGE_RATIO = 0.96f;

    String CONFIG_OF_CRITICAL_LEVEL_HEAP_USAGE_RATIO_DELTA_AFTER_GC =
        "accounting.oom.critical.heap.usage.ratio.delta.after.gc";
    float DEFAULT_CONFIG_OF_CRITICAL_LEVEL_HEAP_USAGE_RATIO_DELTA_AFTER_GC = 0.15f;

    String CONFIG_OF_ALARMING_LEVEL_HEAP_USAGE_RATIO = "accounting.oom.alarming.usage.ratio";
    float DEFAULT_ALARMING_LEVEL_HEAP_USAGE_RATIO = 0.75f;

    String CONFIG_OF_HEAP_USAGE_PUBLISHING_PERIOD_MS = "accounting.heap.usage.publishing.period.ms";
    int DEFAULT_HEAP_USAGE_PUBLISH_PERIOD = 5000;

    String CONFIG_OF_SLEEP_TIME_MS = "accounting.sleep.ms";
    int DEFAULT_SLEEP_TIME_MS = 30;

    String CONFIG_OF_SLEEP_TIME_DENOMINATOR = "accounting.sleep.time.denominator";
    int DEFAULT_SLEEP_TIME_DENOMINATOR = 3;

    String CONFIG_OF_MIN_MEMORY_FOOTPRINT_TO_KILL_RATIO =
        "accounting.min.memory.footprint.to.kill.ratio";
    double DEFAULT_MEMORY_FOOTPRINT_TO_KILL_RATIO = 0.025;

    String CONFIG_OF_GC_BACKOFF_COUNT = "accounting.gc.backoff.count";
    int DEFAULT_GC_BACKOFF_COUNT = 5;

    String CONFIG_OF_GC_WAIT_TIME_MS = "accounting.gc.wait.time.ms";
    int DEFAULT_CONFIG_OF_GC_WAIT_TIME_MS = 0;

    String CONFIG_OF_QUERY_KILLED_METRIC_ENABLED = "accounting.query.killed.metric.enabled";
    boolean DEFAULT_QUERY_KILLED_METRIC_ENABLED = false;

    String CONFIG_OF_CANCEL_CALLBACK_CACHE_MAX_SIZE = "accounting.cancel.callback.cache.max.size";
    int DEFAULT_CANCEL_CALLBACK_CACHE_MAX_SIZE = 500;

    String CONFIG_OF_CANCEL_CALLBACK_CACHE_EXPIRY_SECONDS =
        "accounting.cancel.callback.cache.expiry.seconds";
    int DEFAULT_CANCEL_CALLBACK_CACHE_EXPIRY_SECONDS = 1200;

    String CONFIG_OF_THREAD_SELF_TERMINATE =
        "accounting.thread.self.terminate";
    boolean DEFAULT_THREAD_SELF_TERMINATE = false;

    /**
     * QUERY WORKLOAD ISOLATION Configs
     *
     * This is a set of configs to enable query workload isolation. Queries are classified into workload based on the
     * QueryOption - WORKLOAD_NAME. The CPU and Memory cost for a workload are set globally in ZK. The CPU and memory
     * costs are for a certain time duration, called "enforcementWindow". The workload cost is split into smaller cost
     * for each instance involved in executing queries of the workload.
     *
     *
     * At each instance (broker,server), there are two parts to workload isolation:
     * 1. Workload Cost Collection
     * 2. Workload Cost Enforcement
     *
     *
     * Workload Cost collection happens at various stages of query execution. On server, the resource costs associated
     * with pruning, planning and execution are collected. On broker, the resource costs associated with compilation &
     * reduce are collected. WorkloadBudgetManager maintains the budget and usage for each workload in the instance.
     * Workload Enforcement enforces the budget for a workload if the resource usages are exceeded. The queries in the
     * workload are killed until the enforcementWindow is refreshed.
     *
     * More details in https://tinyurl.com/2p9vuzbd
     *
     * Pre-req configs for enabling Query Workload Isolation:
     *  - CommonConstants.Accounting.CONFIG_OF_FACTORY_NAME  = ResourceUsageAccountantFactory
     *  - CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_CPU_SAMPLING = true
     *  - CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_MEMORY_SAMPLING = true
     *  - CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_SAMPLING_MSE = true
     *  - Instance Config: enableThreadCpuTimeMeasurement = true
     *  - Instance Config: enableThreadAllocatedBytesMeasurement = true
     */

    String CONFIG_OF_WORKLOAD_ENABLE_COST_COLLECTION =
        "accounting.workload.enable.cost.collection";
    boolean DEFAULT_WORKLOAD_ENABLE_COST_COLLECTION = false;

    String CONFIG_OF_WORKLOAD_ENABLE_COST_ENFORCEMENT =
        "accounting.workload.enable.cost.enforcement";
    boolean DEFAULT_WORKLOAD_ENABLE_COST_ENFORCEMENT = false;

    String CONFIG_OF_WORKLOAD_ENFORCEMENT_WINDOW_MS =
        "accounting.workload.enforcement.window.ms";
    long DEFAULT_WORKLOAD_ENFORCEMENT_WINDOW_MS = 60_000L;

    String CONFIG_OF_WORKLOAD_SLEEP_TIME_MS =
        "accounting.workload.sleep.time.ms";
    int DEFAULT_WORKLOAD_SLEEP_TIME_MS = 1;

    String DEFAULT_WORKLOAD_NAME = "default";
    String CONFIG_OF_SECONDARY_WORKLOAD_NAME = "accounting.secondary.workload.name";
    String DEFAULT_SECONDARY_WORKLOAD_NAME = "defaultSecondary";
    String CONFIG_OF_SECONDARY_WORKLOAD_CPU_PERCENTAGE =
        "accounting.secondary.workload.cpu.percentage";
    double DEFAULT_SECONDARY_WORKLOAD_CPU_PERCENTAGE = 0.0;
  }

  interface ExecutorService {
    String PINOT_QUERY_RUNNER_NAME_PREFIX = "pqr-";
    String PINOT_QUERY_RUNNER_NAME_FORMAT = PINOT_QUERY_RUNNER_NAME_PREFIX + "%d";
    String PINOT_QUERY_WORKER_NAME_PREFIX = "pqw-";
    String PINOT_QUERY_WORKER_NAME_FORMAT = PINOT_QUERY_WORKER_NAME_PREFIX + "%d";
  }

  interface Segment {
    interface Realtime {
      enum Status {
        IN_PROGRESS, // The segment is still consuming data
        COMMITTING, // This state will only be utilised by pauseless ingestion when the segment has been consumed but
                    // is yet to be build and uploaded by the server.
        DONE, // The segment has finished consumption and has been committed to the segment store
        UPLOADED; // The segment is uploaded by an external party

        /**
         * Returns {@code true} if the segment is completed (DONE/UPLOADED), {@code false} otherwise.
         *
         * The segment is
         * 1. still Consuming if the status is IN_PROGRESS
         * 2. just done consuming but not yet committed if the status is COMMITTING (for pauseless tables)
         */
        public boolean isCompleted() {
          return (this == DONE) || (this == UPLOADED);
        }
      }

      /**
       * During realtime segment completion, the value of this enum decides how  non-winner servers should replace
       * the completed segment.
       */
      enum CompletionMode {
        // default behavior - if the in memory segment in the non-winner server is equivalent to the committed
        // segment, then build and replace, else download
        DEFAULT, // non-winner servers always download the segment, never build it
        DOWNLOAD
      }

      String STATUS = "segment.realtime.status";
      String START_OFFSET = "segment.realtime.startOffset";
      String END_OFFSET = "segment.realtime.endOffset";
      String NUM_REPLICAS = "segment.realtime.numReplicas";
      String FLUSH_THRESHOLD_SIZE = "segment.flush.threshold.size";
      @Deprecated
      String FLUSH_THRESHOLD_TIME = "segment.flush.threshold.time";

      // Deprecated, but kept for backward-compatibility of reading old segments' ZK metadata
      @Deprecated
      String DOWNLOAD_URL = "segment.realtime.download.url";
    }

    // Deprecated, but kept for backward-compatibility of reading old segments' ZK metadata
    @Deprecated
    interface Offline {
      String DOWNLOAD_URL = "segment.offline.download.url";
      String PUSH_TIME = "segment.offline.push.time";
      String REFRESH_TIME = "segment.offline.refresh.time";
    }

    String START_TIME = "segment.start.time";
    String END_TIME = "segment.end.time";
    String RAW_START_TIME = "segment.start.time.raw";
    String RAW_END_TIME = "segment.end.time.raw";
    String TIME_UNIT = "segment.time.unit";
    String INDEX_VERSION = "segment.index.version";
    String TOTAL_DOCS = "segment.total.docs";
    String CRC = "segment.crc";
    String TIER = "segment.tier";
    String CREATION_TIME = "segment.creation.time";
    String PUSH_TIME = "segment.push.time";
    String REFRESH_TIME = "segment.refresh.time";
    String DOWNLOAD_URL = "segment.download.url";
    String CRYPTER_NAME = "segment.crypter";
    String PARTITION_METADATA = "segment.partition.metadata";
    String CUSTOM_MAP = "custom.map";
    String SIZE_IN_BYTES = "segment.size.in.bytes";

    /**
     * This field is used for parallel push protection to lock the segment globally.
     * We put the segment upload start timestamp so that if the previous push failed without unlock the segment, the
     * next upload won't be blocked forever.
     */
    String SEGMENT_UPLOAD_START_TIME = "segment.upload.start.time";

    String SEGMENT_BACKUP_DIR_SUFFIX = ".segment.bak";
    String SEGMENT_TEMP_DIR_SUFFIX = ".segment.tmp";

    String LOCAL_SEGMENT_SCHEME = "file";
    String PEER_SEGMENT_DOWNLOAD_SCHEME = "peer://";
    String METADATA_URI_FOR_PEER_DOWNLOAD = "";

    interface AssignmentStrategy {
      String BALANCE_NUM_SEGMENT_ASSIGNMENT_STRATEGY = "balanced";
      String REPLICA_GROUP_SEGMENT_ASSIGNMENT_STRATEGY = "replicagroup";
      String DIM_TABLE_SEGMENT_ASSIGNMENT_STRATEGY = "allservers";
    }

    interface BuiltInVirtualColumn {
      String DOCID = "$docId";
      String HOSTNAME = "$hostName";
      String SEGMENTNAME = "$segmentName";
      Set<String> BUILT_IN_VIRTUAL_COLUMNS = Set.of(DOCID, HOSTNAME, SEGMENTNAME);
    }
  }

  interface Tier {
    String BACKEND_PROP_DATA_DIR = "dataDir";
  }

  interface Explain {
    interface Response {
      interface ServerResponseStatus {
        String STATUS_ERROR = "ERROR";
        String STATUS_OK = "OK";
      }
    }
  }

  interface Query {

    /**
     * Configuration keys for query context mode.
     *
     * Valid values are 'strict' (ignoring case) or empty.
     *
     * In strict mode, if the {@link QueryThreadContext} is not initialized, an {@link IllegalStateException} will be
     * thrown when setter and getter methods are used. Otherwise a warning will be logged and the fake instance will be
     * returned.
     */
    String CONFIG_OF_QUERY_CONTEXT_MODE = "pinot.query.context.mode";

    interface Request {
      interface MetadataKeys {
        /// This is the request id, which may change during the execution.
        ///
        /// See [QueryThreadContext#getRequestId()] for more details.
        String REQUEST_ID = "requestId";
        /// Ths is the correlation id, which is set when the query starts and will not change during the execution.
        /// This value is either set by the client or generated by the broker, in which case it will be equal to the
        /// original request id.
        ///
        /// See [QueryThreadContext#getCid()] for more details.
        String CORRELATION_ID = "correlationId";
        String BROKER_ID = "brokerId";
        String ENABLE_TRACE = "enableTrace";
        String ENABLE_STREAMING = "enableStreaming";
        String PAYLOAD_TYPE = "payloadType";
      }

      interface PayloadType {
        String SQL = "sql";
        String BROKER_REQUEST = "brokerRequest";
      }
    }

    interface Response {
      interface MetadataKeys {
        String RESPONSE_TYPE = "responseType";
      }

      interface ResponseType {
        // For streaming response, multiple (could be 0 if no data should be returned, or query encounters exception)
        // data responses will be returned, followed by one single metadata response
        String DATA = "data";
        String METADATA = "metadata";
        // For non-streaming response
        String NON_STREAMING = "nonStreaming";
      }

      /**
       * Configuration keys for {@link org.apache.pinot.common.proto.Worker.QueryResponse} extra metadata.
       */
      interface ServerResponseStatus {
        String STATUS_ERROR = "ERROR";
        String STATUS_OK = "OK";
      }
    }

    interface OptimizationConstants {
      int DEFAULT_AVG_MV_ENTRIES_DENOMINATOR = 2;
    }

    interface Range {
      char DELIMITER = '\0';
      char LOWER_EXCLUSIVE = '(';
      char LOWER_INCLUSIVE = '[';
      char UPPER_EXCLUSIVE = ')';
      char UPPER_INCLUSIVE = ']';
      String UNBOUNDED = "*";
      String LOWER_UNBOUNDED = LOWER_EXCLUSIVE + UNBOUNDED + DELIMITER;
      String UPPER_UNBOUNDED = DELIMITER + UNBOUNDED + UPPER_EXCLUSIVE;
    }
  }

  interface IdealState {
    String HYBRID_TABLE_TIME_BOUNDARY = "HYBRID_TABLE_TIME_BOUNDARY";
  }

  interface RewriterConstants {
    String PARENT_AGGREGATION_NAME_PREFIX = "pinotparentagg";
    String CHILD_AGGREGATION_NAME_PREFIX = "pinotchildagg";
    String CHILD_AGGREGATION_SEPERATOR = "@";
    String CHILD_KEY_SEPERATOR = "_";
  }

  /**
   * Configuration for setting up multi-stage query runner, this service could be running on either broker or server.
   */
  interface MultiStageQueryRunner {
    /**
     * Configuration for mailbox data block size
     */
    String KEY_OF_MAX_INBOUND_QUERY_DATA_BLOCK_SIZE_BYTES = "pinot.query.runner.max.msg.size.bytes";
    int DEFAULT_MAX_INBOUND_QUERY_DATA_BLOCK_SIZE_BYTES = 16 * 1024 * 1024;

    /**
     * Enable splitting of data block payload during mailbox transfer.
     */
    String KEY_OF_ENABLE_DATA_BLOCK_PAYLOAD_SPLIT =
        "pinot.query.runner.enable.data.block.payload.split";
    boolean DEFAULT_ENABLE_DATA_BLOCK_PAYLOAD_SPLIT = false;

    /**
     * Configuration for server port, port that opens and accepts
     * {@link org.apache.pinot.query.runtime.plan.DistributedStagePlan} and start executing query stages.
     */
    String KEY_OF_QUERY_SERVER_PORT = "pinot.query.server.port";
    int DEFAULT_QUERY_SERVER_PORT = 0;

    /**
     * Configuration for mailbox hostname and port, this hostname and port opens streaming channel to receive
     * {@link org.apache.pinot.common.datablock.DataBlock}.
     */
    String KEY_OF_QUERY_RUNNER_HOSTNAME = "pinot.query.runner.hostname";
    String KEY_OF_QUERY_RUNNER_PORT = "pinot.query.runner.port";
    int DEFAULT_QUERY_RUNNER_PORT = 0;

    /**
     * Configuration for join overflow.
     */
    String KEY_OF_MAX_ROWS_IN_JOIN = "pinot.query.join.max.rows";
    String KEY_OF_JOIN_OVERFLOW_MODE = "pinot.query.join.overflow.mode";

    /// Specifies the send stats mode used in MSE.
    ///
    /// Valid values are (in lower or upper case):
    /// - "SAFE": MSE will only send stats if all instances in the cluster are running 1.4.0 or later.
    /// - "ALWAYS": MSE will always send stats, regardless of the version of the instances in the cluster.
    /// - "NEVER": MSE will never send stats.
    ///
    /// The reason for this flag that versions 1.3.0 and lower have two undesired behaviors:
    /// 1. Some queries using intersection generate incorrect stats
    /// 2. When stats from other nodes are sent but are different from expected, the query fails.
    ///
    /// In 1.4.0 the first issue is solved and instead of failing when unexpected stats are received, the query
    /// continues without children stats. But if a query involves servers in versions 1.3.0 and 1.4.0, the one
    /// running 1.3.0 may fail, which breaks backward compatibility.
    String KEY_OF_SEND_STATS_MODE = "pinot.query.mse.stats.mode";
    String DEFAULT_SEND_STATS_MODE = "SAFE";

    /// Used to indicate that MSE stats should be logged at INFO level for successful queries.
    ///
    /// When an MSE query is executed, the stats are collected and logged.
    /// By default, successful queries are logged in the DEBUG level, while errors are logged in the INFO level.
    /// But if this property is set to true (upper or lower case), stats will be logged in the INFO level for both
    /// successful queries and errors.
    String KEY_OF_LOG_STATS = "logStats";

    enum JoinOverFlowMode {
      THROW, BREAK
    }

    /**
     * Configuration for window overflow.
     */
    String KEY_OF_MAX_ROWS_IN_WINDOW = "pinot.query.window.max.rows";
    String KEY_OF_WINDOW_OVERFLOW_MODE = "pinot.query.window.overflow.mode";

    enum WindowOverFlowMode {
      THROW, BREAK
    }

    /**
     * Constants related to plan versions.
     */
    interface PlanVersions {
      int V1 = 1;
    }

    String KEY_OF_MULTISTAGE_EXPLAIN_INCLUDE_SEGMENT_PLAN =
        "pinot.query.multistage.explain.include.segment.plan";
    boolean DEFAULT_OF_MULTISTAGE_EXPLAIN_INCLUDE_SEGMENT_PLAN = false;

    /// Max number of rows operators stored in the op stats cache.
    /// Although the cache stores stages, each entry has a weight equal to the number of operators in the stage.
    String KEY_OF_OP_STATS_CACHE_SIZE = "pinot.server.query.op.stats.cache.size";
    int DEFAULT_OF_OP_STATS_CACHE_SIZE = 1000;

    /// Max time to keep the op stats in the cache.
    String KEY_OF_OP_STATS_CACHE_EXPIRE_MS = "pinot.server.query.op.stats.cache.ms";
    int DEFAULT_OF_OP_STATS_CACHE_EXPIRE_MS = 60 * 1000;
    /// Timeout of the cancel request, in milliseconds.
    String KEY_OF_CANCEL_TIMEOUT_MS = "pinot.server.query.cancel.timeout.ms";
    long DEFAULT_OF_CANCEL_TIMEOUT_MS = 1000;
  }

  interface NullValuePlaceHolder {
    int INT = 0;
    long LONG = 0L;
    float FLOAT = 0f;
    double DOUBLE = 0d;
    BigDecimal BIG_DECIMAL = BigDecimal.ZERO;
    String STRING = "";
    byte[] BYTES = new byte[0];
    ByteArray INTERNAL_BYTES = new ByteArray(BYTES);
    int[] INT_ARRAY = new int[0];
    long[] LONG_ARRAY = new long[0];
    float[] FLOAT_ARRAY = new float[0];
    double[] DOUBLE_ARRAY = new double[0];
    String[] STRING_ARRAY = new String[0];
    byte[][] BYTES_ARRAY = new byte[0][];
    Object MAP = Collections.emptyMap();
  }

  interface CursorConfigs {
    String PREFIX_OF_CONFIG_OF_CURSOR = "pinot.broker.cursor";
    String PREFIX_OF_CONFIG_OF_RESPONSE_STORE = "pinot.broker.cursor.response.store";
    String DEFAULT_RESPONSE_STORE_TYPE = "file";
    String RESPONSE_STORE_TYPE = "type";
    int DEFAULT_CURSOR_FETCH_ROWS = 10000;
    String CURSOR_FETCH_ROWS = PREFIX_OF_CONFIG_OF_CURSOR + ".fetch.rows";
    String DEFAULT_RESULTS_EXPIRATION_INTERVAL = "1h"; // 1 hour.
    String RESULTS_EXPIRATION_INTERVAL = PREFIX_OF_CONFIG_OF_RESPONSE_STORE + ".expiration";

    String RESPONSE_STORE_CLEANER_FREQUENCY_PERIOD =
        "controller.cluster.response.store.cleaner.frequencyPeriod";
    String DEFAULT_RESPONSE_STORE_CLEANER_FREQUENCY_PERIOD = "1h";
    String RESPONSE_STORE_CLEANER_INITIAL_DELAY =
        "controller.cluster.response.store.cleaner.initialDelay";
  }

  interface ForwardIndexConfigs {
    String CONFIG_OF_DEFAULT_RAW_INDEX_WRITER_VERSION =
        "pinot.forward.index.default.raw.index.writer.version";
    String CONFIG_OF_DEFAULT_TARGET_MAX_CHUNK_SIZE =
        "pinot.forward.index.default.target.max.chunk.size";
    String CONFIG_OF_DEFAULT_TARGET_DOCS_PER_CHUNK =
        "pinot.forward.index.default.target.docs.per.chunk";
  }

  interface FieldSpecConfigs {
    String CONFIG_OF_DEFAULT_JSON_MAX_LENGTH_EXCEED_STRATEGY =
        "pinot.field.spec.default.json.max.length.exceed.strategy";
    String CONFIG_OF_DEFAULT_JSON_MAX_LENGTH =
        "pinot.field.spec.default.json.max.length";
  }

  /**
   * Configuration for setting up groovy static analyzer.
   * User can config different configuration for query and ingestion (table creation and update) static analyzer.
   * The all configuration is the default configuration for both query and ingestion static analyzer.
   */
  interface Groovy {
    String GROOVY_ALL_STATIC_ANALYZER_CONFIG = "pinot.groovy.all.static.analyzer";
    String GROOVY_QUERY_STATIC_ANALYZER_CONFIG = "pinot.groovy.query.static.analyzer";
    String GROOVY_INGESTION_STATIC_ANALYZER_CONFIG = "pinot.groovy.ingestion.static.analyzer";
  }

  /**
   * ZK paths used by Pinot.
   */
  interface ZkPaths {
    String LOGICAL_TABLE_PARENT_PATH = "/LOGICAL/TABLE";
    String LOGICAL_TABLE_PATH_PREFIX = "/LOGICAL/TABLE/";
    String TABLE_CONFIG_PATH_PREFIX = "/CONFIGS/TABLE/";
    String SCHEMA_PATH_PREFIX = "/SCHEMAS/";
  }
}
