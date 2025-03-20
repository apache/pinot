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
package org.apache.pinot.controller;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.controller.rebalancer.strategy.AutoRebalanceStrategy;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.controller.helix.core.rebalance.RebalanceConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.LocalPinotFS;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.TimeUtils;

import static org.apache.pinot.spi.utils.CommonConstants.Controller.CONFIG_OF_CONTROLLER_METRICS_PREFIX;
import static org.apache.pinot.spi.utils.CommonConstants.Controller.CONFIG_OF_INSTANCE_ID;
import static org.apache.pinot.spi.utils.CommonConstants.Controller.DEFAULT_METRICS_PREFIX;


public class ControllerConf extends PinotConfiguration {
  public static final List<String> SUPPORTED_PROTOCOLS =
      Arrays.asList(CommonConstants.HTTP_PROTOCOL, CommonConstants.HTTPS_PROTOCOL);

  public static final String CONTROLLER_VIP_HOST = "controller.vip.host";
  public static final String CONTROLLER_VIP_PORT = "controller.vip.port";
  public static final String CONTROLLER_VIP_PROTOCOL = "controller.vip.protocol";
  public static final String CONTROLLER_BROKER_PROTOCOL = "controller.broker.protocol";
  public static final String CONTROLLER_BROKER_PORT_OVERRIDE = "controller.broker.port.override";
  public static final String CONTROLLER_BROKER_TLS_PREFIX = "controller.broker.tls";
  public static final String CONTROLLER_BROKER_AUTH_PREFIX = "controller.broker.auth";
  public static final String CONTROLLER_TLS_PREFIX = "controller.tls";
  public static final String CONTROLLER_HOST = "controller.host";
  public static final String CONTROLLER_PORT = "controller.port";
  public static final String CONTROLLER_ACCESS_PROTOCOLS = "controller.access.protocols";
  public static final String DATA_DIR = "controller.data.dir";
  // Potentially same as data dir if local
  public static final String LOCAL_TEMP_DIR = "controller.local.temp.dir";
  public static final String ZK_STR = "controller.zk.str";
  // boolean: Update the statemodel on boot?
  public static final String UPDATE_SEGMENT_STATE_MODEL = "controller.update_segment_state_model";
  public static final String MIN_NUM_CHARS_IN_IS_TO_TURN_ON_COMPRESSION = "controller.min_is_size_for_compression";
  public static final String HELIX_CLUSTER_NAME = "controller.helix.cluster.name";
  public static final String CLUSTER_TENANT_ISOLATION_ENABLE = "cluster.tenant.isolation.enable";
  public static final String CONSOLE_WEBAPP_ROOT_PATH = "controller.query.console";
  public static final String CONSOLE_SWAGGER_ENABLE = "controller.swagger.enable";
  public static final String CONSOLE_SWAGGER_USE_HTTPS = "controller.swagger.use.https";
  public static final String CONTROLLER_MODE = "controller.mode";
  public static final String LEAD_CONTROLLER_RESOURCE_REBALANCE_STRATEGY = "controller.resource.rebalance.strategy";
  public static final String LEAD_CONTROLLER_RESOURCE_REBALANCE_DELAY_MS = "controller.resource.rebalance.delay_ms";

  // Comma separated list of packages that contain TableConfigTuners to be added to the registry
  public static final String TABLE_CONFIG_TUNER_PACKAGES = "controller.table.config.tuner.packages";
  public static final String DEFAULT_TABLE_CONFIG_TUNER_PACKAGES = "org.apache.pinot";

  // Comma separated list of packages that contains javax service resources.
  public static final String CONTROLLER_RESOURCE_PACKAGES = "controller.restlet.api.resource.packages";
  public static final String DEFAULT_CONTROLLER_RESOURCE_PACKAGES = "org.apache.pinot.controller.api.resources";

  // Consider tierConfigs when assigning new offline segment
  public static final String CONTROLLER_ENABLE_TIERED_SEGMENT_ASSIGNMENT = "controller.segment.enableTieredAssignment";

  public enum ControllerMode {
    DUAL, PINOT_ONLY, HELIX_ONLY
  }

  public static class ControllerPeriodicTasksConf {
    // frequency configs
    // Deprecated as of 0.8.0
    @Deprecated
    public static final String DEPRECATED_RETENTION_MANAGER_FREQUENCY_IN_SECONDS =
        "controller.retention.frequencyInSeconds";
    public static final String RETENTION_MANAGER_FREQUENCY_PERIOD = "controller.retention.frequencyPeriod";
    // Deprecated as of 0.8.0
    @Deprecated
    public static final String DEPRECATED_OFFLINE_SEGMENT_INTERVAL_CHECKER_FREQUENCY_IN_SECONDS =
        "controller.offline.segment.interval.checker.frequencyInSeconds";
    public static final String OFFLINE_SEGMENT_INTERVAL_CHECKER_FREQUENCY_PERIOD =
        "controller.offline.segment.interval.checker.frequencyPeriod";
    // Deprecated as of 0.8.0
    @Deprecated
    public static final String DEPRECATED_REALTIME_SEGMENT_VALIDATION_FREQUENCY_IN_SECONDS =
        "controller.realtime.segment.validation.frequencyInSeconds";
    public static final String REALTIME_SEGMENT_VALIDATION_FREQUENCY_PERIOD =
        "controller.realtime.segment.validation.frequencyPeriod";
    public static final String REALTIME_SEGMENT_VALIDATION_INITIAL_DELAY_IN_SECONDS =
        "controller.realtime.segment.validation.initialDelayInSeconds";
    // Deprecated as of 0.8.0
    @Deprecated
    public static final String DEPRECATED_BROKER_RESOURCE_VALIDATION_FREQUENCY_IN_SECONDS =
        "controller.broker.resource.validation.frequencyInSeconds";
    public static final String BROKER_RESOURCE_VALIDATION_FREQUENCY_PERIOD =
        "controller.broker.resource.validation.frequencyPeriod";
    public static final String BROKER_RESOURCE_VALIDATION_INITIAL_DELAY_IN_SECONDS =
        "controller.broker.resource.validation.initialDelayInSeconds";
    // Deprecated as of 0.8.0
    @Deprecated
    public static final String DEPRECATED_STATUS_CHECKER_FREQUENCY_IN_SECONDS =
        "controller.statuschecker.frequencyInSeconds";
    public static final String STATUS_CHECKER_FREQUENCY_PERIOD = "controller.statuschecker.frequencyPeriod";
    // Deprecated as of 0.8.0
    @Deprecated
    public static final String DEPRECATED_STATUS_CHECKER_WAIT_FOR_PUSH_TIME_IN_SECONDS =
        "controller.statuschecker.waitForPushTimeInSeconds";
    public static final String STATUS_CHECKER_WAIT_FOR_PUSH_TIME_PERIOD =
        "controller.statuschecker.waitForPushTimePeriod";
    // Deprecated as of 0.8.0
    @Deprecated
    public static final String DEPRECATED_TASK_MANAGER_FREQUENCY_IN_SECONDS = "controller.task.frequencyInSeconds";
    public static final String TASK_MANAGER_FREQUENCY_PERIOD = "controller.task.frequencyPeriod";
    public static final String TASK_MANAGER_SKIP_LATE_CRON_SCHEDULE = "controller.task.skipLateCronSchedule";
    public static final String TASK_MANAGER_MAX_CRON_SCHEDULE_DELAY_IN_SECONDS =
        "controller.task.maxCronScheduleDelayInSeconds";
    // Deprecated as of 0.8.0
    @Deprecated
    public static final String DEPRECATED_MINION_INSTANCES_CLEANUP_TASK_FREQUENCY_IN_SECONDS =
        "controller.minion.instances.cleanup.task.frequencyInSeconds";
    @Deprecated
    public static final String MINION_INSTANCES_CLEANUP_TASK_FREQUENCY_PERIOD =
        "controller.minion.instances.cleanup.task.frequencyPeriod";
    @Deprecated
    public static final String MINION_INSTANCES_CLEANUP_TASK_INITIAL_DELAY_SECONDS =
        "controller.minion.instances.cleanup.task.initialDelaySeconds";
    // Deprecated as of 0.8.0
    @Deprecated
    public static final String DEPRECATED_MINION_INSTANCES_CLEANUP_TASK_MIN_OFFLINE_TIME_BEFORE_DELETION_SECONDS =
        "controller.minion.instances.cleanup.task.minOfflineTimeBeforeDeletionSeconds";
    @Deprecated
    public static final String MINION_INSTANCES_CLEANUP_TASK_MIN_OFFLINE_TIME_BEFORE_DELETION_PERIOD =
        "controller.minion.instances.cleanup.task.minOfflineTimeBeforeDeletionPeriod";

    public static final String STALE_INSTANCES_CLEANUP_TASK_FREQUENCY_PERIOD =
        "controller.stale.instances.cleanup.task.frequencyPeriod";
    public static final String STALE_INSTANCES_CLEANUP_TASK_INITIAL_DELAY_SECONDS =
        "controller.stale.instances.cleanup.task.initialDelaySeconds";
    public static final String STALE_INSTANCES_CLEANUP_TASK_INSTANCES_RETENTION_PERIOD =
        "controller.stale.instances.cleanup.task.minOfflineTimeBeforeDeletionPeriod";

    // Deprecated as of 0.8.0
    @Deprecated
    public static final String DEPRECATED_TASK_METRICS_EMITTER_FREQUENCY_IN_SECONDS =
        "controller.minion.task.metrics.emitter.frequencyInSeconds";
    public static final String TASK_METRICS_EMITTER_FREQUENCY_PERIOD =
        "controller.minion.task.metrics.emitter.frequencyPeriod";

    public static final String PINOT_TASK_MANAGER_SCHEDULER_ENABLED = "controller.task.scheduler.enabled";
    // This is the expiry for the ended tasks. Helix cleans up the task info from ZK after the expiry time from the
    // end of the task.
    public static final String PINOT_TASK_EXPIRE_TIME_MS = "controller.task.expire.time.ms";

    @Deprecated
    // RealtimeSegmentRelocator has been rebranded as SegmentRelocator
    public static final String DEPRECATED_REALTIME_SEGMENT_RELOCATOR_FREQUENCY =
        "controller.realtime.segment.relocator.frequency";
    // Deprecated as of 0.8.0
    @Deprecated
    public static final String DEPRECATED_SEGMENT_RELOCATOR_FREQUENCY_IN_SECONDS =
        "controller.segment.relocator.frequencyInSeconds";
    public static final String SEGMENT_RELOCATOR_FREQUENCY_PERIOD = "controller.segment.relocator.frequencyPeriod";

    public static final String SEGMENT_RELOCATOR_REASSIGN_INSTANCES = "controller.segment.relocator.reassignInstances";
    public static final String SEGMENT_RELOCATOR_BOOTSTRAP = "controller.segment.relocator.bootstrap";
    public static final String SEGMENT_RELOCATOR_DOWNTIME = "controller.segment.relocator.downtime";
    // For no-downtime rebalance, minimum number of replicas to keep alive during rebalance, or maximum number of
    // replicas allowed to be unavailable if value is negative. Default value is -1 (only allowing one replica down).
    public static final String SEGMENT_RELOCATOR_MIN_AVAILABLE_REPLICAS =
        "controller.segment.relocator.minAvailableReplicas";
    // Whether segment relocator should do a best-efforts rebalance. Default is 'true'.
    public static final String SEGMENT_RELOCATOR_BEST_EFFORTS = "controller.segment.relocator.bestEfforts";
    public static final String SEGMENT_RELOCATOR_EXTERNAL_VIEW_CHECK_INTERVAL_IN_MS =
        "controller.segmentRelocator.externalViewCheckIntervalInMs";
    public static final String SEGMENT_RELOCATOR_EXTERNAL_VIEW_STABILIZATION_TIMEOUT_IN_MS =
        "controller.segmentRelocator.externalViewStabilizationTimeoutInMs";
    public static final String SEGMENT_RELOCATOR_ENABLE_LOCAL_TIER_MIGRATION =
        "controller.segmentRelocator.enableLocalTierMigration";
    public static final String SEGMENT_RELOCATOR_REBALANCE_TABLES_SEQUENTIALLY =
        "controller.segmentRelocator.rebalanceTablesSequentially";

    public static final String REBALANCE_CHECKER_FREQUENCY_PERIOD = "controller.rebalance.checker.frequencyPeriod";
    // Because segment level validation is expensive and requires heavy ZK access, we run segment level validation
    // with a separate interval
    // Deprecated as of 0.8.0
    @Deprecated
    public static final String DEPRECATED_SEGMENT_LEVEL_VALIDATION_INTERVAL_IN_SECONDS =
        "controller.segment.level.validation.intervalInSeconds";
    public static final String SEGMENT_LEVEL_VALIDATION_INTERVAL_PERIOD =
        "controller.segment.level.validation.intervalPeriod";
    public static final String AUTO_RESET_ERROR_SEGMENTS_VALIDATION =
        "controller.segment.error.autoReset";

    // Initial delays
    public static final String STATUS_CHECKER_INITIAL_DELAY_IN_SECONDS =
        "controller.statusChecker.initialDelayInSeconds";
    public static final String RETENTION_MANAGER_INITIAL_DELAY_IN_SECONDS =
        "controller.retentionManager.initialDelayInSeconds";
    public static final String OFFLINE_SEGMENT_INTERVAL_CHECKER_INITIAL_DELAY_IN_SECONDS =
        "controller.offlineSegmentIntervalChecker.initialDelayInSeconds";
    @Deprecated
    // RealtimeSegmentRelocator has been rebranded as SegmentRelocator
    public static final String DEPRECATED_REALTIME_SEGMENT_RELOCATION_INITIAL_DELAY_IN_SECONDS =
        "controller.realtimeSegmentRelocation.initialDelayInSeconds";
    public static final String SEGMENT_RELOCATOR_INITIAL_DELAY_IN_SECONDS =
        "controller.segmentRelocator.initialDelayInSeconds";
    public static final String REBALANCE_CHECKER_INITIAL_DELAY_IN_SECONDS =
        "controller.rebalanceChecker.initialDelayInSeconds";

    // The flag to indicate if controller periodic job will fix the missing LLC segment deep store copy.
    // Default value is false.
    public static final String ENABLE_DEEP_STORE_RETRY_UPLOAD_LLC_SEGMENT =
        "controller.realtime.segment.deepStoreUploadRetryEnabled";
    public static final String DEEP_STORE_RETRY_UPLOAD_TIMEOUT_MS =
        "controller.realtime.segment.deepStoreUploadRetry.timeoutMs";
    public static final String DEEP_STORE_RETRY_UPLOAD_PARALLELISM =
        "controller.realtime.segment.deepStoreUploadRetry.parallelism";
    public static final String ENABLE_TMP_SEGMENT_ASYNC_DELETION =
        "controller.realtime.segment.tmpFileAsyncDeletionEnabled";
    // temporary segments within expiration won't be deleted so that ongoing split commit won't be impacted.
    public static final String TMP_SEGMENT_RETENTION_IN_SECONDS =
        "controller.realtime.segment.tmpFileRetentionInSeconds";

    // Enables the deletion of untracked segments during the retention manager run.
    // Untracked segments are those that exist in deep store but have no corresponding entry in the ZK property store.
    public static final String ENABLE_UNTRACKED_SEGMENT_DELETION =
        "controller.retentionManager.untrackedSegmentDeletionEnabled";
    public static final int MIN_INITIAL_DELAY_IN_SECONDS = 120;
    public static final int MAX_INITIAL_DELAY_IN_SECONDS = 300;
    public static final int DEFAULT_SPLIT_COMMIT_TMP_SEGMENT_LIFETIME_SECOND = 60 * 60; // 1 Hour.

    private static final Random RANDOM = new Random();

    private static long getRandomInitialDelayInSeconds() {
      return MIN_INITIAL_DELAY_IN_SECONDS + RANDOM.nextInt(MAX_INITIAL_DELAY_IN_SECONDS - MIN_INITIAL_DELAY_IN_SECONDS);
    }

    // Default values
    private static final int DEFAULT_RETENTION_MANAGER_FREQUENCY_IN_SECONDS = 6 * 60 * 60; // 6 Hours.
    private static final int DEFAULT_OFFLINE_SEGMENT_INTERVAL_CHECKER_FREQUENCY_IN_SECONDS = 24 * 60 * 60; // 24 Hours.
    private static final int DEFAULT_REALTIME_SEGMENT_VALIDATION_FREQUENCY_IN_SECONDS = 60 * 60; // 1 Hour.
    private static final int DEFAULT_BROKER_RESOURCE_VALIDATION_FREQUENCY_IN_SECONDS = 60 * 60; // 1 Hour.
    private static final int DEFAULT_STATUS_CHECKER_FREQUENCY_IN_SECONDS = 5 * 60; // 5 minutes
    private static final int DEFAULT_REBALANCE_CHECKER_FREQUENCY_IN_SECONDS = 5 * 60; // 5 minutes
    private static final int DEFAULT_TASK_METRICS_EMITTER_FREQUENCY_IN_SECONDS = 5 * 60; // 5 minutes
    private static final int DEFAULT_STATUS_CONTROLLER_WAIT_FOR_PUSH_TIME_IN_SECONDS = 10 * 60; // 10 minutes
    private static final int DEFAULT_TASK_MANAGER_FREQUENCY_IN_SECONDS = -1; // Disabled
    @Deprecated
    private static final int DEFAULT_MINION_INSTANCES_CLEANUP_TASK_FREQUENCY_IN_SECONDS = 60 * 60; // 1 Hour.
    @Deprecated
    private static final int DEFAULT_MINION_INSTANCES_CLEANUP_TASK_MIN_OFFLINE_TIME_BEFORE_DELETION_IN_SECONDS =
        60 * 60; // 1 Hour.

    private static final int DEFAULT_SEGMENT_LEVEL_VALIDATION_INTERVAL_IN_SECONDS = 24 * 60 * 60;
    private static final int DEFAULT_SEGMENT_RELOCATOR_FREQUENCY_IN_SECONDS = 60 * 60;

    // Realtime Consumer Monitor
    private static final String RT_CONSUMER_MONITOR_FREQUENCY_PERIOD =
        "controller.realtimeConsumerMonitor.frequencyPeriod";
    private static final String RT_CONSUMER_MONITOR_INITIAL_DELAY_IN_SECONDS =
        "controller.realtimeConsumerMonitor.initialDelayInSeconds";

    private static final int DEFAULT_RT_CONSUMER_MONITOR_FREQUENCY_IN_SECONDS = -1; // Disabled by default
  }

  private static final String SERVER_ADMIN_REQUEST_TIMEOUT_SECONDS = "server.request.timeoutSeconds";
  private static final String MINION_ADMIN_REQUEST_TIMEOUT_SECONDS = "minion.request.timeoutSeconds";
  private static final String SEGMENT_COMMIT_TIMEOUT_SECONDS = "controller.realtime.segment.commit.timeoutSeconds";
  private static final String CONTROLLER_EXECUTOR_NUM_THREADS = "controller.executor.numThreads";
  public static final String CONTROLLER_EXECUTOR_REBALANCE_NUM_THREADS = "controller.executor.rebalance.numThreads";

  private static final String DELETED_SEGMENTS_RETENTION_IN_DAYS = "controller.deleted.segments.retentionInDays";
  public static final String TABLE_MIN_REPLICAS = "table.minReplicas";
  private static final String JERSEY_ADMIN_API_PORT = "jersey.admin.api.port";
  private static final String JERSEY_ADMIN_IS_PRIMARY = "jersey.admin.isprimary";
  public static final String ACCESS_CONTROL_FACTORY_CLASS = "controller.admin.access.control.factory.class";
  public static final String ACCESS_CONTROL_USERNAME = "access.control.init.username";
  public static final String ACCESS_CONTROL_PASSWORD = "access.control.init.password";
  public static final String LINEAGE_MANAGER_CLASS = "controller.lineage.manager.class";
  public static final String REBALANCE_PRE_CHECKER_CLASS = "controller.rebalance.pre.checker.class";
  // Amount of the time the segment can take from the beginning of upload to the end of upload. Used when parallel push
  // protection is enabled. If the upload does not finish within the timeout, next upload can override the previous one.
  private static final String SEGMENT_UPLOAD_TIMEOUT_IN_MILLIS = "controller.segment.upload.timeoutInMillis";
  private static final String REALTIME_SEGMENT_METADATA_COMMIT_NUMLOCKS =
      "controller.realtime.segment.metadata.commit.numLocks";
  private static final String ENABLE_STORAGE_QUOTA_CHECK = "controller.enable.storage.quota.check";
  private static final String REBALANCE_DISK_UTILIZATION_THRESHOLD = "controller.rebalance.disk.utilization.threshold";
  private static final String DISK_UTILIZATION_THRESHOLD = "controller.disk.utilization.threshold"; // 0 < threshold < 1
  private static final String DISK_UTILIZATION_CHECK_TIMEOUT_MS = "controller.disk.utilization.check.timeoutMs";
  private static final String DISK_UTILIZATION_PATH = "controller.disk.utilization.path";
  private static final String ENABLE_RESOURCE_UTILIZATION_CHECK = "controller.enable.resource.utilization.check";
  public static final String RESOURCE_UTILIZATION_CHECKER_INITIAL_DELAY =
      "controller.resource.utilization.checker.initial.delay";
  private static final String RESOURCE_UTILIZATION_CHECKER_FREQUENCY =
      "controller.resource.utilization.checker.frequency";
  private static final String ENABLE_BATCH_MESSAGE_MODE = "controller.enable.batch.message.mode";
  public static final String DIM_TABLE_MAX_SIZE = "controller.dimTable.maxSize";

  // Defines the kind of storage and the underlying PinotFS implementation
  private static final String PINOT_FS_FACTORY_CLASS_LOCAL = "controller.storage.factory.class.file";

  private static final int DEFAULT_SERVER_ADMIN_REQUEST_TIMEOUT_SECONDS = 30;
  private static final int DEFAULT_MINION_ADMIN_REQUEST_TIMEOUT_SECONDS = 30;
  private static final int DEFAULT_DELETED_SEGMENTS_RETENTION_IN_DAYS = 7;
  private static final int DEFAULT_TABLE_MIN_REPLICAS = 1;
  private static final int DEFAULT_JERSEY_ADMIN_PORT = 21000;
  private static final String DEFAULT_ACCESS_CONTROL_FACTORY_CLASS =
      "org.apache.pinot.controller.api.access.AllowAllAccessFactory";
  private static final String DEFAULT_ACCESS_CONTROL_USERNAME = "admin";
  private static final String DEFAULT_ACCESS_CONTROL_PASSWORD = "admin";
  private static final String DEFAULT_LINEAGE_MANAGER =
      "org.apache.pinot.controller.helix.core.lineage.DefaultLineageManager";
  private static final String DEFAULT_REBALANCE_PRE_CHECKER =
      "org.apache.pinot.controller.helix.core.rebalance.DefaultRebalancePreChecker";
  private static final long DEFAULT_SEGMENT_UPLOAD_TIMEOUT_IN_MILLIS = 600_000L; // 10 minutes
  private static final int DEFAULT_MIN_NUM_CHARS_IN_IS_TO_TURN_ON_COMPRESSION = -1;
  private static final int DEFAULT_REALTIME_SEGMENT_METADATA_COMMIT_NUMLOCKS = 64;
  private static final boolean DEFAULT_ENABLE_STORAGE_QUOTA_CHECK = true;
  private static final double DEFAULT_REBALANCE_DISK_UTILIZATION_THRESHOLD = 0.9;
  private static final double DEFAULT_DISK_UTILIZATION_THRESHOLD = 0.95;
  private static final int DEFAULT_DISK_UTILIZATION_CHECK_TIMEOUT_MS = 30_000;
  private static final String DEFAULT_DISK_UTILIZATION_PATH = "/home/pinot/data";
  private static final boolean DEFAULT_ENABLE_RESOURCE_UTILIZATION_CHECK = false;
  private static final long DEFAULT_RESOURCE_UTILIZATION_CHECKER_INITIAL_DELAY = 300L; // 5 minutes
  private static final long DEFAULT_RESOURCE_UTILIZATION_CHECKER_FREQUENCY = 300L; // 5 minutes
  private static final boolean DEFAULT_ENABLE_BATCH_MESSAGE_MODE = false;
  // Disallow any high level consumer (HLC) table
  private static final boolean DEFAULT_ALLOW_HLC_TABLES = false;
  private static final String DEFAULT_CONTROLLER_MODE = ControllerMode.DUAL.name();
  private static final String DEFAULT_LEAD_CONTROLLER_RESOURCE_REBALANCE_STRATEGY =
      AutoRebalanceStrategy.class.getName();
  private static final int DEFAULT_LEAD_CONTROLLER_RESOURCE_REBALANCE_DELAY_MS = 300_000; // 5 minutes
  private static final String DEFAULT_DIM_TABLE_MAX_SIZE = "200M";
  private static final int UNSPECIFIED_THREAD_POOL = -1;

  private static final String DEFAULT_PINOT_FS_FACTORY_CLASS_LOCAL = LocalPinotFS.class.getName();

  public static final String DISABLE_GROOVY = "controller.disable.ingestion.groovy";
  public static final boolean DEFAULT_DISABLE_GROOVY = true;

  public static final String ENFORCE_POOL_BASED_ASSIGNMENT_KEY = "enforce.pool.based.assignment";
  public static final boolean DEFAULT_ENFORCE_POOL_BASED_ASSIGNMENT = false;

  public ControllerConf() {
    super(new HashMap<>());
  }

  public ControllerConf(Map<String, Object> baseProperties) {
    super(baseProperties);
  }

  public ControllerConf(Configuration baseConfiguration) {
    super(baseConfiguration);
  }

  public void setLocalTempDir(String localTempDir) {
    setProperty(LOCAL_TEMP_DIR, localTempDir);
  }

  public String getLocalTempDir() {
    return getProperty(LOCAL_TEMP_DIR);
  }

  public void setPinotFSFactoryClasses(Configuration pinotFSFactoryClasses) {
    setProperty(PINOT_FS_FACTORY_CLASS_LOCAL, DEFAULT_PINOT_FS_FACTORY_CLASS_LOCAL);

    if (pinotFSFactoryClasses != null) {
      pinotFSFactoryClasses.getKeys().forEachRemaining(key -> setProperty(key, pinotFSFactoryClasses.getProperty(key)));
    }
  }

  public void setQueryConsolePath(String path) {
    setProperty(CONSOLE_WEBAPP_ROOT_PATH, path);
  }

  public String getQueryConsoleWebappPath() {
    return Optional.ofNullable(getProperty(CONSOLE_WEBAPP_ROOT_PATH))

        .orElseGet(() -> ControllerConf.class.getClassLoader().getResource("webapp").toExternalForm());
  }

  public void setJerseyAdminPrimary(String jerseyAdminPrimary) {
    setProperty(JERSEY_ADMIN_IS_PRIMARY, jerseyAdminPrimary);
  }

  public void setHelixClusterName(String clusterName) {
    setProperty(CommonConstants.Helix.CONFIG_OF_CLUSTER_NAME, clusterName);
  }

  public void setControllerHost(String host) {
    setProperty(CONTROLLER_HOST, host);
  }

  public void setControllerVipHost(String vipHost) {
    setProperty(CONTROLLER_VIP_HOST, vipHost);
  }

  public void setControllerVipPort(String vipPort) {
    setProperty(CONTROLLER_VIP_PORT, vipPort);
  }

  public void setControllerVipProtocol(String vipProtocol) {
    setProperty(CONTROLLER_VIP_PROTOCOL, vipProtocol);
  }

  public void setControllerBrokerProtocol(String protocol) {
    setProperty(CONTROLLER_BROKER_PROTOCOL, protocol);
  }

  public void setControllerPort(String port) {
    setProperty(CONTROLLER_PORT, port);
  }

  public void setDataDir(String dataDir) {
    setProperty(DATA_DIR, StringUtils.removeEnd(dataDir, "/"));
  }

  public void setRealtimeSegmentCommitTimeoutSeconds(int timeoutSec) {
    setProperty(SEGMENT_COMMIT_TIMEOUT_SECONDS, Integer.toString(timeoutSec));
  }

  public void setControllerExecutorNumThreads(int numThreads) {
    setProperty(CONTROLLER_EXECUTOR_NUM_THREADS, Integer.toString(numThreads));
  }

  public void setControllerExecutorRebalanceNumThreads(int numThreads) {
    setProperty(CONTROLLER_EXECUTOR_REBALANCE_NUM_THREADS, Integer.toString(numThreads));
  }

  public void setUpdateSegmentStateModel(String updateStateModel) {
    setProperty(UPDATE_SEGMENT_STATE_MODEL, updateStateModel);
  }

  public void setMinISSizeForCompression(int minSize) {
    setProperty(MIN_NUM_CHARS_IN_IS_TO_TURN_ON_COMPRESSION, minSize);
  }

  public void setZkStr(String zkStr) {
    setProperty(CommonConstants.Helix.CONFIG_OF_ZOOKEEPR_SERVER, zkStr);
  }

  public void setDimTableMaxSize(String size) {
    setProperty(DIM_TABLE_MAX_SIZE, size);
  }

  public String getDimTableMaxSize() {
    return getProperty(DIM_TABLE_MAX_SIZE, DEFAULT_DIM_TABLE_MAX_SIZE);
  }

  // A boolean to decide whether Jersey API should be the primary one. For now, we set this to be false,
  // but we turn it on to true when we are sure that jersey api has no backward compatibility problems.
  public boolean isJerseyAdminPrimary() {
    return getProperty(JERSEY_ADMIN_IS_PRIMARY, true);
  }

  public String getHelixClusterName() {
    return containsKey(CommonConstants.Helix.CONFIG_OF_CLUSTER_NAME) ? getProperty(
        CommonConstants.Helix.CONFIG_OF_CLUSTER_NAME) : getProperty(HELIX_CLUSTER_NAME);
  }

  public String getControllerHost() {
    return getProperty(CONTROLLER_HOST);
  }

  public String getControllerPort() {
    return getProperty(CONTROLLER_PORT);
  }

  public String getInstanceId() {
    return getProperty(CONFIG_OF_INSTANCE_ID);
  }

  public List<String> getControllerAccessProtocols() {
    return getProperty(CONTROLLER_ACCESS_PROTOCOLS, getControllerPort() == null ? List.of("http") : List.of());
  }

  public String getControllerAccessProtocolProperty(String protocol, String property) {
    return getProperty(CONTROLLER_ACCESS_PROTOCOLS + "." + protocol + "." + property);
  }

  public String getControllerAccessProtocolProperty(String protocol, String property, String defaultValue) {
    return getProperty(CONTROLLER_ACCESS_PROTOCOLS + "." + protocol + "." + property, defaultValue);
  }

  public boolean getControllerAccessProtocolProperty(String protocol, String property, boolean defaultValue) {
    return getProperty(CONTROLLER_ACCESS_PROTOCOLS + "." + protocol + "." + property, defaultValue);
  }

  public String getDataDir() {
    return getProperty(DATA_DIR);
  }

  public int getSegmentCommitTimeoutSeconds() {
    return getProperty(SEGMENT_COMMIT_TIMEOUT_SECONDS,
        SegmentCompletionProtocol.getDefaultMaxSegmentCommitTimeSeconds());
  }

  public int getControllerExecutorNumThreads() {
    return getProperty(CONTROLLER_EXECUTOR_NUM_THREADS, UNSPECIFIED_THREAD_POOL);
  }

  public int getControllerExecutorRebalanceNumThreads() {
    return getProperty(CONTROLLER_EXECUTOR_REBALANCE_NUM_THREADS, UNSPECIFIED_THREAD_POOL);
  }

  public boolean isUpdateSegmentStateModel() {
    return getProperty(UPDATE_SEGMENT_STATE_MODEL, false);
  }

  public String generateVipUrl() {
    return getControllerVipProtocol() + "://" + getControllerVipHost() + ":" + getControllerVipPort();
  }

  public String getZkStr() {
    String zkAddress = containsKey(CommonConstants.Helix.CONFIG_OF_ZOOKEEPR_SERVER) ? getProperty(
        CommonConstants.Helix.CONFIG_OF_ZOOKEEPR_SERVER) : getProperty(ZK_STR);
    Preconditions.checkState(zkAddress != null,
        "ZK address is not configured. Please configure it using the config: 'pinot.zk.server'");
    return zkAddress;
  }

  @Override
  public String toString() {
    return super.toString();
  }

  public String getControllerVipHost() {
    return Optional.ofNullable(getProperty(CONTROLLER_VIP_HOST))

        .filter(controllerVipHost -> !controllerVipHost.isEmpty())

        .orElseGet(() -> getProperty(CONTROLLER_HOST));
  }

  public String getControllerVipPort() {
    return Optional.ofNullable(getProperty(CONTROLLER_VIP_PORT))

        .filter(controllerVipPort -> !controllerVipPort.isEmpty())

        .orElseGet(() -> getControllerAccessProtocols().stream()

            .filter(protocol -> getControllerAccessProtocolProperty(protocol, "vip", false))

            .map(protocol -> Optional.ofNullable(getControllerAccessProtocolProperty(protocol, "port")))

            .filter(Optional::isPresent)

            .map(Optional::get)

            .findFirst()

            // No protocol defines a port as VIP. Fallback on legacy controller.port property.
            .orElseGet(this::getControllerPort));
  }

  public String getControllerVipProtocol() {
    return getSupportedProtocol(CONTROLLER_VIP_PROTOCOL);
  }

  public String getControllerBrokerProtocol() {
    return getSupportedProtocol(CONTROLLER_BROKER_PROTOCOL);
  }

  public int getRetentionControllerFrequencyInSeconds() {
    return Optional.ofNullable(getProperty(ControllerPeriodicTasksConf.RETENTION_MANAGER_FREQUENCY_PERIOD))
        .map(period -> (int) convertPeriodToSeconds(period)).orElseGet(
            () -> getProperty(ControllerPeriodicTasksConf.DEPRECATED_RETENTION_MANAGER_FREQUENCY_IN_SECONDS,
                ControllerPeriodicTasksConf.DEFAULT_RETENTION_MANAGER_FREQUENCY_IN_SECONDS));
  }

  public void setRetentionControllerFrequencyInSeconds(int retentionFrequencyInSeconds) {
    setProperty(ControllerPeriodicTasksConf.DEPRECATED_RETENTION_MANAGER_FREQUENCY_IN_SECONDS,
        Integer.toString(retentionFrequencyInSeconds));
  }

  /**
   * Returns <code>controller.offline.segment.interval.checker.frequencyPeriod</code>, or
   * <code>controller.offline.segment.interval.checker.frequencyPeriod</code> or the segment level
   * validation interval, in the order of decreasing preference from left to right. Falls-back to
   * the next config only if the current config is missing. This is done in order to retain the
   * current behavior, wherein the offline validation tasks were done at segment level validation
   * interval frequency The default value is the new DEFAULT_OFFLINE_SEGMENT_INTERVAL_CHECKER_FREQUENCY_IN_SECONDS
   *
   * @return the supplied config in seconds
   */
  public int getOfflineSegmentIntervalCheckerFrequencyInSeconds() {
    return Optional.ofNullable(
            getProperty(ControllerPeriodicTasksConf.OFFLINE_SEGMENT_INTERVAL_CHECKER_FREQUENCY_PERIOD))
        .map(period -> (int) convertPeriodToSeconds(period)).orElseGet(() -> getProperty(
            ControllerPeriodicTasksConf.DEPRECATED_OFFLINE_SEGMENT_INTERVAL_CHECKER_FREQUENCY_IN_SECONDS,
            ControllerPeriodicTasksConf.DEFAULT_OFFLINE_SEGMENT_INTERVAL_CHECKER_FREQUENCY_IN_SECONDS));
  }

  public void setOfflineSegmentIntervalCheckerFrequencyInSeconds(int validationFrequencyInSeconds) {
    setProperty(ControllerPeriodicTasksConf.DEPRECATED_OFFLINE_SEGMENT_INTERVAL_CHECKER_FREQUENCY_IN_SECONDS,
        Integer.toString(validationFrequencyInSeconds));
  }

  /**
   * Returns <code>controller.realtime.segment.validation.frequencyPeriod</code> or
   * <code>controller.realtime.segment.validation.frequencyInSeconds</code> or the default realtime segment
   * validation frequncy, in the order of decreasing preference from left to right. This is done in
   * order to retain the current behavior, wherein the realtime validation tasks were done at
   * validation controller frequency The default value is the new
   * DEFAULT_REALTIME_SEGMENT_VALIDATION_FREQUENCY_IN_SECONDS
   *
   * @return supplied config in seconds
   */
  public int getRealtimeSegmentValidationFrequencyInSeconds() {
    return Optional.ofNullable(getProperty(ControllerPeriodicTasksConf.REALTIME_SEGMENT_VALIDATION_FREQUENCY_PERIOD))
        .map(period -> (int) convertPeriodToSeconds(period)).orElseGet(
            () -> getProperty(ControllerPeriodicTasksConf.DEPRECATED_REALTIME_SEGMENT_VALIDATION_FREQUENCY_IN_SECONDS,
                ControllerPeriodicTasksConf.DEFAULT_REALTIME_SEGMENT_VALIDATION_FREQUENCY_IN_SECONDS));
  }

  public void setRealtimeSegmentValidationFrequencyInSeconds(int validationFrequencyInSeconds) {
    setProperty(ControllerPeriodicTasksConf.DEPRECATED_REALTIME_SEGMENT_VALIDATION_FREQUENCY_IN_SECONDS,
        Integer.toString(validationFrequencyInSeconds));
  }

  /**
   * Return <code>controller.broker.resource.validation.frequencyPeriod</code> or
   * <code>controller.broker.resource.validation.frequencyInSeconds</code> or the default broker resource validation
   * frequency, in order of decreasing preference from left to righ. This is done in order
   * to retain the current behavior, wherein the broker resource validation tasks were done at
   * validation controller frequency The default value is the new
   * DEFAULT_BROKER_RESOURCE_VALIDATION_FREQUENCY_IN_SECONDS
   *
   * @return the supplied config in seconds
   */
  public int getBrokerResourceValidationFrequencyInSeconds() {
    return Optional.ofNullable(getProperty(ControllerPeriodicTasksConf.BROKER_RESOURCE_VALIDATION_FREQUENCY_PERIOD))
        .map(period -> (int) convertPeriodToSeconds(period)).orElseGet(
            () -> getProperty(ControllerPeriodicTasksConf.DEPRECATED_BROKER_RESOURCE_VALIDATION_FREQUENCY_IN_SECONDS,
                ControllerPeriodicTasksConf.DEFAULT_BROKER_RESOURCE_VALIDATION_FREQUENCY_IN_SECONDS));
  }

  public void setBrokerResourceValidationFrequencyInSeconds(int validationFrequencyInSeconds) {
    setProperty(ControllerPeriodicTasksConf.DEPRECATED_BROKER_RESOURCE_VALIDATION_FREQUENCY_IN_SECONDS,
        Integer.toString(validationFrequencyInSeconds));
  }

  public long getBrokerResourceValidationInitialDelayInSeconds() {
    return getProperty(ControllerPeriodicTasksConf.BROKER_RESOURCE_VALIDATION_INITIAL_DELAY_IN_SECONDS,
        getPeriodicTaskInitialDelayInSeconds());
  }

  public int getStatusCheckerFrequencyInSeconds() {
    return Optional.ofNullable(getProperty(ControllerPeriodicTasksConf.STATUS_CHECKER_FREQUENCY_PERIOD))
        .map(period -> (int) convertPeriodToSeconds(period)).orElseGet(
            () -> getProperty(ControllerPeriodicTasksConf.DEPRECATED_STATUS_CHECKER_FREQUENCY_IN_SECONDS,
                ControllerPeriodicTasksConf.DEFAULT_STATUS_CHECKER_FREQUENCY_IN_SECONDS));
  }

  public void setStatusCheckerFrequencyInSeconds(int statusCheckerFrequencyInSeconds) {
    setProperty(ControllerPeriodicTasksConf.DEPRECATED_STATUS_CHECKER_FREQUENCY_IN_SECONDS,
        Integer.toString(statusCheckerFrequencyInSeconds));
  }

  public int getRebalanceCheckerFrequencyInSeconds() {
    return Optional.ofNullable(getProperty(ControllerPeriodicTasksConf.REBALANCE_CHECKER_FREQUENCY_PERIOD))
        .map(period -> (int) convertPeriodToSeconds(period))
        .orElse(ControllerPeriodicTasksConf.DEFAULT_REBALANCE_CHECKER_FREQUENCY_IN_SECONDS);
  }

  public long getRebalanceCheckerInitialDelayInSeconds() {
    return getProperty(ControllerPeriodicTasksConf.REBALANCE_CHECKER_INITIAL_DELAY_IN_SECONDS,
        ControllerPeriodicTasksConf.getRandomInitialDelayInSeconds());
  }

  public int getRealtimeConsumerMonitorRunFrequency() {
    return Optional.ofNullable(getProperty(ControllerPeriodicTasksConf.RT_CONSUMER_MONITOR_FREQUENCY_PERIOD))
        .map(period -> (int) convertPeriodToSeconds(period))
        .orElse(ControllerPeriodicTasksConf.DEFAULT_RT_CONSUMER_MONITOR_FREQUENCY_IN_SECONDS);
  }

  public long getRealtimeConsumerMonitorInitialDelayInSeconds() {
    return getProperty(ControllerPeriodicTasksConf.RT_CONSUMER_MONITOR_INITIAL_DELAY_IN_SECONDS,
        ControllerPeriodicTasksConf.getRandomInitialDelayInSeconds());
  }

  public int getTaskMetricsEmitterFrequencyInSeconds() {
    return Optional.ofNullable(getProperty(ControllerPeriodicTasksConf.TASK_METRICS_EMITTER_FREQUENCY_PERIOD))
        .map(period -> (int) convertPeriodToSeconds(period)).orElseGet(
            () -> getProperty(ControllerPeriodicTasksConf.DEPRECATED_TASK_METRICS_EMITTER_FREQUENCY_IN_SECONDS,
                ControllerPeriodicTasksConf.DEFAULT_TASK_METRICS_EMITTER_FREQUENCY_IN_SECONDS));
  }

  public void setTaskMetricsEmitterFrequencyInSeconds(int taskMetricsEmitterFrequencyInSeconds) {
    setProperty(ControllerPeriodicTasksConf.DEPRECATED_TASK_METRICS_EMITTER_FREQUENCY_IN_SECONDS,
        Integer.toString(taskMetricsEmitterFrequencyInSeconds));
  }

  public int getStatusCheckerWaitForPushTimeInSeconds() {
    return Optional.ofNullable(getProperty(ControllerPeriodicTasksConf.STATUS_CHECKER_WAIT_FOR_PUSH_TIME_PERIOD))
        .map(period -> (int) convertPeriodToSeconds(period)).orElseGet(
            () -> getProperty(ControllerPeriodicTasksConf.DEPRECATED_STATUS_CHECKER_WAIT_FOR_PUSH_TIME_IN_SECONDS,
                ControllerPeriodicTasksConf.DEFAULT_STATUS_CONTROLLER_WAIT_FOR_PUSH_TIME_IN_SECONDS));
  }

  public void setStatusCheckerWaitForPushTimeInSeconds(int statusCheckerWaitForPushTimeInSeconds) {
    setProperty(ControllerPeriodicTasksConf.DEPRECATED_STATUS_CHECKER_WAIT_FOR_PUSH_TIME_IN_SECONDS,
        Integer.toString(statusCheckerWaitForPushTimeInSeconds));
  }

  /**
   * RealtimeSegmentRelocator has been rebranded to SegmentRelocator. Returns <code>controller.segment.relocator
   * .frequencyPeriod</code> or <code>controller.segment.relocator .frequencyInSeconds</code> or
   * REALTIME_SEGMENT_RELOCATOR_FREQUENCY, in the order of decreasing perference (left -> right).
   */
  public int getSegmentRelocatorFrequencyInSeconds() {
    return Optional.ofNullable(getProperty(ControllerPeriodicTasksConf.SEGMENT_RELOCATOR_FREQUENCY_PERIOD))
        .map(period -> (int) convertPeriodToSeconds(period)).orElseGet(() -> {
          Integer segmentRelocatorFreqSeconds =
              getProperty(ControllerPeriodicTasksConf.DEPRECATED_SEGMENT_RELOCATOR_FREQUENCY_IN_SECONDS, Integer.class);
          if (segmentRelocatorFreqSeconds == null) {
            String realtimeSegmentRelocatorPeriod =
                getProperty(ControllerPeriodicTasksConf.DEPRECATED_REALTIME_SEGMENT_RELOCATOR_FREQUENCY);
            if (realtimeSegmentRelocatorPeriod != null) {
              segmentRelocatorFreqSeconds = (int) convertPeriodToSeconds(realtimeSegmentRelocatorPeriod);
            } else {
              segmentRelocatorFreqSeconds = ControllerPeriodicTasksConf.DEFAULT_SEGMENT_RELOCATOR_FREQUENCY_IN_SECONDS;
            }
          }
          return segmentRelocatorFreqSeconds;
        });
  }

  public void setSegmentRelocatorFrequencyInSeconds(int segmentRelocatorFrequencyInSeconds) {
    setProperty(ControllerPeriodicTasksConf.DEPRECATED_SEGMENT_RELOCATOR_FREQUENCY_IN_SECONDS,
        Integer.toString(segmentRelocatorFrequencyInSeconds));
  }

  public boolean getSegmentRelocatorReassignInstances() {
    return Optional.ofNullable(getProperty(ControllerPeriodicTasksConf.SEGMENT_RELOCATOR_REASSIGN_INSTANCES))
        .map(Boolean::parseBoolean).orElse(false);
  }

  public boolean getSegmentRelocatorBootstrap() {
    return Optional.ofNullable(getProperty(ControllerPeriodicTasksConf.SEGMENT_RELOCATOR_BOOTSTRAP))
        .map(Boolean::parseBoolean).orElse(false);
  }

  public boolean getSegmentRelocatorDowntime() {
    return Optional.ofNullable(getProperty(ControllerPeriodicTasksConf.SEGMENT_RELOCATOR_DOWNTIME))
        .map(Boolean::parseBoolean).orElse(false);
  }

  public int getSegmentRelocatorMinAvailableReplicas() {
    return Optional.ofNullable(getProperty(ControllerPeriodicTasksConf.SEGMENT_RELOCATOR_MIN_AVAILABLE_REPLICAS))
        .map(Integer::parseInt).orElse(-1);
  }

  public boolean getSegmentRelocatorBestEfforts() {
    return Optional.ofNullable(getProperty(ControllerPeriodicTasksConf.SEGMENT_RELOCATOR_BEST_EFFORTS))
        .map(Boolean::parseBoolean).orElse(true);
  }

  public long getSegmentRelocatorExternalViewCheckIntervalInMs() {
    return getProperty(ControllerPeriodicTasksConf.SEGMENT_RELOCATOR_EXTERNAL_VIEW_CHECK_INTERVAL_IN_MS,
        RebalanceConfig.DEFAULT_EXTERNAL_VIEW_CHECK_INTERVAL_IN_MS);
  }

  public long getSegmentRelocatorExternalViewStabilizationTimeoutInMs() {
    return getProperty(ControllerPeriodicTasksConf.SEGMENT_RELOCATOR_EXTERNAL_VIEW_STABILIZATION_TIMEOUT_IN_MS,
        RebalanceConfig.DEFAULT_EXTERNAL_VIEW_STABILIZATION_TIMEOUT_IN_MS);
  }

  public boolean enableSegmentRelocatorLocalTierMigration() {
    return getProperty(ControllerPeriodicTasksConf.SEGMENT_RELOCATOR_ENABLE_LOCAL_TIER_MIGRATION, false);
  }

  public boolean isSegmentRelocatorRebalanceTablesSequentially() {
    return getProperty(ControllerPeriodicTasksConf.SEGMENT_RELOCATOR_REBALANCE_TABLES_SEQUENTIALLY, false);
  }

  public boolean tieredSegmentAssignmentEnabled() {
    return getProperty(CONTROLLER_ENABLE_TIERED_SEGMENT_ASSIGNMENT, false);
  }

  public void setTieredSegmentAssignmentEnabled(boolean enabled) {
    setProperty(CONTROLLER_ENABLE_TIERED_SEGMENT_ASSIGNMENT, enabled);
  }

  public boolean tenantIsolationEnabled() {
    return getProperty(CLUSTER_TENANT_ISOLATION_ENABLE, true);
  }

  public void setTenantIsolationEnabled(boolean isSingleTenant) {
    setProperty(CLUSTER_TENANT_ISOLATION_ENABLE, isSingleTenant);
  }

  public void setServerAdminRequestTimeoutSeconds(int timeoutSeconds) {
    setProperty(SERVER_ADMIN_REQUEST_TIMEOUT_SECONDS, timeoutSeconds);
  }

  public int getServerAdminRequestTimeoutSeconds() {
    return getProperty(SERVER_ADMIN_REQUEST_TIMEOUT_SECONDS, DEFAULT_SERVER_ADMIN_REQUEST_TIMEOUT_SECONDS);
  }

  public void setMinionAdminRequestTimeoutSeconds(int timeoutSeconds) {
    setProperty(MINION_ADMIN_REQUEST_TIMEOUT_SECONDS, timeoutSeconds);
  }

  public int getMinionAdminRequestTimeoutSeconds() {
    return getProperty(MINION_ADMIN_REQUEST_TIMEOUT_SECONDS, DEFAULT_MINION_ADMIN_REQUEST_TIMEOUT_SECONDS);
  }

  public int getDeletedSegmentsRetentionInDays() {
    return getProperty(DELETED_SEGMENTS_RETENTION_IN_DAYS, DEFAULT_DELETED_SEGMENTS_RETENTION_IN_DAYS);
  }

  public void setDeletedSegmentsRetentionInDays(int retentionInDays) {
    setProperty(DELETED_SEGMENTS_RETENTION_IN_DAYS, retentionInDays);
  }

  public boolean isSkipLateCronSchedule() {
    return getProperty(ControllerPeriodicTasksConf.TASK_MANAGER_SKIP_LATE_CRON_SCHEDULE, false);
  }

  public int getMaxCronScheduleDelayInSeconds() {
    return getProperty(ControllerPeriodicTasksConf.TASK_MANAGER_MAX_CRON_SCHEDULE_DELAY_IN_SECONDS, 600);
  }

  public int getTaskManagerFrequencyInSeconds() {
    return Optional.ofNullable(getProperty(ControllerPeriodicTasksConf.TASK_MANAGER_FREQUENCY_PERIOD))
        .map(period -> (int) convertPeriodToSeconds(period)).orElseGet(
            () -> getProperty(ControllerPeriodicTasksConf.DEPRECATED_TASK_MANAGER_FREQUENCY_IN_SECONDS,
                ControllerPeriodicTasksConf.DEFAULT_TASK_MANAGER_FREQUENCY_IN_SECONDS));
  }

  public void setTaskManagerFrequencyInSeconds(int frequencyInSeconds) {
    setProperty(ControllerPeriodicTasksConf.DEPRECATED_TASK_MANAGER_FREQUENCY_IN_SECONDS,
        Integer.toString(frequencyInSeconds));
  }

  @Deprecated
  public int getMinionInstancesCleanupTaskFrequencyInSeconds() {
    return Optional.ofNullable(getProperty(ControllerPeriodicTasksConf.MINION_INSTANCES_CLEANUP_TASK_FREQUENCY_PERIOD))
        .map(period -> (int) convertPeriodToSeconds(period)).orElseGet(
            () -> getProperty(ControllerPeriodicTasksConf.DEPRECATED_MINION_INSTANCES_CLEANUP_TASK_FREQUENCY_IN_SECONDS,
                ControllerPeriodicTasksConf.DEFAULT_MINION_INSTANCES_CLEANUP_TASK_FREQUENCY_IN_SECONDS));
  }

  @Deprecated
  public void setMinionInstancesCleanupTaskFrequencyInSeconds(int frequencyInSeconds) {
    setProperty(ControllerPeriodicTasksConf.DEPRECATED_MINION_INSTANCES_CLEANUP_TASK_FREQUENCY_IN_SECONDS,
        Integer.toString(frequencyInSeconds));
  }

  @Deprecated
  public long getMinionInstancesCleanupTaskInitialDelaySeconds() {
    return getProperty(ControllerPeriodicTasksConf.MINION_INSTANCES_CLEANUP_TASK_INITIAL_DELAY_SECONDS,
        ControllerPeriodicTasksConf.getRandomInitialDelayInSeconds());
  }

  @Deprecated
  public void setMinionInstancesCleanupTaskInitialDelaySeconds(int initialDelaySeconds) {
    setProperty(ControllerPeriodicTasksConf.MINION_INSTANCES_CLEANUP_TASK_INITIAL_DELAY_SECONDS,
        Integer.toString(initialDelaySeconds));
  }

  @Deprecated
  public int getMinionInstancesCleanupTaskMinOfflineTimeBeforeDeletionInSeconds() {
    return Optional.ofNullable(
        getProperty(ControllerPeriodicTasksConf.MINION_INSTANCES_CLEANUP_TASK_MIN_OFFLINE_TIME_BEFORE_DELETION_PERIOD))
        .map(period -> (int) convertPeriodToSeconds(period)).orElseGet(() -> getProperty(
            ControllerPeriodicTasksConf.
                DEPRECATED_MINION_INSTANCES_CLEANUP_TASK_MIN_OFFLINE_TIME_BEFORE_DELETION_SECONDS,
            ControllerPeriodicTasksConf.
                DEFAULT_MINION_INSTANCES_CLEANUP_TASK_MIN_OFFLINE_TIME_BEFORE_DELETION_IN_SECONDS));
  }

  @Deprecated
  public void setMinionInstancesCleanupTaskMinOfflineTimeBeforeDeletionInSeconds(int maxOfflineTimeRangeInSeconds) {
    setProperty(
        ControllerPeriodicTasksConf.DEPRECATED_MINION_INSTANCES_CLEANUP_TASK_MIN_OFFLINE_TIME_BEFORE_DELETION_SECONDS,
        Integer.toString(maxOfflineTimeRangeInSeconds));
  }

  public int getStaleInstancesCleanupTaskFrequencyInSeconds() {
    return Optional.ofNullable(getProperty(ControllerPeriodicTasksConf.STALE_INSTANCES_CLEANUP_TASK_FREQUENCY_PERIOD))
        .map(period -> (int) convertPeriodToSeconds(period))
        // Backward compatible for existing users who configured MinionInstancesCleanupTask
        .orElse(getMinionInstancesCleanupTaskFrequencyInSeconds());
  }

  public void setStaleInstanceCleanupTaskFrequencyInSeconds(String frequencyPeriod) {
    setProperty(ControllerPeriodicTasksConf.STALE_INSTANCES_CLEANUP_TASK_FREQUENCY_PERIOD, frequencyPeriod);
  }

  public long getStaleInstanceCleanupTaskInitialDelaySeconds() {
    return getProperty(ControllerPeriodicTasksConf.STALE_INSTANCES_CLEANUP_TASK_INITIAL_DELAY_SECONDS,
        // Backward compatible for existing users who configured MinionInstancesCleanupTask
        getMinionInstancesCleanupTaskInitialDelaySeconds());
  }

  public void setStaleInstanceCleanupTaskInitialDelaySeconds(long initialDelaySeconds) {
    setProperty(ControllerPeriodicTasksConf.STALE_INSTANCES_CLEANUP_TASK_INITIAL_DELAY_SECONDS, initialDelaySeconds);
  }

  public int getStaleInstancesCleanupTaskInstancesRetentionInSeconds() {
    return Optional.ofNullable(
            getProperty(ControllerPeriodicTasksConf.STALE_INSTANCES_CLEANUP_TASK_INSTANCES_RETENTION_PERIOD))
        .map(period -> (int) convertPeriodToSeconds(period))
        // Backward compatible for existing users who configured MinionInstancesCleanupTask
        .orElse(getMinionInstancesCleanupTaskMinOfflineTimeBeforeDeletionInSeconds());
  }

  public void setStaleInstancesCleanupTaskInstancesRetentionPeriod(String retentionPeriod) {
    setProperty(ControllerPeriodicTasksConf.STALE_INSTANCES_CLEANUP_TASK_INSTANCES_RETENTION_PERIOD, retentionPeriod);
  }

  public int getDefaultTableMinReplicas() {
    return getProperty(TABLE_MIN_REPLICAS, DEFAULT_TABLE_MIN_REPLICAS);
  }

  public void setTableMinReplicas(int minReplicas) {
    setProperty(TABLE_MIN_REPLICAS, minReplicas);
  }

  public String getJerseyAdminApiPort() {
    return getProperty(JERSEY_ADMIN_API_PORT, String.valueOf(DEFAULT_JERSEY_ADMIN_PORT));
  }

  public void setInitAccessControlUsername(String username) {
    setProperty(ACCESS_CONTROL_USERNAME, username);
  }

  public void setInitAccessControlPassword(String password) {
    setProperty(ACCESS_CONTROL_PASSWORD, password);
  }

  public String getInitAccessControlUsername() {
    return getProperty(ACCESS_CONTROL_USERNAME, DEFAULT_ACCESS_CONTROL_USERNAME);
  }

  public String getInitAccessControlPassword() {
    return getProperty(ACCESS_CONTROL_PASSWORD, DEFAULT_ACCESS_CONTROL_PASSWORD);
  }

  public String getAccessControlFactoryClass() {
    return getProperty(ACCESS_CONTROL_FACTORY_CLASS, DEFAULT_ACCESS_CONTROL_FACTORY_CLASS);
  }

  public void setAccessControlFactoryClass(String accessControlFactoryClass) {
    setProperty(ACCESS_CONTROL_FACTORY_CLASS, accessControlFactoryClass);
  }

  public String getLineageManagerClass() {
    return getProperty(LINEAGE_MANAGER_CLASS, DEFAULT_LINEAGE_MANAGER);
  }

  public void setLineageManagerClass(String lineageModifierClass) {
    setProperty(LINEAGE_MANAGER_CLASS, lineageModifierClass);
  }

  public String getRebalancePreCheckerClass() {
    return getProperty(REBALANCE_PRE_CHECKER_CLASS, DEFAULT_REBALANCE_PRE_CHECKER);
  }

  public void setRebalancePreCheckerClass(String rebalancePreCheckerClass) {
    setProperty(REBALANCE_PRE_CHECKER_CLASS, rebalancePreCheckerClass);
  }

  public long getSegmentUploadTimeoutInMillis() {
    return getProperty(SEGMENT_UPLOAD_TIMEOUT_IN_MILLIS, DEFAULT_SEGMENT_UPLOAD_TIMEOUT_IN_MILLIS);
  }

  public int getMinNumCharsInISToTurnOnCompression() {
    return getProperty(MIN_NUM_CHARS_IN_IS_TO_TURN_ON_COMPRESSION, DEFAULT_MIN_NUM_CHARS_IN_IS_TO_TURN_ON_COMPRESSION);
  }

  public void setSegmentUploadTimeoutInMillis(long segmentUploadTimeoutInMillis) {
    setProperty(SEGMENT_UPLOAD_TIMEOUT_IN_MILLIS, segmentUploadTimeoutInMillis);
  }

  public int getRealtimeSegmentMetadataCommitNumLocks() {
    return getProperty(REALTIME_SEGMENT_METADATA_COMMIT_NUMLOCKS, DEFAULT_REALTIME_SEGMENT_METADATA_COMMIT_NUMLOCKS);
  }

  public void setRealtimeSegmentMetadataCommitNumLocks(int realtimeSegmentMetadataCommitNumLocks) {
    setProperty(REALTIME_SEGMENT_METADATA_COMMIT_NUMLOCKS, realtimeSegmentMetadataCommitNumLocks);
  }

  public boolean getEnableStorageQuotaCheck() {
    return getProperty(ENABLE_STORAGE_QUOTA_CHECK, DEFAULT_ENABLE_STORAGE_QUOTA_CHECK);
  }

  public String getDiskUtilizationPath() {
    return getProperty(DISK_UTILIZATION_PATH, DEFAULT_DISK_UTILIZATION_PATH);
  }

  public double getDiskUtilizationThreshold() {
    return getProperty(DISK_UTILIZATION_THRESHOLD, DEFAULT_DISK_UTILIZATION_THRESHOLD);
  }

  public double getRebalanceDiskUtilizationThreshold() {
    return getProperty(REBALANCE_DISK_UTILIZATION_THRESHOLD, DEFAULT_REBALANCE_DISK_UTILIZATION_THRESHOLD);
  }

  public int getDiskUtilizationCheckTimeoutMs() {
    return getProperty(DISK_UTILIZATION_CHECK_TIMEOUT_MS, DEFAULT_DISK_UTILIZATION_CHECK_TIMEOUT_MS);
  }

  public long getResourceUtilizationCheckerInitialDelay() {
    return getProperty(RESOURCE_UTILIZATION_CHECKER_INITIAL_DELAY, DEFAULT_RESOURCE_UTILIZATION_CHECKER_INITIAL_DELAY);
  }

  public long getResourceUtilizationCheckerFrequency() {
    return getProperty(RESOURCE_UTILIZATION_CHECKER_FREQUENCY, DEFAULT_RESOURCE_UTILIZATION_CHECKER_FREQUENCY);
  }

  public boolean isResourceUtilizationCheckEnabled() {
    return getProperty(ENABLE_RESOURCE_UTILIZATION_CHECK, DEFAULT_ENABLE_RESOURCE_UTILIZATION_CHECK);
  }

  public boolean getEnableBatchMessageMode() {
    return getProperty(ENABLE_BATCH_MESSAGE_MODE, DEFAULT_ENABLE_BATCH_MESSAGE_MODE);
  }

  public int getSegmentLevelValidationIntervalInSeconds() {
    return Optional.ofNullable(getProperty(ControllerPeriodicTasksConf.SEGMENT_LEVEL_VALIDATION_INTERVAL_PERIOD))
        .map(period -> (int) convertPeriodToSeconds(period)).orElseGet(
            () -> getProperty(ControllerPeriodicTasksConf.DEPRECATED_SEGMENT_LEVEL_VALIDATION_INTERVAL_IN_SECONDS,
                ControllerPeriodicTasksConf.DEFAULT_SEGMENT_LEVEL_VALIDATION_INTERVAL_IN_SECONDS));
  }

  public boolean isAutoResetErrorSegmentsOnValidationEnabled() {
    return getProperty(ControllerPeriodicTasksConf.AUTO_RESET_ERROR_SEGMENTS_VALIDATION, false);
  }

  public long getStatusCheckerInitialDelayInSeconds() {
    return getProperty(ControllerPeriodicTasksConf.STATUS_CHECKER_INITIAL_DELAY_IN_SECONDS,
        ControllerPeriodicTasksConf.getRandomInitialDelayInSeconds());
  }

  public long getRetentionManagerInitialDelayInSeconds() {
    return getProperty(ControllerPeriodicTasksConf.RETENTION_MANAGER_INITIAL_DELAY_IN_SECONDS,
        ControllerPeriodicTasksConf.getRandomInitialDelayInSeconds());
  }

  public long getOfflineSegmentIntervalCheckerInitialDelayInSeconds() {
    return getProperty(ControllerPeriodicTasksConf.OFFLINE_SEGMENT_INTERVAL_CHECKER_INITIAL_DELAY_IN_SECONDS,
        ControllerPeriodicTasksConf.getRandomInitialDelayInSeconds());
  }

  public long getRealtimeSegmentValidationManagerInitialDelaySeconds() {
    return getProperty(ControllerPeriodicTasksConf.REALTIME_SEGMENT_VALIDATION_INITIAL_DELAY_IN_SECONDS,
        getPeriodicTaskInitialDelayInSeconds());
  }

  public boolean isDeepStoreRetryUploadLLCSegmentEnabled() {
    return getProperty(ControllerPeriodicTasksConf.ENABLE_DEEP_STORE_RETRY_UPLOAD_LLC_SEGMENT, false);
  }

  public boolean isTmpSegmentAsyncDeletionEnabled() {
    return getProperty(ControllerPeriodicTasksConf.ENABLE_TMP_SEGMENT_ASYNC_DELETION, false);
  }

  public int getDeepStoreRetryUploadTimeoutMs() {
    return getProperty(ControllerPeriodicTasksConf.DEEP_STORE_RETRY_UPLOAD_TIMEOUT_MS, -1);
  }

  public int getDeepStoreRetryUploadParallelism() {
    return getProperty(ControllerPeriodicTasksConf.DEEP_STORE_RETRY_UPLOAD_PARALLELISM, 1);
  }

  public int getTmpSegmentRetentionInSeconds() {
    return getProperty(ControllerPeriodicTasksConf.TMP_SEGMENT_RETENTION_IN_SECONDS,
        ControllerPeriodicTasksConf.DEFAULT_SPLIT_COMMIT_TMP_SEGMENT_LIFETIME_SECOND);
  }

  public boolean getUntrackedSegmentDeletionEnabled() {
    return getProperty(ControllerPeriodicTasksConf.ENABLE_UNTRACKED_SEGMENT_DELETION, false);
  }

  public void setUntrackedSegmentDeletionEnabled(boolean untrackedSegmentDeletionEnabled) {
    setProperty(ControllerPeriodicTasksConf.ENABLE_UNTRACKED_SEGMENT_DELETION, untrackedSegmentDeletionEnabled);
  }


  public long getPinotTaskManagerInitialDelaySeconds() {
    return getPeriodicTaskInitialDelayInSeconds();
  }

  public boolean isPinotTaskManagerSchedulerEnabled() {
    return getProperty(ControllerPeriodicTasksConf.PINOT_TASK_MANAGER_SCHEDULER_ENABLED, false);
  }

  public long getPinotTaskExpireTimeInMs() {
    return getProperty(ControllerPeriodicTasksConf.PINOT_TASK_EXPIRE_TIME_MS, TimeUnit.HOURS.toMillis(24));
  }

  /**
   * RealtimeSegmentRelocator has been rebranded to SegmentRelocator.
   * Check for SEGMENT_RELOCATOR_INITIAL_DELAY_IN_SECONDS property, if not found, return
   * REALTIME_SEGMENT_RELOCATION_INITIAL_DELAY_IN_SECONDS
   */
  public long getSegmentRelocatorInitialDelayInSeconds() {
    Long segmentRelocatorInitialDelaySeconds =
        getProperty(ControllerPeriodicTasksConf.SEGMENT_RELOCATOR_INITIAL_DELAY_IN_SECONDS, Long.class);
    if (segmentRelocatorInitialDelaySeconds == null) {
      segmentRelocatorInitialDelaySeconds =
          getProperty(ControllerPeriodicTasksConf.DEPRECATED_REALTIME_SEGMENT_RELOCATION_INITIAL_DELAY_IN_SECONDS,
              ControllerPeriodicTasksConf.getRandomInitialDelayInSeconds());
    }
    return segmentRelocatorInitialDelaySeconds;
  }

  public long getPeriodicTaskInitialDelayInSeconds() {
    return ControllerPeriodicTasksConf.getRandomInitialDelayInSeconds();
  }

  public void setControllerMode(ControllerMode controllerMode) {
    setProperty(CONTROLLER_MODE, controllerMode.name());
  }

  public ControllerMode getControllerMode() {
    return ControllerMode.valueOf(getProperty(CONTROLLER_MODE, DEFAULT_CONTROLLER_MODE).toUpperCase());
  }

  public void setLeadControllerResourceRebalanceStrategy(String rebalanceStrategy) {
    setProperty(LEAD_CONTROLLER_RESOURCE_REBALANCE_STRATEGY, rebalanceStrategy);
  }

  public String getLeadControllerResourceRebalanceStrategy() {
    return getProperty(LEAD_CONTROLLER_RESOURCE_REBALANCE_STRATEGY,
        DEFAULT_LEAD_CONTROLLER_RESOURCE_REBALANCE_STRATEGY);
  }

  public void setLeadControllerResourceRebalanceDelayMs(long rebalanceDelayMs) {
    setProperty(LEAD_CONTROLLER_RESOURCE_REBALANCE_DELAY_MS, rebalanceDelayMs);
  }

  public int getLeadControllerResourceRebalanceDelayMs() {
    return getProperty(LEAD_CONTROLLER_RESOURCE_REBALANCE_DELAY_MS,
        DEFAULT_LEAD_CONTROLLER_RESOURCE_REBALANCE_DELAY_MS);
  }

  public boolean getHLCTablesAllowed() {
    return DEFAULT_ALLOW_HLC_TABLES;
  }

  public String getMetricsPrefix() {
    return getProperty(CONFIG_OF_CONTROLLER_METRICS_PREFIX, DEFAULT_METRICS_PREFIX);
  }

  public int getControllerBrokerPortOverride() {
    return getProperty(CONTROLLER_BROKER_PORT_OVERRIDE, -1);
  }

  public List<String> getTableConfigTunerPackages() {
    return Arrays.asList(
        getProperty(TABLE_CONFIG_TUNER_PACKAGES, DEFAULT_TABLE_CONFIG_TUNER_PACKAGES).split("\\s*,\\s*"));
  }

  public String getControllerResourcePackages() {
    return getProperty(CONTROLLER_RESOURCE_PACKAGES, DEFAULT_CONTROLLER_RESOURCE_PACKAGES);
  }

  /**
   * @return true if Groovy functions are disabled in controller config, otherwise returns false.
   */
  public boolean isDisableIngestionGroovy() {
    return getProperty(DISABLE_GROOVY, DEFAULT_DISABLE_GROOVY);
  }

  private long convertPeriodToUnit(String period, TimeUnit timeUnitToConvertTo) {
    return timeUnitToConvertTo.convert(TimeUtils.convertPeriodToMillis(period), TimeUnit.MILLISECONDS);
  }

  private long convertPeriodToSeconds(String period) {
    return convertPeriodToUnit(period, TimeUnit.SECONDS);
  }

  private String getSupportedProtocol(String property) {
    String value = getProperty(property, CommonConstants.HTTP_PROTOCOL);
    Preconditions.checkArgument(SUPPORTED_PROTOCOLS.contains(value), "Unsupported %s protocol '%s'", property, value);
    return value;
  }

  public boolean isEnforcePoolBasedAssignmentEnabled() {
    return getProperty(ENFORCE_POOL_BASED_ASSIGNMENT_KEY, DEFAULT_ENFORCE_POOL_BASED_ASSIGNMENT);
  }

  public void setEnableSwagger(boolean value) {
    setProperty(ControllerConf.CONSOLE_SWAGGER_ENABLE, value);
  }

  public boolean isEnableSwagger() {
    String enableSwagger = getProperty(ControllerConf.CONSOLE_SWAGGER_ENABLE);
    return enableSwagger == null || Boolean.parseBoolean(enableSwagger);
  }
}
