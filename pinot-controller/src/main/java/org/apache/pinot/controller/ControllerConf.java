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
import org.apache.commons.configuration.Configuration;
import org.apache.helix.controller.rebalancer.strategy.AutoRebalanceStrategy;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.common.utils.StringUtil;
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
  public static final String HELIX_CLUSTER_NAME = "controller.helix.cluster.name";
  public static final String CLUSTER_TENANT_ISOLATION_ENABLE = "cluster.tenant.isolation.enable";
  public static final String CONSOLE_WEBAPP_ROOT_PATH = "controller.query.console";
  public static final String CONTROLLER_MODE = "controller.mode";
  public static final String LEAD_CONTROLLER_RESOURCE_REBALANCE_STRATEGY = "controller.resource.rebalance.strategy";

  // Comma separated list of list of packages that contain TableConfigTuners to be added to the registry
  public static final String TABLE_CONFIG_TUNER_PACKAGES = "controller.table.config.tuner.packages";
  public static final String DEFAULT_TABLE_CONFIG_TUNER_PACKAGES = "org.apache.pinot";

  public enum ControllerMode {
    DUAL, PINOT_ONLY, HELIX_ONLY
  }

  public static class ControllerPeriodicTasksConf {

    // frequency configs
    @Deprecated
    public static final String RETENTION_MANAGER_FREQUENCY_IN_SECONDS = "controller.retention.frequencyInSeconds";
    public static final String RETENTION_MANAGER_FREQUENCY_PERIOD = "controller.retention.frequencyPeriod";
    @Deprecated
    // The ValidationManager has been split up into 3 separate tasks, each having their own frequency config settings
    public static final String DEPRECATED_VALIDATION_MANAGER_FREQUENCY_IN_SECONDS =
        "controller.validation.frequencyInSeconds";
    @Deprecated
    public static final String OFFLINE_SEGMENT_INTERVAL_CHECKER_FREQUENCY_IN_SECONDS =
        "controller.offline.segment.interval.checker.frequencyInSeconds";
    public static final String OFFLINE_SEGMENT_INTERVAL_CHECKER_FREQUENCY_PERIOD =
        "controller.offline.segment.interval.checker.frequencyPeriod";
    @Deprecated
    public static final String REALTIME_SEGMENT_VALIDATION_FREQUENCY_IN_SECONDS =
        "controller.realtime.segment.validation.frequencyInSeconds";
    public static final String REALTIME_SEGMENT_VALIDATION_FREQUENCY_PERIOD =
        "controller.realtime.segment.validation.frequencyPeriod";
    @Deprecated
    public static final String REALTIME_SEGMENT_VALIDATION_INITIAL_DELAY_IN_SECONDS =
        "controller.realtime.segment.validation.initialDelayInSeconds";
    public static final String REALTIME_SEGMENT_VALIDATION_INITIAL_DELAY_PERIOD =
        "controller.realtime.segment.validation.initialDelayPeriod";
    @Deprecated
    public static final String BROKER_RESOURCE_VALIDATION_FREQUENCY_IN_SECONDS =
        "controller.broker.resource.validation.frequencyInSeconds";
    public static final String BROKER_RESOURCE_VALIDATION_FREQUENCY_PERIOD =
        "controller.broker.resource.validation.frequencyPeriod";
    @Deprecated
    public static final String BROKER_RESOURCE_VALIDATION_INITIAL_DELAY_IN_SECONDS =
        "controller.broker.resource.validation.initialDelayInSeconds";
    public static final String BROKER_RESOURCE_VALIDATION_INITIAL_DELAY_PERIOD =
        "controller.broker.resource.validation.initialDelayPeriod";
    @Deprecated
    public static final String STATUS_CHECKER_FREQUENCY_IN_SECONDS = "controller.statuschecker.frequencyInSeconds";
    public static final String STATUS_CHECKER_FREQUENCY_PERIOD = "controller.statuschecker.frequencyPeriod";
    @Deprecated
    public static final String STATUS_CHECKER_WAIT_FOR_PUSH_TIME_IN_SECONDS =
        "controller.statuschecker.waitForPushTimeInSeconds";
    public static final String STATUS_CHECKER_WAIT_FOR_PUSH_TIME_PERIOD =
        "controller.statuschecker.waitForPushTimePeriod";
    @Deprecated
    public static final String TASK_MANAGER_FREQUENCY_IN_SECONDS = "controller.task.frequencyInSeconds";
    public static final String TASK_MANAGER_FREQUENCY_PERIOD = "controller.task.frequencyPeriod";
    @Deprecated
    public static final String MINION_INSTANCES_CLEANUP_TASK_FREQUENCY_IN_SECONDS =
        "controller.minion.instances.cleanup.task.frequencyInSeconds";
    public static final String MINION_INSTANCES_CLEANUP_TASK_FREQUENCY_PERIOD =
        "controller.minion.instances.cleanup.task.frequencyPeriod";
    @Deprecated
    public static final String MINION_INSTANCES_CLEANUP_TASK_INITIAL_DELAY_SECONDS =
        "controller.minion.instances.cleanup.task.initialDelaySeconds";
    public static final String MINION_INSTANCES_CLEANUP_TASK_INITIAL_DELAY_PERIOD =
        "controller.minion.instances.cleanup.task.initialDelayPeriod";
    @Deprecated
    public static final String MINION_INSTANCES_CLEANUP_TASK_MIN_OFFLINE_TIME_BEFORE_DELETION_SECONDS =
        "controller.minion.instances.cleanup.task.minOfflineTimeBeforeDeletionSeconds";
    public static final String MINION_INSTANCES_CLEANUP_TASK_MIN_OFFLINE_TIME_BEFORE_DELETION_PERIOD =
        "controller.minion.instances.cleanup.task.minOfflineTimeBeforeDeletionPeriod";
    @Deprecated
    public static final String TASK_METRICS_EMITTER_FREQUENCY_IN_SECONDS =
        "controller.minion.task.metrics.emitter.frequencyInSeconds";
    public static final String TASK_METRICS_EMITTER_FREQUENCY_PERIOD =
        "controller.minion.task.metrics.emitter.frequencyPeriod";

    public static final String PINOT_TASK_MANAGER_SCHEDULER_ENABLED = "controller.task.scheduler.enabled";
    @Deprecated
    // RealtimeSegmentRelocator has been rebranded as SegmentRelocator
    public static final String DEPRECATED_REALTIME_SEGMENT_RELOCATOR_FREQUENCY =
        "controller.realtime.segment.relocator.frequency";
    @Deprecated
    public static final String SEGMENT_RELOCATOR_FREQUENCY_IN_SECONDS =
        "controller.segment.relocator.frequencyInSeconds";
    public static final String SEGMENT_RELOCATOR_FREQUENCY_PERIOD = "controller.segment.relocator.frequencyPeriod";
    // Because segment level validation is expensive and requires heavy ZK access, we run segment level validation with a
    // separate interval
    @Deprecated
    public static final String SEGMENT_LEVEL_VALIDATION_INTERVAL_IN_SECONDS =
        "controller.segment.level.validation.intervalInSeconds";
    public static final String SEGMENT_LEVEL_VALIDATION_INTERVAL_PERIOD =
        "controller.segment.level.validation.intervalPeriod";

    // Initial delays
    @Deprecated
    public static final String STATUS_CHECKER_INITIAL_DELAY_IN_SECONDS =
        "controller.statusChecker.initialDelayInSeconds";
    public static final String STATUS_CHECKER_INITIAL_DELAY_PERIOD = "controller.statusChecker.initialDelayPeriod";

    @Deprecated
    public static final String RETENTION_MANAGER_INITIAL_DELAY_IN_SECONDS =
        "controller.retentionManager.initialDelayInSeconds";
    public static final String RETENTION_MANAGER_INITIAL_DELAY_PERIOD =
        "controller.retentionManager.initialDelayPeriod";

    @Deprecated
    public static final String OFFLINE_SEGMENT_INTERVAL_CHECKER_INITIAL_DELAY_IN_SECONDS =
        "controller.offlineSegmentIntervalChecker.initialDelayInSeconds";
    public static final String OFFLINE_SEGMENT_INTERVAL_CHECKER_INITIAL_DELAY_PERIOD =
        "controller.offlineSegmentIntervalChecker.initialDelayPeriod";
    @Deprecated
    // RealtimeSegmentRelocator has been rebranded as SegmentRelocator
    public static final String DEPRECATED_REALTIME_SEGMENT_RELOCATION_INITIAL_DELAY_IN_SECONDS =
        "controller.realtimeSegmentRelocation.initialDelayInSeconds";
    @Deprecated
    public static final String SEGMENT_RELOCATOR_INITIAL_DELAY_IN_SECONDS =
        "controller.segmentRelocator.initialDelayInSeconds";
    public static final String SEGMENT_RELOCATOR_INITIAL_DELAY_PERIOD =
        "controller.segmentRelocator.initialDelayPeriod";

    public static final int MIN_INITIAL_DELAY_IN_SECONDS = 120;
    public static final int MAX_INITIAL_DELAY_IN_SECONDS = 300;

    private static final Random RANDOM = new Random();

    private static long getRandomInitialDelayInSeconds() {
      return MIN_INITIAL_DELAY_IN_SECONDS + RANDOM.nextInt(MAX_INITIAL_DELAY_IN_SECONDS - MIN_INITIAL_DELAY_IN_SECONDS);
    }

    // Default values
    private static final int DEFAULT_RETENTION_CONTROLLER_FREQUENCY_IN_SECONDS = 6 * 60 * 60; // 6 Hours.
    private static final int DEFAULT_OFFLINE_SEGMENT_INTERVAL_CHECKER_FREQUENCY_IN_SECONDS = 24 * 60 * 60; // 24 Hours.
    private static final int DEFAULT_REALTIME_SEGMENT_VALIDATION_FREQUENCY_IN_SECONDS = 60 * 60; // 1 Hour.
    private static final int DEFAULT_BROKER_RESOURCE_VALIDATION_FREQUENCY_IN_SECONDS = 60 * 60; // 1 Hour.
    private static final int DEFAULT_STATUS_CONTROLLER_FREQUENCY_IN_SECONDS = 5 * 60; // 5 minutes
    private static final int DEFAULT_TASK_METRICS_EMITTER_FREQUENCY_IN_SECONDS = 5 * 60; // 5 minutes
    private static final int DEFAULT_STATUS_CONTROLLER_WAIT_FOR_PUSH_TIME_IN_SECONDS = 10 * 60; // 10 minutes
    private static final int DEFAULT_TASK_MANAGER_FREQUENCY_IN_SECONDS = -1; // Disabled
    private static final int DEFAULT_MINION_INSTANCES_CLEANUP_TASK_FREQUENCY_IN_SECONDS = 60 * 60; // 1 Hour.
    private static final int DEFAULT_MINION_INSTANCES_CLEANUP_TASK_MIN_OFFLINE_TIME_BEFORE_DELETION_IN_SECONDS =
        60 * 60; // 1 Hour.

    private static final int DEFAULT_SEGMENT_LEVEL_VALIDATION_INTERVAL_IN_SECONDS = 24 * 60 * 60;
    private static final int DEFAULT_SEGMENT_RELOCATOR_FREQUENCY_IN_SECONDS = 60 * 60;
  }

  @Deprecated
  private static final String SERVER_ADMIN_REQUEST_TIMEOUT_SECONDS = "server.request.timeoutSeconds";
  private static final String SERVER_ADMIN_REQUEST_TIMEOUT_PERIOD = "server.request.timeoutPeriod";

  @Deprecated
  private static final String SEGMENT_COMMIT_TIMEOUT_SECONDS = "controller.realtime.segment.commit.timeoutSeconds";
  private static final String SEGMENT_COMMIT_TIMEOUT_PERIOD = "controller.realtime.segment.commit.timeoutPeriod";

  private static final String DELETED_SEGMENTS_RETENTION_IN_DAYS = "controller.deleted.segments.retentionInDays";

  public static final String TABLE_MIN_REPLICAS = "table.minReplicas";
  public static final String ENABLE_SPLIT_COMMIT = "controller.enable.split.commit";
  private static final String JERSEY_ADMIN_API_PORT = "jersey.admin.api.port";
  private static final String JERSEY_ADMIN_IS_PRIMARY = "jersey.admin.isprimary";
  public static final String ACCESS_CONTROL_FACTORY_CLASS = "controller.admin.access.control.factory.class";
  // Amount of the time the segment can take from the beginning of upload to the end of upload. Used when parallel push
  // protection is enabled. If the upload does not finish within the timeout, next upload can override the previous one.
  private static final String SEGMENT_UPLOAD_TIMEOUT_IN_MILLIS = "controller.segment.upload.timeoutInMillis";
  private static final String REALTIME_SEGMENT_METADATA_COMMIT_NUMLOCKS =
      "controller.realtime.segment.metadata.commit.numLocks";
  private static final String ENABLE_STORAGE_QUOTA_CHECK = "controller.enable.storage.quota.check";
  private static final String ENABLE_BATCH_MESSAGE_MODE = "controller.enable.batch.message.mode";
  // It is used to disable the HLC realtime segment completion and disallow HLC table in the cluster. True by default.
  // If it's set to false, existing HLC realtime tables will stop consumption, and creation of new HLC tables will be disallowed.
  // Please make sure there is no HLC table running in the cluster before disallowing it.
  public static final String ALLOW_HLC_TABLES = "controller.allow.hlc.tables";
  public static final String DIM_TABLE_MAX_SIZE = "controller.dimTable.maxSize";

  // Defines the kind of storage and the underlying PinotFS implementation
  private static final String PINOT_FS_FACTORY_CLASS_LOCAL = "controller.storage.factory.class.file";

  private static final int DEFAULT_SERVER_ADMIN_REQUEST_TIMEOUT_SECONDS = 30;
  private static final int DEFAULT_DELETED_SEGMENTS_RETENTION_IN_DAYS = 7;
  private static final int DEFAULT_TABLE_MIN_REPLICAS = 1;
  private static final boolean DEFAULT_ENABLE_SPLIT_COMMIT = false;
  private static final int DEFAULT_JERSEY_ADMIN_PORT = 21000;
  private static final String DEFAULT_ACCESS_CONTROL_FACTORY_CLASS =
      "org.apache.pinot.controller.api.access.AllowAllAccessFactory";
  private static final long DEFAULT_SEGMENT_UPLOAD_TIMEOUT_IN_MILLIS = 600_000L; // 10 minutes
  private static final int DEFAULT_REALTIME_SEGMENT_METADATA_COMMIT_NUMLOCKS = 64;
  private static final boolean DEFAULT_ENABLE_STORAGE_QUOTA_CHECK = true;
  private static final boolean DEFAULT_ENABLE_BATCH_MESSAGE_MODE = false;
  private static final boolean DEFAULT_ALLOW_HLC_TABLES = true;
  private static final String DEFAULT_CONTROLLER_MODE = ControllerMode.DUAL.name();
  private static final String DEFAULT_LEAD_CONTROLLER_RESOURCE_REBALANCE_STRATEGY =
      AutoRebalanceStrategy.class.getName();
  private static final String DEFAULT_DIM_TABLE_MAX_SIZE = "200M";

  private static final String DEFAULT_PINOT_FS_FACTORY_CLASS_LOCAL = LocalPinotFS.class.getName();

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
      pinotFSFactoryClasses.getKeys()
          .forEachRemaining(key -> setProperty((String) key, pinotFSFactoryClasses.getProperty((String) key)));
    }
  }

  public void setSplitCommit(boolean isSplitCommit) {
    setProperty(ENABLE_SPLIT_COMMIT, isSplitCommit);
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
    setProperty(DATA_DIR, dataDir);
  }

  public void setRealtimeSegmentCommitTimeoutSeconds(int timeoutSec) {
    setProperty(SEGMENT_COMMIT_TIMEOUT_SECONDS, Integer.toString(timeoutSec));
  }

  public void setUpdateSegmentStateModel(String updateStateModel) {
    setProperty(UPDATE_SEGMENT_STATE_MODEL, updateStateModel);
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
    return getProperty(CONTROLLER_ACCESS_PROTOCOLS,
        getControllerPort() == null ? Arrays.asList("http") : Arrays.asList());
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
    return Optional.ofNullable(getProperty(SEGMENT_COMMIT_TIMEOUT_PERIOD, String.class))
        .map(period -> (int) convertPeriodToSeconds(period)).orElseGet(() -> getProperty(SEGMENT_COMMIT_TIMEOUT_SECONDS,
            SegmentCompletionProtocol.getDefaultMaxSegmentCommitTimeSeconds()));
  }

  public boolean isUpdateSegmentStateModel() {
    return getProperty(UPDATE_SEGMENT_STATE_MODEL, false);
  }

  public String generateVipUrl() {
    return getControllerVipProtocol() + "://" + getControllerVipHost() + ":" + getControllerVipPort();
  }

  public String getZkStr() {
    Object zkAddressObj = containsKey(CommonConstants.Helix.CONFIG_OF_ZOOKEEPR_SERVER) ? getProperty(
        CommonConstants.Helix.CONFIG_OF_ZOOKEEPR_SERVER) : getProperty(ZK_STR);

    // The set method converted comma separated string into ArrayList, so need to convert back to String here.
    if (zkAddressObj instanceof List) {
      List<String> zkAddressList = (List<String>) zkAddressObj;
      String[] zkAddress = zkAddressList.toArray(new String[0]);
      return StringUtil.join(",", zkAddress);
    } else if (zkAddressObj instanceof String) {
      return (String) zkAddressObj;
    } else {
      throw new RuntimeException(
          "Unexpected data type for zkAddress PropertiesConfiguration, expecting String but got " + zkAddressObj
              .getClass().getName());
    }
  }

  @Override
  public String toString() {
    return super.toString();
  }

  public boolean getAcceptSplitCommit() {
    return getProperty(ENABLE_SPLIT_COMMIT, DEFAULT_ENABLE_SPLIT_COMMIT);
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
    return Optional
        .ofNullable(getProperty(ControllerPeriodicTasksConf.RETENTION_MANAGER_FREQUENCY_PERIOD, String.class))
        .map(period -> (int) convertPeriodToSeconds(period)).orElseGet(
            () -> getProperty(ControllerPeriodicTasksConf.RETENTION_MANAGER_FREQUENCY_IN_SECONDS,
                ControllerPeriodicTasksConf.DEFAULT_RETENTION_CONTROLLER_FREQUENCY_IN_SECONDS));
  }

  public void setRetentionControllerFrequencyInSeconds(int retentionFrequencyInSeconds) {
    setProperty(ControllerPeriodicTasksConf.RETENTION_MANAGER_FREQUENCY_IN_SECONDS,
        Integer.toString(retentionFrequencyInSeconds));
  }

  /**
   * Returns the config value for controller.offline.segment.interval.checker.frequencyInSeconds if
   * it exists. If it doesn't exist, returns the segment level validation interval. This is done in
   * order to retain the current behavior, wherein the offline validation tasks were done at segment
   * level validation interval frequency The default value is the new
   * DEFAULT_OFFLINE_SEGMENT_INTERVAL_CHECKER_FREQUENCY_IN_SECONDS
   *
   * @return
   */
  public int getOfflineSegmentIntervalCheckerFrequencyInSeconds() {
    return Optional.ofNullable(
        getProperty(ControllerPeriodicTasksConf.OFFLINE_SEGMENT_INTERVAL_CHECKER_FREQUENCY_PERIOD, String.class))
        .map(period -> (int) convertPeriodToSeconds(period)).orElseGet(
            () -> getProperty(ControllerPeriodicTasksConf.OFFLINE_SEGMENT_INTERVAL_CHECKER_FREQUENCY_IN_SECONDS,
                ControllerPeriodicTasksConf.DEFAULT_OFFLINE_SEGMENT_INTERVAL_CHECKER_FREQUENCY_IN_SECONDS));
  }

  public void setOfflineSegmentIntervalCheckerFrequencyInSeconds(int validationFrequencyInSeconds) {
    setProperty(ControllerPeriodicTasksConf.OFFLINE_SEGMENT_INTERVAL_CHECKER_FREQUENCY_IN_SECONDS,
        Integer.toString(validationFrequencyInSeconds));
  }

  /**
   * Returns the config value for controller.realtime.segment.validation.frequencyInSeconds if it
   * exists. If it doesn't exist, returns the validation controller frequency. This is done in order
   * to retain the current behavior, wherein the realtime validation tasks were done at validation
   * controller frequency The default value is the new DEFAULT_REALTIME_SEGMENT_VALIDATION_FREQUENCY_IN_SECONDS
   *
   * @return
   */
  public int getRealtimeSegmentValidationFrequencyInSeconds() {
    return Optional
        .ofNullable(getProperty(ControllerPeriodicTasksConf.REALTIME_SEGMENT_VALIDATION_FREQUENCY_PERIOD, String.class))
        .map(period -> (int) convertPeriodToSeconds(period)).orElseGet(() -> {
          Integer frequency =
              getProperty(ControllerPeriodicTasksConf.REALTIME_SEGMENT_VALIDATION_FREQUENCY_IN_SECONDS, Integer.class);
          //fallback to the default value if frequency is not supplied
          if (null == frequency) {
            return getProperty(ControllerPeriodicTasksConf.DEPRECATED_VALIDATION_MANAGER_FREQUENCY_IN_SECONDS,
                ControllerPeriodicTasksConf.DEFAULT_REALTIME_SEGMENT_VALIDATION_FREQUENCY_IN_SECONDS);
          }
          return frequency;
        });
  }

  public void setRealtimeSegmentValidationFrequencyInSeconds(int validationFrequencyInSeconds) {
    setProperty(ControllerPeriodicTasksConf.REALTIME_SEGMENT_VALIDATION_FREQUENCY_IN_SECONDS,
        Integer.toString(validationFrequencyInSeconds));
  }

  /**
   * Returns the config value for  controller.broker.resource.validation.frequencyInSeconds if it
   * exists. If it doesn't exist, returns the validation controller frequency. This is done in order
   * to retain the current behavior, wherein the broker resource validation tasks were done at
   * validation controller frequency The default value is the new DEFAULT_BROKER_RESOURCE_VALIDATION_FREQUENCY_IN_SECONDS
   *
   * @return
   */
  public int getBrokerResourceValidationFrequencyInSeconds() {
    return Optional
        .ofNullable(getProperty(ControllerPeriodicTasksConf.BROKER_RESOURCE_VALIDATION_FREQUENCY_PERIOD, String.class))
        .map(period -> (int) convertPeriodToSeconds(period)).orElseGet(() -> {
          Integer frequency =
              getProperty(ControllerPeriodicTasksConf.BROKER_RESOURCE_VALIDATION_FREQUENCY_IN_SECONDS, Integer.class);
          //fallback to the default value if frequency is not supplied
          if (null == frequency) {
            return getProperty(ControllerPeriodicTasksConf.DEPRECATED_VALIDATION_MANAGER_FREQUENCY_IN_SECONDS,
                ControllerPeriodicTasksConf.DEFAULT_BROKER_RESOURCE_VALIDATION_FREQUENCY_IN_SECONDS);
          }
          return frequency;
        });
  }

  public void setBrokerResourceValidationFrequencyInSeconds(int validationFrequencyInSeconds) {
    setProperty(ControllerPeriodicTasksConf.BROKER_RESOURCE_VALIDATION_FREQUENCY_IN_SECONDS,
        Integer.toString(validationFrequencyInSeconds));
  }

  public long getBrokerResourceValidationInitialDelayInSeconds() {
    return Optional.ofNullable(
        getProperty(ControllerPeriodicTasksConf.BROKER_RESOURCE_VALIDATION_INITIAL_DELAY_PERIOD, String.class))
        .map(this::convertPeriodToSeconds).orElseGet(
            () -> getProperty(ControllerPeriodicTasksConf.BROKER_RESOURCE_VALIDATION_INITIAL_DELAY_IN_SECONDS,
                getPeriodicTaskInitialDelayInSeconds()));
  }

  public int getStatusCheckerFrequencyInSeconds() {
    return Optional.ofNullable(getProperty(ControllerPeriodicTasksConf.STATUS_CHECKER_FREQUENCY_PERIOD, String.class))
        .map(period -> (int) convertPeriodToSeconds(period)).orElseGet(
            () -> getProperty(ControllerPeriodicTasksConf.STATUS_CHECKER_FREQUENCY_IN_SECONDS,
                ControllerPeriodicTasksConf.DEFAULT_STATUS_CONTROLLER_FREQUENCY_IN_SECONDS));
  }

  public void setStatusCheckerFrequencyInSeconds(int statusCheckerFrequencyInSeconds) {
    setProperty(ControllerPeriodicTasksConf.STATUS_CHECKER_FREQUENCY_IN_SECONDS,
        Integer.toString(statusCheckerFrequencyInSeconds));
  }

  public int getTaskMetricsEmitterFrequencyInSeconds() {
    return Optional
        .ofNullable(getProperty(ControllerPeriodicTasksConf.TASK_METRICS_EMITTER_FREQUENCY_PERIOD, String.class))
        .map(period -> (int) convertPeriodToSeconds(period)).orElseGet(
            () -> getProperty(ControllerPeriodicTasksConf.TASK_METRICS_EMITTER_FREQUENCY_IN_SECONDS,
                ControllerPeriodicTasksConf.DEFAULT_TASK_METRICS_EMITTER_FREQUENCY_IN_SECONDS));
  }

  public void setTaskMetricsEmitterFrequencyInSeconds(int taskMetricsEmitterFrequencyInSeconds) {
    setProperty(ControllerPeriodicTasksConf.TASK_METRICS_EMITTER_FREQUENCY_IN_SECONDS,
        Integer.toString(taskMetricsEmitterFrequencyInSeconds));
  }

  public int getStatusCheckerWaitForPushTimeInSeconds() {
    return Optional
        .ofNullable(getProperty(ControllerPeriodicTasksConf.STATUS_CHECKER_WAIT_FOR_PUSH_TIME_PERIOD, String.class))
        .map(period -> (int) convertPeriodToSeconds(period)).orElseGet(
            () -> getProperty(ControllerPeriodicTasksConf.STATUS_CHECKER_WAIT_FOR_PUSH_TIME_IN_SECONDS,
                ControllerPeriodicTasksConf.DEFAULT_STATUS_CONTROLLER_WAIT_FOR_PUSH_TIME_IN_SECONDS));
  }

  public void setStatusCheckerWaitForPushTimeInSeconds(int statusCheckerWaitForPushTimeInSeconds) {
    setProperty(ControllerPeriodicTasksConf.STATUS_CHECKER_WAIT_FOR_PUSH_TIME_IN_SECONDS,
        Integer.toString(statusCheckerWaitForPushTimeInSeconds));
  }

  /**
   * RealtimeSegmentRelocator has been rebranded to SegmentRelocator. Check for
   * SEGMENT_RELOCATOR_FREQUENCY_IN_SECONDS property, if not found, return
   * REALTIME_SEGMENT_RELOCATOR_FREQUENCY
   */
  public int getSegmentRelocatorFrequencyInSeconds() {
    return Optional
        .ofNullable(getProperty(ControllerPeriodicTasksConf.SEGMENT_RELOCATOR_FREQUENCY_PERIOD, String.class))
        .map(period -> (int) convertPeriodToSeconds(period)).orElseGet(() -> {
          Integer segmentRelocatorFreqSeconds =
              getProperty(ControllerPeriodicTasksConf.SEGMENT_RELOCATOR_FREQUENCY_IN_SECONDS, Integer.class);
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
    setProperty(ControllerPeriodicTasksConf.SEGMENT_RELOCATOR_FREQUENCY_IN_SECONDS,
        Integer.toString(segmentRelocatorFrequencyInSeconds));
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
    return Optional.ofNullable(getProperty(SERVER_ADMIN_REQUEST_TIMEOUT_PERIOD, String.class))
        .map(period -> (int) TimeUnit.SECONDS.convert(TimeUtils.convertPeriodToMillis(period), TimeUnit.MILLISECONDS))
        .orElseGet(
            () -> getProperty(SERVER_ADMIN_REQUEST_TIMEOUT_SECONDS, DEFAULT_SERVER_ADMIN_REQUEST_TIMEOUT_SECONDS));
  }

  public int getDeletedSegmentsRetentionInDays() {
    return getProperty(DELETED_SEGMENTS_RETENTION_IN_DAYS, DEFAULT_DELETED_SEGMENTS_RETENTION_IN_DAYS);
  }

  public void setDeletedSegmentsRetentionInDays(int retentionInDays) {
    setProperty(DELETED_SEGMENTS_RETENTION_IN_DAYS, retentionInDays);
  }

  public int getTaskManagerFrequencyInSeconds() {
    return Optional.ofNullable(getProperty(ControllerPeriodicTasksConf.TASK_MANAGER_FREQUENCY_PERIOD, String.class))
        .map(period -> (int) convertPeriodToSeconds(period)).orElseGet(
            () -> getProperty(ControllerPeriodicTasksConf.TASK_MANAGER_FREQUENCY_IN_SECONDS,
                ControllerPeriodicTasksConf.DEFAULT_TASK_MANAGER_FREQUENCY_IN_SECONDS));
  }

  public void setTaskManagerFrequencyInSeconds(int frequencyInSeconds) {
    setProperty(ControllerPeriodicTasksConf.TASK_MANAGER_FREQUENCY_IN_SECONDS, Integer.toString(frequencyInSeconds));
  }

  public long getMinionInstancesCleanupTaskFrequencyInSeconds() {
    return Optional.ofNullable(
        getProperty(ControllerPeriodicTasksConf.MINION_INSTANCES_CLEANUP_TASK_FREQUENCY_PERIOD, String.class))
        .map(this::convertPeriodToSeconds).orElseGet(
            () -> (long) getProperty(ControllerPeriodicTasksConf.MINION_INSTANCES_CLEANUP_TASK_FREQUENCY_IN_SECONDS,
                ControllerPeriodicTasksConf.DEFAULT_MINION_INSTANCES_CLEANUP_TASK_FREQUENCY_IN_SECONDS));
  }

  public void setMinionInstancesCleanupTaskFrequencyInSeconds(int frequencyInSeconds) {
    setProperty(ControllerPeriodicTasksConf.MINION_INSTANCES_CLEANUP_TASK_FREQUENCY_IN_SECONDS,
        Integer.toString(frequencyInSeconds));
  }

  public long getMinionInstancesCleanupTaskInitialDelaySeconds() {
    return Optional
        .ofNullable(getProperty(ControllerPeriodicTasksConf.MINION_INSTANCES_CLEANUP_TASK_INITIAL_DELAY_PERIOD, String.class))
        .map(this::convertPeriodToSeconds).orElseGet(
            () -> getProperty(ControllerPeriodicTasksConf.MINION_INSTANCES_CLEANUP_TASK_INITIAL_DELAY_SECONDS,
                ControllerPeriodicTasksConf.getRandomInitialDelayInSeconds()));
  }

  public void setMinionInstancesCleanupTaskInitialDelaySeconds(int initialDelaySeconds) {
    setProperty(ControllerPeriodicTasksConf.MINION_INSTANCES_CLEANUP_TASK_INITIAL_DELAY_SECONDS,
        Integer.toString(initialDelaySeconds));
  }

  public int getMinionInstancesCleanupTaskMinOfflineTimeBeforeDeletionInSeconds() {
    return Optional.ofNullable(
        getProperty(ControllerPeriodicTasksConf.MINION_INSTANCES_CLEANUP_TASK_MIN_OFFLINE_TIME_BEFORE_DELETION_PERIOD,
            String.class)).map(period -> (int) convertPeriodToSeconds(period)).orElseGet(() -> getProperty(
        ControllerPeriodicTasksConf.MINION_INSTANCES_CLEANUP_TASK_MIN_OFFLINE_TIME_BEFORE_DELETION_SECONDS,
        ControllerPeriodicTasksConf.DEFAULT_MINION_INSTANCES_CLEANUP_TASK_MIN_OFFLINE_TIME_BEFORE_DELETION_IN_SECONDS));
  }

  public void setMinionInstancesCleanupTaskMinOfflineTimeBeforeDeletionInSeconds(int maxOfflineTimeRangeInSeconds) {
    setProperty(ControllerPeriodicTasksConf.MINION_INSTANCES_CLEANUP_TASK_MIN_OFFLINE_TIME_BEFORE_DELETION_SECONDS,
        Integer.toString(maxOfflineTimeRangeInSeconds));
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

  public String getAccessControlFactoryClass() {
    return getProperty(ACCESS_CONTROL_FACTORY_CLASS, DEFAULT_ACCESS_CONTROL_FACTORY_CLASS);
  }

  public void setAccessControlFactoryClass(String accessControlFactoryClass) {
    setProperty(ACCESS_CONTROL_FACTORY_CLASS, accessControlFactoryClass);
  }

  public long getSegmentUploadTimeoutInMillis() {
    return getProperty(SEGMENT_UPLOAD_TIMEOUT_IN_MILLIS, DEFAULT_SEGMENT_UPLOAD_TIMEOUT_IN_MILLIS);
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

  public boolean getEnableBatchMessageMode() {
    return getProperty(ENABLE_BATCH_MESSAGE_MODE, DEFAULT_ENABLE_BATCH_MESSAGE_MODE);
  }

  public int getSegmentLevelValidationIntervalInSeconds() {
    return Optional
        .ofNullable(getProperty(ControllerPeriodicTasksConf.SEGMENT_LEVEL_VALIDATION_INTERVAL_PERIOD, String.class))
        .map(period -> (int) convertPeriodToSeconds(period)).orElseGet(
            () -> getProperty(ControllerPeriodicTasksConf.SEGMENT_LEVEL_VALIDATION_INTERVAL_IN_SECONDS,
                ControllerPeriodicTasksConf.DEFAULT_SEGMENT_LEVEL_VALIDATION_INTERVAL_IN_SECONDS));
  }

  public long getStatusCheckerInitialDelayInSeconds() {
    return Optional
        .ofNullable(getProperty(ControllerPeriodicTasksConf.STATUS_CHECKER_INITIAL_DELAY_PERIOD, String.class))
        .map(this::convertPeriodToSeconds).orElseGet(
            () -> getProperty(ControllerPeriodicTasksConf.STATUS_CHECKER_INITIAL_DELAY_IN_SECONDS,
                ControllerPeriodicTasksConf.getRandomInitialDelayInSeconds()));
  }

  public long getRetentionManagerInitialDelayInSeconds() {
    return Optional
        .ofNullable(getProperty(ControllerPeriodicTasksConf.RETENTION_MANAGER_INITIAL_DELAY_PERIOD, String.class))
        .map(this::convertPeriodToSeconds).orElseGet(
            () -> getProperty(ControllerPeriodicTasksConf.RETENTION_MANAGER_INITIAL_DELAY_IN_SECONDS,
                ControllerPeriodicTasksConf.getRandomInitialDelayInSeconds()));
  }

  public long getOfflineSegmentIntervalCheckerInitialDelayInSeconds() {
    return Optional.ofNullable(
        getProperty(ControllerPeriodicTasksConf.OFFLINE_SEGMENT_INTERVAL_CHECKER_INITIAL_DELAY_PERIOD, String.class))
        .map(this::convertPeriodToSeconds).orElseGet(
            () -> getProperty(ControllerPeriodicTasksConf.OFFLINE_SEGMENT_INTERVAL_CHECKER_INITIAL_DELAY_IN_SECONDS,
                ControllerPeriodicTasksConf.getRandomInitialDelayInSeconds()));
  }

  public long getRealtimeSegmentValidationManagerInitialDelaySeconds() {
    return Optional.ofNullable(
        getProperty(ControllerPeriodicTasksConf.REALTIME_SEGMENT_VALIDATION_INITIAL_DELAY_PERIOD, String.class))
        .map(this::convertPeriodToSeconds).orElseGet(
            () -> getProperty(ControllerPeriodicTasksConf.REALTIME_SEGMENT_VALIDATION_INITIAL_DELAY_IN_SECONDS,
                getPeriodicTaskInitialDelayInSeconds()));
  }

  public long getPinotTaskManagerInitialDelaySeconds() {
    return getPeriodicTaskInitialDelayInSeconds();
  }

  public boolean isPinotTaskManagerSchedulerEnabled() {
    return getProperty(ControllerPeriodicTasksConf.PINOT_TASK_MANAGER_SCHEDULER_ENABLED, false);
  }

  /**
   * RealtimeSegmentRelocator has been rebranded to SegmentRelocator. Check for
   * SEGMENT_RELOCATOR_INITIAL_DELAY_IN_SECONDS property, if not found, return
   * REALTIME_SEGMENT_RELOCATION_INITIAL_DELAY_IN_SECONDS
   */
  public long getSegmentRelocatorInitialDelayInSeconds() {
    return Optional
        .ofNullable(getProperty(ControllerPeriodicTasksConf.SEGMENT_RELOCATOR_INITIAL_DELAY_PERIOD, String.class))
        .map(period -> TimeUnit.SECONDS.convert(TimeUtils.convertPeriodToMillis(period), TimeUnit.MILLISECONDS))
        .orElseGet(() -> {
          Long segmentRelocatorInitialDelaySeconds =
              getProperty(ControllerPeriodicTasksConf.SEGMENT_RELOCATOR_INITIAL_DELAY_IN_SECONDS, Long.class);
          if (segmentRelocatorInitialDelaySeconds == null) {
            segmentRelocatorInitialDelaySeconds =
                getProperty(ControllerPeriodicTasksConf.DEPRECATED_REALTIME_SEGMENT_RELOCATION_INITIAL_DELAY_IN_SECONDS,
                    ControllerPeriodicTasksConf.getRandomInitialDelayInSeconds());
          }
          return segmentRelocatorInitialDelaySeconds;
        });
  }

  public long getPeriodicTaskInitialDelayInSeconds() {
    return ControllerPeriodicTasksConf.getRandomInitialDelayInSeconds();
  }

  public void setControllerMode(ControllerMode controllerMode) {
    setProperty(CONTROLLER_MODE, controllerMode.name());
  }

  public ControllerMode getControllerMode() {
    return ControllerMode.valueOf(getProperty(CONTROLLER_MODE, DEFAULT_CONTROLLER_MODE.toString()).toUpperCase());
  }

  public void setLeadControllerResourceRebalanceStrategy(String rebalanceStrategy) {
    setProperty(LEAD_CONTROLLER_RESOURCE_REBALANCE_STRATEGY, rebalanceStrategy);
  }

  public String getLeadControllerResourceRebalanceStrategy() {
    return getProperty(LEAD_CONTROLLER_RESOURCE_REBALANCE_STRATEGY,
        DEFAULT_LEAD_CONTROLLER_RESOURCE_REBALANCE_STRATEGY);
  }

  public boolean getHLCTablesAllowed() {
    return getProperty(ALLOW_HLC_TABLES, DEFAULT_ALLOW_HLC_TABLES);
  }

  public void setHLCTablesAllowed(boolean allowHLCTables) {
    setProperty(ALLOW_HLC_TABLES, allowHLCTables);
  }

  public String getMetricsPrefix() {
    return getProperty(CONFIG_OF_CONTROLLER_METRICS_PREFIX, DEFAULT_METRICS_PREFIX);
  }

  public int getControllerBrokerPortOverride() {
    return getProperty(CONTROLLER_BROKER_PORT_OVERRIDE, -1);
  }

  public List<String> getTableConfigTunerPackages() {
    return Arrays
        .asList(getProperty(TABLE_CONFIG_TUNER_PACKAGES, DEFAULT_TABLE_CONFIG_TUNER_PACKAGES).split("\\s*,\\s*"));
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
}
