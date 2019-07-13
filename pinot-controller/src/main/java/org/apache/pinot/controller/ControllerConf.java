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

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.filesystem.LocalPinotFS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ControllerConf extends PropertiesConfiguration {
  private static final Logger LOGGER = LoggerFactory.getLogger(ControllerConf.class);

  private static final String CONTROLLER_VIP_HOST = "controller.vip.host";
  private static final String CONTROLLER_VIP_PORT = "controller.vip.port";
  private static final String CONTROLLER_VIP_PROTOCOL = "controller.vip.protocol";
  private static final String CONTROLLER_HOST = "controller.host";
  private static final String CONTROLLER_PORT = "controller.port";
  private static final String DATA_DIR = "controller.data.dir";
  // Potentially same as data dir if local
  private static final String LOCAL_TEMP_DIR = "controller.local.temp.dir";
  private static final String ZK_STR = "controller.zk.str";
  // boolean: Update the statemodel on boot?
  private static final String UPDATE_SEGMENT_STATE_MODEL = "controller.update_segment_state_model";
  private static final String HELIX_CLUSTER_NAME = "controller.helix.cluster.name";
  private static final String CLUSTER_TENANT_ISOLATION_ENABLE = "cluster.tenant.isolation.enable";
  private static final String CONSOLE_WEBAPP_ROOT_PATH = "controller.query.console";
  private static final String CONSOLE_WEBAPP_USE_HTTPS = "controller.query.console.useHttps";
  private static final String EXTERNAL_VIEW_ONLINE_TO_OFFLINE_TIMEOUT = "controller.upload.onlineToOfflineTimeout";
  private static final String CONTROLLER_MODE = "controller.mode";

  public enum ControllerMode {
    DUAL, PINOT_ONLY, HELIX_ONLY
  }

  public static class ControllerPeriodicTasksConf {
    // frequency configs
    private static final String RETENTION_MANAGER_FREQUENCY_IN_SECONDS = "controller.retention.frequencyInSeconds";
    @Deprecated
    // The ValidationManager has been split up into 3 separate tasks, each having their own frequency config settings
    private static final String DEPRECATED_VALIDATION_MANAGER_FREQUENCY_IN_SECONDS =
        "controller.validation.frequencyInSeconds";
    private static final String OFFLINE_SEGMENT_INTERVAL_CHECKER_FREQUENCY_IN_SECONDS =
        "controller.offline.segment.interval.checker.frequencyInSeconds";
    private static final String REALTIME_SEGMENT_VALIDATION_FREQUENCY_IN_SECONDS =
        "controller.realtime.segment.validation.frequencyInSeconds";
    private static final String BROKER_RESOURCE_VALIDATION_FREQUENCY_IN_SECONDS =
        "controller.broker.resource.validation.frequencyInSeconds";
    private static final String BROKER_RESOURCE_VALIDATION_INITIAL_DELAY_IN_SECONDS =
        "controller.broker.resource.validation.initialDelayInSeconds";
    private static final String STATUS_CHECKER_FREQUENCY_IN_SECONDS = "controller.statuschecker.frequencyInSeconds";
    private static final String STATUS_CHECKER_WAIT_FOR_PUSH_TIME_IN_SECONDS =
        "controller.statuschecker.waitForPushTimeInSeconds";
    private static final String TASK_MANAGER_FREQUENCY_IN_SECONDS = "controller.task.frequencyInSeconds";
    private static final String REALTIME_SEGMENT_RELOCATOR_FREQUENCY =
        "controller.realtime.segment.relocator.frequency";
    // Because segment level validation is expensive and requires heavy ZK access, we run segment level validation with a
    // separate interval
    private static final String SEGMENT_LEVEL_VALIDATION_INTERVAL_IN_SECONDS =
        "controller.segment.level.validation.intervalInSeconds";

    // Initial delays
    private static final String STATUS_CHECKER_INITIAL_DELAY_IN_SECONDS =
        "controller.statusChecker.initialDelayInSeconds";
    private static final String RETENTION_MANAGER_INITIAL_DELAY_IN_SECONDS =
        "controller.retentionManager.initialDelayInSeconds";
    private static final String OFFLINE_SEGMENT_INTERVAL_CHECKER_INITIAL_DELAY_IN_SECONDS =
        "controller.offlineSegmentIntervalChecker.initialDelayInSeconds";
    private static final String REALTIME_SEGMENT_RELOCATION_INITIAL_DELAY_IN_SECONDS =
        "controller.realtimeSegmentRelocation.initialDelayInSeconds";

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
    private static final int DEFAULT_STATUS_CONTROLLER_WAIT_FOR_PUSH_TIME_IN_SECONDS = 10 * 60; // 10 minutes
    private static final int DEFAULT_TASK_MANAGER_FREQUENCY_IN_SECONDS = -1; // Disabled
    private static final String DEFAULT_REALTIME_SEGMENT_RELOCATOR_FREQUENCY = "1h"; // 1 hour
    private static final int DEFAULT_SEGMENT_LEVEL_VALIDATION_INTERVAL_IN_SECONDS = 24 * 60 * 60;
  }

  private static final String SERVER_ADMIN_REQUEST_TIMEOUT_SECONDS = "server.request.timeoutSeconds";
  private static final String SEGMENT_COMMIT_TIMEOUT_SECONDS = "controller.realtime.segment.commit.timeoutSeconds";
  private static final String DELETED_SEGMENTS_RETENTION_IN_DAYS = "controller.deleted.segments.retentionInDays";
  private static final String TABLE_MIN_REPLICAS = "table.minReplicas";
  private static final String ENABLE_SPLIT_COMMIT = "controller.enable.split.commit";
  private static final String JERSEY_ADMIN_API_PORT = "jersey.admin.api.port";
  private static final String JERSEY_ADMIN_IS_PRIMARY = "jersey.admin.isprimary";
  private static final String ACCESS_CONTROL_FACTORY_CLASS = "controller.admin.access.control.factory.class";
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
  private static final String ALLOW_HLC_TABLES = "controller.allow.hlc.tables";

  // Defines the kind of storage and the underlying PinotFS implementation
  private static final String PINOT_FS_FACTORY_CLASS_PREFIX = "controller.storage.factory.class";
  private static final String PINOT_FS_FACTORY_CLASS_LOCAL = "controller.storage.factory.class.file";

  private static final long DEFAULT_EXTERNAL_VIEW_ONLINE_TO_OFFLINE_TIMEOUT_MILLIS = 120_000L; // 2 minutes
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

  private static final String DEFAULT_PINOT_FS_FACTORY_CLASS_LOCAL = LocalPinotFS.class.getName();

  public ControllerConf(File file)
      throws ConfigurationException {
    super(file);
  }

  public ControllerConf() {
    super();
  }

  public void setLocalTempDir(String localTempDir) {
    setProperty(LOCAL_TEMP_DIR, localTempDir);
  }

  public String getLocalTempDir() {
    return getString(LOCAL_TEMP_DIR, getDataDir());
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
    if (containsKey(CONSOLE_WEBAPP_ROOT_PATH)) {
      return (String) getProperty(CONSOLE_WEBAPP_ROOT_PATH);
    }
    return ControllerConf.class.getClassLoader().getResource("webapp").toExternalForm();
  }

  public void setQueryConsoleUseHttps(boolean useHttps) {
    setProperty(CONSOLE_WEBAPP_USE_HTTPS, useHttps);
  }

  public boolean getQueryConsoleUseHttps() {
    return containsKey(CONSOLE_WEBAPP_USE_HTTPS) && getBoolean(CONSOLE_WEBAPP_USE_HTTPS);
  }

  public void setJerseyAdminPrimary(String jerseyAdminPrimary) {
    setProperty(JERSEY_ADMIN_IS_PRIMARY, jerseyAdminPrimary);
  }

  public void setHelixClusterName(String clusterName) {
    setProperty(HELIX_CLUSTER_NAME, clusterName);
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
    setProperty(ZK_STR, zkStr);
  }

  // A boolean to decide whether Jersey API should be the primary one. For now, we set this to be false,
  // but we turn it on to true when we are sure that jersey api has no backward compatibility problems.
  public boolean isJerseyAdminPrimary() {
    return getBoolean(JERSEY_ADMIN_IS_PRIMARY, true);
  }

  public String getHelixClusterName() {
    return (String) getProperty(HELIX_CLUSTER_NAME);
  }

  public String getControllerHost() {
    return (String) getProperty(CONTROLLER_HOST);
  }

  public String getControllerPort() {
    return (String) getProperty(CONTROLLER_PORT);
  }

  public String getDataDir() {
    return (String) getProperty(DATA_DIR);
  }

  public int getSegmentCommitTimeoutSeconds() {
    if (containsKey(SEGMENT_COMMIT_TIMEOUT_SECONDS)) {
      return Integer.parseInt((String) getProperty(SEGMENT_COMMIT_TIMEOUT_SECONDS));
    }
    return SegmentCompletionProtocol.getDefaultMaxSegmentCommitTimeSeconds();
  }

  public boolean isUpdateSegmentStateModel() {
    if (containsKey(UPDATE_SEGMENT_STATE_MODEL)) {
      return Boolean.parseBoolean(getProperty(UPDATE_SEGMENT_STATE_MODEL).toString());
    }
    return false;   // Default is to leave the statemodel untouched.
  }

  public String generateVipUrl() {
    return getControllerVipProtocol() + "://" + getControllerVipHost() + ":" + getControllerVipPort();
  }

  public String getZkStr() {
    Object zkAddressObj = getProperty(ZK_STR);

    // The set method converted comma separated string into ArrayList, so need to convert back to String here.
    if (zkAddressObj instanceof ArrayList) {
      List<String> zkAddressList = (ArrayList<String>) zkAddressObj;
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

  public Configuration getPinotFSFactoryClasses() {
    return this.subset(PINOT_FS_FACTORY_CLASS_PREFIX);
  }

  public boolean getAcceptSplitCommit() {
    return getBoolean(ENABLE_SPLIT_COMMIT, DEFAULT_ENABLE_SPLIT_COMMIT);
  }

  public String getControllerVipHost() {
    if (containsKey(CONTROLLER_VIP_HOST) && ((String) getProperty(CONTROLLER_VIP_HOST)).length() > 0) {
      return (String) getProperty(CONTROLLER_VIP_HOST);
    }
    return (String) getProperty(CONTROLLER_HOST);
  }

  public String getControllerVipPort() {
    if (containsKey(CONTROLLER_VIP_PORT) && ((String) getProperty(CONTROLLER_VIP_PORT)).length() > 0) {
      return (String) getProperty(CONTROLLER_VIP_PORT);
    }
    return getControllerPort();
  }

  public String getControllerVipProtocol() {
    if (containsKey(CONTROLLER_VIP_PROTOCOL) && getProperty(CONTROLLER_VIP_PROTOCOL).equals("https")) {
      return "https";
    }
    return "http";
  }

  public int getRetentionControllerFrequencyInSeconds() {
    if (containsKey(ControllerPeriodicTasksConf.RETENTION_MANAGER_FREQUENCY_IN_SECONDS)) {
      return Integer.parseInt((String) getProperty(ControllerPeriodicTasksConf.RETENTION_MANAGER_FREQUENCY_IN_SECONDS));
    }
    return ControllerPeriodicTasksConf.DEFAULT_RETENTION_CONTROLLER_FREQUENCY_IN_SECONDS;
  }

  public void setRetentionControllerFrequencyInSeconds(int retentionFrequencyInSeconds) {
    setProperty(ControllerPeriodicTasksConf.RETENTION_MANAGER_FREQUENCY_IN_SECONDS,
        Integer.toString(retentionFrequencyInSeconds));
  }

  /**
   * Returns the config value for controller.offline.segment.interval.checker.frequencyInSeconds if it exists.
   * If it doesn't exist, returns the segment level validation interval. This is done in order to retain the current behavior,
   * wherein the offline validation tasks were done at segment level validation interval frequency
   * The default value is the new DEFAULT_OFFLINE_SEGMENT_INTERVAL_CHECKER_FREQUENCY_IN_SECONDS
   * @return
   */
  public int getOfflineSegmentIntervalCheckerFrequencyInSeconds() {
    if (containsKey(ControllerPeriodicTasksConf.OFFLINE_SEGMENT_INTERVAL_CHECKER_FREQUENCY_IN_SECONDS)) {
      return Integer.parseInt(
          (String) getProperty(ControllerPeriodicTasksConf.OFFLINE_SEGMENT_INTERVAL_CHECKER_FREQUENCY_IN_SECONDS));
    }
    return getInt(ControllerPeriodicTasksConf.SEGMENT_LEVEL_VALIDATION_INTERVAL_IN_SECONDS,
        ControllerPeriodicTasksConf.DEFAULT_OFFLINE_SEGMENT_INTERVAL_CHECKER_FREQUENCY_IN_SECONDS);
  }

  public void setOfflineSegmentIntervalCheckerFrequencyInSeconds(int validationFrequencyInSeconds) {
    setProperty(ControllerPeriodicTasksConf.OFFLINE_SEGMENT_INTERVAL_CHECKER_FREQUENCY_IN_SECONDS,
        Integer.toString(validationFrequencyInSeconds));
  }

  /**
   * Returns the config value for controller.realtime.segment.validation.frequencyInSeconds if it exists.
   * If it doesn't exist, returns the validation controller frequency. This is done in order to retain the current behavior,
   * wherein the realtime validation tasks were done at validation controller frequency
   * The default value is the new DEFAULT_REALTIME_SEGMENT_VALIDATION_FREQUENCY_IN_SECONDS
   * @return
   */
  public int getRealtimeSegmentValidationFrequencyInSeconds() {
    if (containsKey(ControllerPeriodicTasksConf.REALTIME_SEGMENT_VALIDATION_FREQUENCY_IN_SECONDS)) {
      return Integer
          .parseInt((String) getProperty(ControllerPeriodicTasksConf.REALTIME_SEGMENT_VALIDATION_FREQUENCY_IN_SECONDS));
    }
    return getInt(ControllerPeriodicTasksConf.DEPRECATED_VALIDATION_MANAGER_FREQUENCY_IN_SECONDS,
        ControllerPeriodicTasksConf.DEFAULT_REALTIME_SEGMENT_VALIDATION_FREQUENCY_IN_SECONDS);
  }

  public void setRealtimeSegmentValidationFrequencyInSeconds(int validationFrequencyInSeconds) {
    setProperty(ControllerPeriodicTasksConf.REALTIME_SEGMENT_VALIDATION_FREQUENCY_IN_SECONDS,
        Integer.toString(validationFrequencyInSeconds));
  }

  /**
   * Returns the config value for  controller.broker.resource.validation.frequencyInSeconds if it exists.
   * If it doesn't exist, returns the validation controller frequency. This is done in order to retain the current behavior,
   * wherein the broker resource validation tasks were done at validation controller frequency
   * The default value is the new DEFAULT_BROKER_RESOURCE_VALIDATION_FREQUENCY_IN_SECONDS
   * @return
   */
  public int getBrokerResourceValidationFrequencyInSeconds() {
    if (containsKey(ControllerPeriodicTasksConf.BROKER_RESOURCE_VALIDATION_FREQUENCY_IN_SECONDS)) {
      return Integer
          .parseInt((String) getProperty(ControllerPeriodicTasksConf.BROKER_RESOURCE_VALIDATION_FREQUENCY_IN_SECONDS));
    }
    return getInt(ControllerPeriodicTasksConf.DEPRECATED_VALIDATION_MANAGER_FREQUENCY_IN_SECONDS,
        ControllerPeriodicTasksConf.DEFAULT_BROKER_RESOURCE_VALIDATION_FREQUENCY_IN_SECONDS);
  }

  public void setBrokerResourceValidationFrequencyInSeconds(int validationFrequencyInSeconds) {
    setProperty(ControllerPeriodicTasksConf.BROKER_RESOURCE_VALIDATION_FREQUENCY_IN_SECONDS,
        Integer.toString(validationFrequencyInSeconds));
  }

  public long getBrokerResourceValidationInitialDelayInSeconds() {
    return getLong(ControllerPeriodicTasksConf.BROKER_RESOURCE_VALIDATION_INITIAL_DELAY_IN_SECONDS,
        getPeriodicTaskInitialDelayInSeconds());
  }

  public int getStatusCheckerFrequencyInSeconds() {
    if (containsKey(ControllerPeriodicTasksConf.STATUS_CHECKER_FREQUENCY_IN_SECONDS)) {
      return Integer.parseInt((String) getProperty(ControllerPeriodicTasksConf.STATUS_CHECKER_FREQUENCY_IN_SECONDS));
    }
    return ControllerPeriodicTasksConf.DEFAULT_STATUS_CONTROLLER_FREQUENCY_IN_SECONDS;
  }

  public void setStatusCheckerFrequencyInSeconds(int statusCheckerFrequencyInSeconds) {
    setProperty(ControllerPeriodicTasksConf.STATUS_CHECKER_FREQUENCY_IN_SECONDS,
        Integer.toString(statusCheckerFrequencyInSeconds));
  }

  public String getRealtimeSegmentRelocatorFrequency() {
    if (containsKey(ControllerPeriodicTasksConf.REALTIME_SEGMENT_RELOCATOR_FREQUENCY)) {
      return (String) getProperty(ControllerPeriodicTasksConf.REALTIME_SEGMENT_RELOCATOR_FREQUENCY);
    }
    return ControllerPeriodicTasksConf.DEFAULT_REALTIME_SEGMENT_RELOCATOR_FREQUENCY;
  }

  public void setRealtimeSegmentRelocatorFrequency(String relocatorFrequency) {
    setProperty(ControllerPeriodicTasksConf.REALTIME_SEGMENT_RELOCATOR_FREQUENCY, relocatorFrequency);
  }

  public int getStatusCheckerWaitForPushTimeInSeconds() {
    if (containsKey(ControllerPeriodicTasksConf.STATUS_CHECKER_WAIT_FOR_PUSH_TIME_IN_SECONDS)) {
      return Integer
          .parseInt((String) getProperty(ControllerPeriodicTasksConf.STATUS_CHECKER_WAIT_FOR_PUSH_TIME_IN_SECONDS));
    }
    return ControllerPeriodicTasksConf.DEFAULT_STATUS_CONTROLLER_WAIT_FOR_PUSH_TIME_IN_SECONDS;
  }

  public void setStatusCheckerWaitForPushTimeInSeconds(int statusCheckerWaitForPushTimeInSeconds) {
    setProperty(ControllerPeriodicTasksConf.STATUS_CHECKER_WAIT_FOR_PUSH_TIME_IN_SECONDS,
        Integer.toString(statusCheckerWaitForPushTimeInSeconds));
  }

  public long getExternalViewOnlineToOfflineTimeout() {
    if (containsKey(EXTERNAL_VIEW_ONLINE_TO_OFFLINE_TIMEOUT)) {
      return Integer.parseInt((String) getProperty(EXTERNAL_VIEW_ONLINE_TO_OFFLINE_TIMEOUT));
    }
    return DEFAULT_EXTERNAL_VIEW_ONLINE_TO_OFFLINE_TIMEOUT_MILLIS;
  }

  public void setExternalViewOnlineToOfflineTimeout(long timeout) {
    setProperty(EXTERNAL_VIEW_ONLINE_TO_OFFLINE_TIMEOUT, timeout);
  }

  public boolean tenantIsolationEnabled() {
    if (containsKey(CLUSTER_TENANT_ISOLATION_ENABLE)) {
      return Boolean.parseBoolean(getProperty(CLUSTER_TENANT_ISOLATION_ENABLE).toString());
    }
    return true;
  }

  public void setTenantIsolationEnabled(boolean isSingleTenant) {
    setProperty(CLUSTER_TENANT_ISOLATION_ENABLE, isSingleTenant);
  }

  public void setServerAdminRequestTimeoutSeconds(int timeoutSeconds) {
    setProperty(SERVER_ADMIN_REQUEST_TIMEOUT_SECONDS, timeoutSeconds);
  }

  public int getServerAdminRequestTimeoutSeconds() {
    return getInt(SERVER_ADMIN_REQUEST_TIMEOUT_SECONDS, DEFAULT_SERVER_ADMIN_REQUEST_TIMEOUT_SECONDS);
  }

  public int getDeletedSegmentsRetentionInDays() {
    return getInt(DELETED_SEGMENTS_RETENTION_IN_DAYS, DEFAULT_DELETED_SEGMENTS_RETENTION_IN_DAYS);
  }

  public void setDeletedSegmentsRetentionInDays(int retentionInDays) {
    setProperty(DELETED_SEGMENTS_RETENTION_IN_DAYS, retentionInDays);
  }

  public int getTaskManagerFrequencyInSeconds() {
    return getInt(ControllerPeriodicTasksConf.TASK_MANAGER_FREQUENCY_IN_SECONDS,
        ControllerPeriodicTasksConf.DEFAULT_TASK_MANAGER_FREQUENCY_IN_SECONDS);
  }

  public void setTaskManagerFrequencyInSeconds(int frequencyInSeconds) {
    setProperty(ControllerPeriodicTasksConf.TASK_MANAGER_FREQUENCY_IN_SECONDS, Integer.toString(frequencyInSeconds));
  }

  public int getDefaultTableMinReplicas() {
    return getInt(TABLE_MIN_REPLICAS, DEFAULT_TABLE_MIN_REPLICAS);
  }

  public void setTableMinReplicas(int minReplicas) {
    setProperty(TABLE_MIN_REPLICAS, minReplicas);
  }

  public String getJerseyAdminApiPort() {
    return getString(JERSEY_ADMIN_API_PORT, String.valueOf(DEFAULT_JERSEY_ADMIN_PORT));
  }

  public String getAccessControlFactoryClass() {
    return getString(ACCESS_CONTROL_FACTORY_CLASS, DEFAULT_ACCESS_CONTROL_FACTORY_CLASS);
  }

  public void setAccessControlFactoryClass(String accessControlFactoryClass) {
    setProperty(ACCESS_CONTROL_FACTORY_CLASS, accessControlFactoryClass);
  }

  public long getSegmentUploadTimeoutInMillis() {
    return getLong(SEGMENT_UPLOAD_TIMEOUT_IN_MILLIS, DEFAULT_SEGMENT_UPLOAD_TIMEOUT_IN_MILLIS);
  }

  public void setSegmentUploadTimeoutInMillis(long segmentUploadTimeoutInMillis) {
    setProperty(SEGMENT_UPLOAD_TIMEOUT_IN_MILLIS, segmentUploadTimeoutInMillis);
  }

  public int getRealtimeSegmentMetadataCommitNumLocks() {
    return getInt(REALTIME_SEGMENT_METADATA_COMMIT_NUMLOCKS, DEFAULT_REALTIME_SEGMENT_METADATA_COMMIT_NUMLOCKS);
  }

  public void setRealtimeSegmentMetadataCommitNumLocks(int realtimeSegmentMetadataCommitNumLocks) {
    setProperty(REALTIME_SEGMENT_METADATA_COMMIT_NUMLOCKS, realtimeSegmentMetadataCommitNumLocks);
  }

  public boolean getEnableStorageQuotaCheck() {
    return getBoolean(ENABLE_STORAGE_QUOTA_CHECK, DEFAULT_ENABLE_STORAGE_QUOTA_CHECK);
  }

  public boolean getEnableBatchMessageMode() {
    return getBoolean(ENABLE_BATCH_MESSAGE_MODE, DEFAULT_ENABLE_BATCH_MESSAGE_MODE);
  }

  public int getSegmentLevelValidationIntervalInSeconds() {
    return getInt(ControllerPeriodicTasksConf.SEGMENT_LEVEL_VALIDATION_INTERVAL_IN_SECONDS,
        ControllerPeriodicTasksConf.DEFAULT_SEGMENT_LEVEL_VALIDATION_INTERVAL_IN_SECONDS);
  }

  public long getStatusCheckerInitialDelayInSeconds() {
    return getLong(ControllerPeriodicTasksConf.STATUS_CHECKER_INITIAL_DELAY_IN_SECONDS,
        ControllerPeriodicTasksConf.getRandomInitialDelayInSeconds());
  }

  public long getRetentionManagerInitialDelayInSeconds() {
    return getLong(ControllerPeriodicTasksConf.RETENTION_MANAGER_INITIAL_DELAY_IN_SECONDS,
        ControllerPeriodicTasksConf.getRandomInitialDelayInSeconds());
  }

  public long getRealtimeSegmentRelocationInitialDelayInSeconds() {
    return getLong(ControllerPeriodicTasksConf.REALTIME_SEGMENT_RELOCATION_INITIAL_DELAY_IN_SECONDS,
        ControllerPeriodicTasksConf.getRandomInitialDelayInSeconds());
  }

  public long getOfflineSegmentIntervalCheckerInitialDelayInSeconds() {
    return getLong(ControllerPeriodicTasksConf.OFFLINE_SEGMENT_INTERVAL_CHECKER_INITIAL_DELAY_IN_SECONDS,
        ControllerPeriodicTasksConf.getRandomInitialDelayInSeconds());
  }

  public long getRealtimeSegmentValidationManagerInitialDelaySeconds() {
    return getPeriodicTaskInitialDelayInSeconds();
  }

  public long getPinotTaskManagerInitialDelaySeconds() {
    return getPeriodicTaskInitialDelayInSeconds();
  }

  public long getPeriodicTaskInitialDelayInSeconds() {
    return ControllerPeriodicTasksConf.getRandomInitialDelayInSeconds();
  }

  public void setControllerMode(ControllerMode controllerMode) {
    setProperty(CONTROLLER_MODE, controllerMode.name());
  }

  public ControllerMode getControllerMode() {
    return ControllerMode.valueOf(getString(CONTROLLER_MODE, DEFAULT_CONTROLLER_MODE).toUpperCase());
  }

  public boolean getHLCTablesAllowed() {
    return getBoolean(ALLOW_HLC_TABLES, DEFAULT_ALLOW_HLC_TABLES);
  }

  public void setHLCTablesAllowed(boolean allowHLCTables) {
    setProperty(ALLOW_HLC_TABLES, allowHLCTables);
  }
}
