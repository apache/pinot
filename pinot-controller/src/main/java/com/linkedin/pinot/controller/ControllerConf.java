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
package com.linkedin.pinot.controller;

import com.linkedin.pinot.common.protocols.SegmentCompletionProtocol;
import com.linkedin.pinot.common.utils.StringUtil;
import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class ControllerConf extends PropertiesConfiguration {
  private static final String CONTROLLER_VIP_HOST = "controller.vip.host";
  private static final String CONTROLLER_VIP_PORT = "controller.vip.port";
  private static final String CONTROLLER_VIP_PROTOCOL = "controller.vip.protocol";
  private static final String CONTROLLER_HOST = "controller.host";
  private static final String CONTROLLER_PORT = "controller.port";
  private static final String DATA_DIR = "controller.data.dir";
  private static final String ZK_STR = "controller.zk.str";
  private static final String UPDATE_SEGMENT_STATE_MODEL = "controller.update_segment_state_model"; // boolean: Update the statemodel on boot?
  private static final String HELIX_CLUSTER_NAME = "controller.helix.cluster.name";
  private static final String CLUSTER_TENANT_ISOLATION_ENABLE = "cluster.tenant.isolation.enable";
  private static final String CONSOLE_WEBAPP_ROOT_PATH = "controller.query.console";
  private static final String EXTERNAL_VIEW_ONLINE_TO_OFFLINE_TIMEOUT = "controller.upload.onlineToOfflineTimeout";
  private static final String RETENTION_MANAGER_FREQUENCY_IN_SECONDS = "controller.retention.frequencyInSeconds";
  private static final String VALIDATION_MANAGER_FREQUENCY_IN_SECONDS = "controller.validation.frequencyInSeconds";
  private static final String STATUS_CHECKER_FREQUENCY_IN_SECONDS = "controller.statuschecker.frequencyInSeconds";
  private static final String STATUS_CHECKER_WAIT_FOR_PUSH_TIME_IN_SECONDS = "controller.statuschecker.waitForPushTimeInSeconds";
  private static final String SERVER_ADMIN_REQUEST_TIMEOUT_SECONDS = "server.request.timeoutSeconds";
  private static final String SEGMENT_COMMIT_TIMEOUT_SECONDS = "controller.realtime.segment.commit.timeoutSeconds";
  private static final String DELETED_SEGMENTS_RETENTION_IN_DAYS = "controller.deleted.segments.retentionInDays";
  private static final String TASK_MANAGER_FREQUENCY_IN_SECONDS = "controller.task.frequencyInSeconds";
  private static final String TABLE_MIN_REPLICAS = "table.minReplicas";
  private static final String ENABLE_SPLIT_COMMIT = "controller.enable.split.commit";
  private static final String JERSEY_ADMIN_API_PORT = "jersey.admin.api.port";
  private static final String JERSEY_ADMIN_IS_PRIMARY = "jersey.admin.isprimary";
  private static final String ACCESS_CONTROL_FACTORY_CLASS = "controller.admin.access.control.factory.class";
  // Amount of the time the segment can take from the beginning of upload to the end of upload. Used when parallel push
  // protection is enabled. If the upload does not finish within the timeout, next upload can override the previous one.
  private static final String SEGMENT_UPLOAD_TIMEOUT_IN_MILLIS = "controller.segment.upload.timeoutInMillis";

  private static final int DEFAULT_RETENTION_CONTROLLER_FREQUENCY_IN_SECONDS = 6 * 60 * 60; // 6 Hours.
  private static final int DEFAULT_VALIDATION_CONTROLLER_FREQUENCY_IN_SECONDS = 60 * 60; // 1 Hour.
  private static final int DEFAULT_STATUS_CONTROLLER_FREQUENCY_IN_SECONDS = 5 * 60; // 5 minutes
  private static final int DEFAULT_STATUS_CONTROLLER_WAIT_FOR_PUSH_TIME_IN_SECONDS = 10 * 60; // 10 minutes
  private static final long DEFAULT_EXTERNAL_VIEW_ONLINE_TO_OFFLINE_TIMEOUT_MILLIS = 120_000L; // 2 minutes
  private static final int DEFAULT_SERVER_ADMIN_REQUEST_TIMEOUT_SECONDS = 30;
  private static final int DEFAULT_DELETED_SEGMENTS_RETENTION_IN_DAYS = 7;
  private static final int DEFAULT_TASK_MANAGER_FREQUENCY_IN_SECONDS = -1; // Disabled
  private static final int DEFAULT_TABLE_MIN_REPLICAS = 1;
  private static final boolean DEFAULT_ENABLE_SPLIT_COMMIT = false;
  private static final int DEFAULT_JERSEY_ADMIN_PORT = 21000;
  private static final String DEFAULT_ACCESS_CONTROL_FACTORY_CLASS =
      "com.linkedin.pinot.controller.api.access.AllowAllAccessFactory";
  private static final long DEFAULT_SEGMENT_UPLOAD_TIMEOUT_IN_MILLIS = 600_000L; // 10 minutes

  public ControllerConf(File file) throws ConfigurationException {
    super(file);
  }

  public ControllerConf() {
    super();
  }

  public static String constructDownloadUrl(String tableName, String segmentName, String vip) {
    try {
      return StringUtil.join("/", vip, "segments", tableName, URLEncoder.encode(segmentName, "UTF-8"));
    } catch (UnsupportedEncodingException e) {
      // Shouldn't happen
      throw new AssertionError("UTF-8 encoding should always be supported", e);
    }
  }

  public void setSplitCommit(boolean isSplitCommit) {
    setProperty(ENABLE_SPLIT_COMMIT, isSplitCommit);
  }

  public void setQueryConsolePath(String path) {
    setProperty(CONSOLE_WEBAPP_ROOT_PATH, path);
  }

  public String getQueryConsole() {
    if (containsKey(CONSOLE_WEBAPP_ROOT_PATH)) {
      return (String) getProperty(CONSOLE_WEBAPP_ROOT_PATH);
    }
    return ControllerConf.class.getClassLoader().getResource("webapp").toExternalForm();
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
      return Integer.parseInt((String)getProperty(SEGMENT_COMMIT_TIMEOUT_SECONDS));
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
      String[] zkAddress =  zkAddressList.toArray(new String[0]);
      return StringUtil.join(",", zkAddress);
    } else if (zkAddressObj instanceof String) {
      return (String) zkAddressObj;
    } else {
      throw new RuntimeException("Unexpected data type for zkAddress PropertiesConfiguration, expecting String but got "
              + zkAddressObj.getClass().getName());
    }
  }

  @Override
  public String toString() {
    return super.toString();
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
    if (containsKey(CONTROLLER_VIP_PROTOCOL) && ((String) getProperty(CONTROLLER_VIP_PROTOCOL)).equals("https")) {
      return "https";
    }
    return "http";
  }

  public int getRetentionControllerFrequencyInSeconds() {
    if (containsKey(RETENTION_MANAGER_FREQUENCY_IN_SECONDS)) {
      return Integer.parseInt((String) getProperty(RETENTION_MANAGER_FREQUENCY_IN_SECONDS));
    }
    return DEFAULT_RETENTION_CONTROLLER_FREQUENCY_IN_SECONDS;
  }

  public void setRetentionControllerFrequencyInSeconds(int retentionFrequencyInSeconds) {
    setProperty(RETENTION_MANAGER_FREQUENCY_IN_SECONDS, Integer.toString(retentionFrequencyInSeconds));
  }

  public int getValidationControllerFrequencyInSeconds() {
    if (containsKey(VALIDATION_MANAGER_FREQUENCY_IN_SECONDS)) {
      return Integer.parseInt((String) getProperty(VALIDATION_MANAGER_FREQUENCY_IN_SECONDS));
    }
    return DEFAULT_VALIDATION_CONTROLLER_FREQUENCY_IN_SECONDS;
  }

  public void setValidationControllerFrequencyInSeconds(int validationFrequencyInSeconds) {
    setProperty(VALIDATION_MANAGER_FREQUENCY_IN_SECONDS, Integer.toString(validationFrequencyInSeconds));
  }

  public int getStatusCheckerFrequencyInSeconds() {
    if (containsKey(STATUS_CHECKER_FREQUENCY_IN_SECONDS)) {
      return Integer.parseInt((String) getProperty(STATUS_CHECKER_FREQUENCY_IN_SECONDS));
    }
    return DEFAULT_STATUS_CONTROLLER_FREQUENCY_IN_SECONDS;
  }

  public void setStatusCheckerFrequencyInSeconds(int statusCheckerFrequencyInSeconds) {
    setProperty(STATUS_CHECKER_FREQUENCY_IN_SECONDS, Integer.toString(statusCheckerFrequencyInSeconds));
  }

  public int getStatusCheckerWaitForPushTimeInSeconds() {
    if (containsKey(STATUS_CHECKER_WAIT_FOR_PUSH_TIME_IN_SECONDS)) {
      return Integer.parseInt((String) getProperty(STATUS_CHECKER_WAIT_FOR_PUSH_TIME_IN_SECONDS));
    }
    return DEFAULT_STATUS_CONTROLLER_WAIT_FOR_PUSH_TIME_IN_SECONDS;
  }

  public void setStatusCheckerWaitForPushTimeInSeconds(int statusCheckerWaitForPushTimeInSeconds) {
    setProperty(STATUS_CHECKER_WAIT_FOR_PUSH_TIME_IN_SECONDS, Integer.toString(statusCheckerWaitForPushTimeInSeconds));
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
    return getInt(TASK_MANAGER_FREQUENCY_IN_SECONDS, DEFAULT_TASK_MANAGER_FREQUENCY_IN_SECONDS);
  }

  public void setTaskManagerFrequencyInSeconds(int frequencyInSeconds) {
    setProperty(TASK_MANAGER_FREQUENCY_IN_SECONDS, Integer.toString(frequencyInSeconds));
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
}
