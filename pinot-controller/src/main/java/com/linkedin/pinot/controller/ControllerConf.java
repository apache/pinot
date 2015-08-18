/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.io.File;
import java.util.ArrayList;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;


/**
 * Sep 26, 2014
 */

public class ControllerConf extends PropertiesConfiguration {
  private static final String CONTROLLER_VIP_HOST = "controller.vip.host";
  private static final String CONTROLLER_HOST = "controller.host";
  private static final String CONTROLLER_PORT = "controller.port";
  private static final String DATA_DIR = "controller.data.dir";
  private static final String ZK_STR = "controller.zk.str";
  private static final String HELIX_CLUSTER_NAME = "controller.helix.cluster.name";
  private static final String CLUSTER_TENANT_ISOLATION_ENABLE = "cluster.tenant.isolation.enable";
  private static final String CONSOLE_WEBAPP_ROOT_PATH = "controller.query.console";
  private static final String EXTERNAL_VIEW_ONLINE_TO_OFFLINE_TIMEOUT = "controller.upload.onlineToOfflineTimeout";
  private static final String RETENTION_MANAGER_FREQUENCY_IN_SECONDS = "controller.retention.frequencyInSeconds";
  private static final String VALIDATION_MANAGER_FREQUENCY_IN_SECONDS = "controller.validation.frequencyInSeconds";
  private static final int DEFAULT_RETENTION_CONTROLLER_FREQUENCY_IN_SECONDS = 6 * 60 * 60; // 6 Hours.
  private static final int DEFAULT_VALIDATION_CONTROLLER_FREQUENCY_IN_SECONDS = 60 * 60; // 1 Hour.
  private static final long DEFAULT_EXTERNAL_VIEW_ONLINE_TO_OFFLINE_TIMEOUT = 10000L;

  public ControllerConf(File file) throws ConfigurationException {
    super(file);
  }

  public ControllerConf() {
    super();
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

  public void setHelixClusterName(String clusterName) {
    setProperty(HELIX_CLUSTER_NAME, clusterName);
  }

  public void setControllerHost(String host) {
    setProperty(CONTROLLER_HOST, host);
  }

  public void setControllerVipHost(String vipHost) {
    setProperty(CONTROLLER_VIP_HOST, vipHost);
  }

  public void setControllerPort(String port) {
    setProperty(CONTROLLER_PORT, port);
  }

  public void setDataDir(String dataDir) {
    setProperty(DATA_DIR, dataDir);
  }

  public void setZkStr(String zkStr) {
    setProperty(ZK_STR, zkStr);
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

  public String getZkStr() {
    Object zkAddressObj = getProperty(ZK_STR);

    // The set method converted comma separated string into ArrayList, so need to convert back to String here.
    if (zkAddressObj instanceof ArrayList) {
      return String.join(",", (ArrayList) zkAddressObj);
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

  public String getControllerVipHost() {
    if (containsKey(CONTROLLER_VIP_HOST) && ((String) getProperty(CONTROLLER_VIP_HOST)).length() > 0) {
      return (String) getProperty(CONTROLLER_VIP_HOST);
    }
    return (String) getProperty(CONTROLLER_HOST);
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

  public long getExternalViewOnlineToOfflineTimeout() {
    if (containsKey(EXTERNAL_VIEW_ONLINE_TO_OFFLINE_TIMEOUT)) {
      return Integer.parseInt((String) getProperty(EXTERNAL_VIEW_ONLINE_TO_OFFLINE_TIMEOUT));
    }
    return DEFAULT_EXTERNAL_VIEW_ONLINE_TO_OFFLINE_TIMEOUT;
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
}
