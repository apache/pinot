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
package org.apache.pinot.minion;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.NetUtils;


public class MinionConf extends PinotConfiguration {
  public static final String END_REPLACE_SEGMENTS_TIMEOUT_MS_KEY = "pinot.minion.endReplaceSegments.timeoutMs";
  public static final String MINION_TASK_PROGRESS_MANAGER_CLASS = "pinot.minion.taskProgressManager.class";
  public static final int DEFAULT_END_REPLACE_SEGMENTS_SOCKET_TIMEOUT_MS = 10 * 60 * 1000; // 10 mins

  public MinionConf() {
    super(new HashMap<>());
  }

  public MinionConf(Map<String, Object> baseProperties) {
    super(baseProperties);
  }

  public String getHelixClusterName() {
    return getProperty(CommonConstants.Helix.CONFIG_OF_CLUSTER_NAME);
  }

  public String getZkAddress() {
    return getProperty(CommonConstants.Helix.CONFIG_OF_ZOOKEEPR_SERVER);
  }

  public String getHostName()
      throws Exception {
    return getProperty(CommonConstants.Helix.KEY_OF_MINION_HOST,
        getProperty(CommonConstants.Helix.SET_INSTANCE_ID_TO_HOSTNAME_KEY, false) ? NetUtils
            .getHostnameOrAddress() : NetUtils.getHostAddress());
  }

  public int getPort() {
    return getProperty(CommonConstants.Helix.KEY_OF_MINION_PORT, CommonConstants.Minion.DEFAULT_HELIX_PORT);
  }

  public String getInstanceId() {
    String instanceId = getProperty(CommonConstants.Minion.CONFIG_OF_MINION_ID);
    return instanceId != null ? instanceId : getProperty(CommonConstants.Helix.Instance.INSTANCE_ID_KEY);
  }

  public int getEndReplaceSegmentsTimeoutMs() {
    return getProperty(END_REPLACE_SEGMENTS_TIMEOUT_MS_KEY, DEFAULT_END_REPLACE_SEGMENTS_SOCKET_TIMEOUT_MS);
  }

  public boolean isAllowDownloadFromServer() {
    return Boolean.parseBoolean(getProperty(CommonConstants.Minion.CONFIG_OF_ALLOW_DOWNLOAD_FROM_SERVER,
        CommonConstants.Minion.DEFAULT_ALLOW_DOWNLOAD_FROM_SERVER));
  }

  public PinotConfiguration getMetricsConfig() {
    return subset(CommonConstants.Minion.METRICS_CONFIG_PREFIX);
  }

  public String getMetricsPrefix() {
    return Optional.ofNullable(getProperty(CommonConstants.Minion.CONFIG_OF_METRICS_PREFIX_KEY))
        .orElseGet(() -> getProperty(CommonConstants.Minion.DEPRECATED_CONFIG_OF_METRICS_PREFIX_KEY,
            CommonConstants.Minion.CONFIG_OF_METRICS_PREFIX));
  }
}
