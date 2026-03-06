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
package org.apache.pinot.systemtable.provider;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.utils.config.InstanceUtils;
import org.apache.pinot.spi.utils.InstanceTypeUtils;
import org.slf4j.Logger;


final class HelixControllerUtils {
  private HelixControllerUtils() {
  }

  static List<String> discoverControllerBaseUrls(HelixAdmin helixAdmin, String clusterName, Logger logger) {
    List<String> urls = new ArrayList<>();
    List<String> instanceIds;
    try {
      instanceIds = helixAdmin.getInstancesInCluster(clusterName);
    } catch (Exception e) {
      logger.warn("Failed to list instances in cluster '{}' while discovering controllers", clusterName, e);
      return List.of();
    }
    for (String instanceId : instanceIds) {
      if (!InstanceTypeUtils.isController(instanceId)) {
        continue;
      }
      String baseUrl = getInstanceBaseUrl(helixAdmin, clusterName, instanceId, logger);
      if (baseUrl != null) {
        urls.add(baseUrl);
      }
    }
    return urls;
  }

  private static @Nullable String getInstanceBaseUrl(HelixAdmin helixAdmin, String clusterName, String instanceId,
      Logger logger) {
    InstanceConfig instanceConfig;
    try {
      instanceConfig = helixAdmin.getInstanceConfig(clusterName, instanceId);
    } catch (Exception e) {
      logger.warn("Failed to fetch Helix InstanceConfig for instance '{}' in cluster '{}'", instanceId, clusterName, e);
      return null;
    }
    if (instanceConfig == null) {
      logger.warn("Missing Helix InstanceConfig for instance '{}' in cluster '{}'", instanceId, clusterName);
      return null;
    }
    String baseUrl = InstanceUtils.getInstanceBaseUri(instanceConfig);
    if (baseUrl.isEmpty()) {
      logger.warn("Failed to build instance base URL from Helix InstanceConfig for instance '{}' in cluster '{}'",
          instanceId, clusterName);
      return null;
    }
    return baseUrl;
  }
}
