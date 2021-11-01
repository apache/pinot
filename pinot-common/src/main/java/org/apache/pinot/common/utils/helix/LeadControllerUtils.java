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
package org.apache.pinot.common.utils.helix;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.ResourceConfig;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;


public class LeadControllerUtils {
  private LeadControllerUtils() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(LeadControllerUtils.class);

  /**
   * Given a raw table name and number of partitions, returns the partition id in lead controller resource.
   * Uses murmur2 function to get hashcode for table, ignores the most significant bit.
   * Note: This method CANNOT be changed when lead controller resource is enabled.
   * Otherwise it will assign different controller for the same table, which will mess up the controller periodic
   * tasks and realtime segment completion.
   * @param rawTableName raw table name
   * @return partition id in lead controller resource.
   */
  public static int getPartitionIdForTable(String rawTableName) {
    return (HashUtil.murmur2(rawTableName.getBytes(UTF_8)) & Integer.MAX_VALUE)
        % Helix.NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE;
  }

  /**
   * Generates participant instance id, e.g. returns Controller_localhost_9000 given localhost as hostname and 9000
   * as port.
   */
  public static String generateParticipantInstanceId(String controllerHost, int controllerPort) {
    return Helix.PREFIX_OF_CONTROLLER_INSTANCE + controllerHost + "_" + controllerPort;
  }

  /**
   * Generates partition name, e.g. returns leadControllerResource_0 given 0 as partition index.
   */
  public static String generatePartitionName(int partitionId) {
    return Helix.LEAD_CONTROLLER_RESOURCE_NAME + "_" + partitionId;
  }

  /**
   * Extracts partition index from partition name, e.g. returns 0 given leadControllerResource_0 as partition name.
   */
  public static int extractPartitionId(String partitionName) {
    return Integer.parseInt(partitionName.substring(partitionName.lastIndexOf('_') + 1));
  }

  /**
   * Checks from ZK if resource config of leadControllerResource is enabled.
   * @param helixManager helix manager
   */
  public static boolean isLeadControllerResourceEnabled(HelixManager helixManager) {
    ConfigAccessor configAccessor = helixManager.getConfigAccessor();
    ResourceConfig resourceConfig =
        configAccessor.getResourceConfig(helixManager.getClusterName(), Helix.LEAD_CONTROLLER_RESOURCE_NAME);
    String resourceEnabled = resourceConfig.getSimpleConfig(Helix.LEAD_CONTROLLER_RESOURCE_ENABLED_KEY);
    return Boolean.parseBoolean(resourceEnabled);
  }

  /**
   * Gets Helix leader in the cluster. Null if there is no leader.
   * @param helixManager helix manager
   * @return instance id of Helix cluster leader, e.g. localhost_9000.
   */
  public static String getHelixClusterLeader(HelixManager helixManager) {
    HelixDataAccessor helixDataAccessor = helixManager.getHelixDataAccessor();
    PropertyKey propertyKey = helixDataAccessor.keyBuilder().controllerLeader();
    LiveInstance liveInstance = helixDataAccessor.getProperty(propertyKey);
    if (liveInstance == null) {
      LOGGER.warn("Helix leader ZNode is missing");
      return null;
    }
    String helixLeaderInstanceId = liveInstance.getInstanceName();
    String helixVersion = liveInstance.getHelixVersion();
    long modifiedTime = liveInstance.getModifiedTime();
    LOGGER.info("Getting Helix leader: {}, Helix version: {}, mtime: {}", helixLeaderInstanceId, helixVersion,
        modifiedTime);
    return helixLeaderInstanceId;
  }
}
