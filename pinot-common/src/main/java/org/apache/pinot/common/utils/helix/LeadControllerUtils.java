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

import java.util.Map;
import java.util.Set;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.common.utils.StringUtil;


public class LeadControllerUtils {

  /**
   * Gets hash code for table using murmur2 function, ignores the most significant bit.
   * Note: This method CANNOT be changed when lead controller resource is enabled.
   * Otherwise it will assign different controller for the same table, which will mess up the controller periodic tasks and realtime segment completion.
   * @param rawTableName table name
   * @return hash code ignoring the most significant bit.
   */
  private static int getHashCodeForTable(String rawTableName) {
    return (HashUtil.murmur2(StringUtil.encodeUtf8(rawTableName)) & 0x7fffffff);
  }

  /**
   * Given a raw table name and number of partitions, returns the partition id in lead controller resource.
   * @param rawTableName raw table name
   * @param numPartitions number of partitions
   * @return partition id in lead controller resource.
   */
  public static int getPartitionIdForTable(String rawTableName, int numPartitions) {
    return getHashCodeForTable(rawTableName) % numPartitions;
  }

  /**
   * Gets lead controller participant id for table from lead controller resource.
   * If the resource is disabled or no controller registered as participant, there is no instance in "MASTER" state.
   * @param leadControllerResourceExternalView external view of lead controller resource
   * @param rawTableName table name without type
   * @return controller participant id for partition leader, e.g. Controller_localhost_9000. Null if not found or resource is disabled.
   */
  public static String getLeadControllerInstanceForTable(ExternalView leadControllerResourceExternalView, String rawTableName) {
    if (leadControllerResourceExternalView == null) {
      return null;
    }
    Set<String> partitionSet = leadControllerResourceExternalView.getPartitionSet();
    if (partitionSet == null || partitionSet.isEmpty()) {
      return null;
    }
    int numPartitions = partitionSet.size();
    int partitionIndex = getPartitionIdForTable(rawTableName, numPartitions);
    String partitionName = generatePartitionName(partitionIndex);
    Map<String, String> partitionStateMap = leadControllerResourceExternalView.getStateMap(partitionName);

    // Get master host from partition map. Return null if no master found.
    for (Map.Entry<String, String> entry : partitionStateMap.entrySet()) {
      if (MasterSlaveSMD.States.MASTER.name().equals(entry.getValue())) {
        return entry.getKey();
      }
    }
    return null;
  }

  /**
   * Generates controller participant id, e.g. returns Controller_localhost_9000 given localhost as hostname and 9000 as port.
   */
  public static String generateControllerParticipantId(String controllerHost, String controllerPort) {
    return org.apache.pinot.common.utils.CommonConstants.Helix.PREFIX_OF_CONTROLLER_INSTANCE + controllerHost + "_"
        + controllerPort;
  }

  /**
   * Extracts lead controller hostname and port from controller participant id, e.g. returns localhost_9000 given Controller_localhost_9000 as controller participant id.
   */
  public static String extractLeadControllerHostNameAndPort(String controllerParticipantId) {
    return controllerParticipantId.substring(controllerParticipantId.indexOf("_") + 1);
  }

  /**
   * Generates partition name, e.g. returns leadControllerResource_0 given 0 as partition index.
   */
  public static String generatePartitionName(int partitionIndex) {
    return org.apache.pinot.common.utils.CommonConstants.Helix.LEAD_CONTROLLER_RESOURCE_NAME + "_" + partitionIndex;
  }

  /**
   * Extracts partition index from partition name, e.g. returns 0 given leadControllerResource_0 as partition name.
   */
  public static int extractPartitionIndex(String partitionName) {
    return Integer.parseInt(partitionName.substring(partitionName.lastIndexOf("_") + 1));
  }
}
