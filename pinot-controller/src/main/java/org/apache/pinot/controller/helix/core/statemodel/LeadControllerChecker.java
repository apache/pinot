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
package org.apache.pinot.controller.helix.core.statemodel;

import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.common.utils.CommonConstants.Helix.NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE;


public class LeadControllerChecker {
  private static final Logger LOGGER = LoggerFactory.getLogger(LeadControllerChecker.class);

  private Map<Integer, Integer> _partitionCache;

  public LeadControllerChecker() {
    _partitionCache = new ConcurrentHashMap<>();
  }

  public void addPartitionLeader(String partitionName) {
    LOGGER.info("Add Partition: {} to LeadControllerChecker", partitionName);
    int partitionIndex = Integer.parseInt(partitionName.substring(partitionName.lastIndexOf("_") + 1));
    _partitionCache.put(partitionIndex, partitionIndex);
  }

  public void removePartitionLeader(String partitionName) {
    LOGGER.info("Remove Partition: {} from LeadControllerChecker", partitionName);
    int partitionIndex = Integer.parseInt(partitionName.substring(partitionName.lastIndexOf("_") + 1));
    _partitionCache.remove(partitionIndex);
  }

  public boolean isPartitionLeader(int partitionIndex) {
    Preconditions
        .checkArgument(partitionIndex >= 0 && partitionIndex < NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE,
            "Invalid partition index: " + partitionIndex);
    return _partitionCache.containsKey(partitionIndex);
  }
}
