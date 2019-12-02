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
package org.apache.pinot.controller.helix.core.assignment.instance;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.helix.model.InstanceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Rotate the instances based on the table name hash to prevent creating hotspot instances. Use this as the default
 * instance constraint to align with the default instance partitions.
 */
public class HashBasedRotateInstanceConstraintApplier implements InstanceConstraintApplier {
  private static final Logger LOGGER = LoggerFactory.getLogger(HashBasedRotateInstanceConstraintApplier.class);

  private final String _tableNameWithType;

  public HashBasedRotateInstanceConstraintApplier(String tableNameWithType) {
    _tableNameWithType = tableNameWithType;
  }

  @Override
  public Map<Integer, List<InstanceConfig>> applyConstraint(
      Map<Integer, List<InstanceConfig>> poolToInstanceConfigsMap) {
    int tableNameHash = Math.abs(_tableNameWithType.hashCode());
    LOGGER.info("Rotating instances for table: {} with hash: {}", _tableNameWithType, tableNameHash);

    for (Map.Entry<Integer, List<InstanceConfig>> entry : poolToInstanceConfigsMap.entrySet()) {
      List<InstanceConfig> instanceConfigs = entry.getValue();
      int numInstanceConfigs = instanceConfigs.size();
      Preconditions.checkState(numInstanceConfigs > 0, "No instance left in pool: %s", entry.getKey());

      // This should match the default InstancePartitions generated from InstancePartitionsUtils
      Collections.rotate(instanceConfigs, -(tableNameHash % numInstanceConfigs));
    }

    return poolToInstanceConfigsMap;
  }
}
