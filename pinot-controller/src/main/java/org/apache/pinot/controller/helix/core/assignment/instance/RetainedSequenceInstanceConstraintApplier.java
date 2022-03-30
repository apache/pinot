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
import java.util.List;
import java.util.Map;
import org.apache.helix.model.InstanceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An extended class from {@link HashBasedRotateInstanceConstraintApplier}
 * Retain the instance sequence if the pool exists in the existingPoolToInstancesMap.
 * If the pool doesn't exist in the existing map, the do the rotation.
 */
public class RetainedSequenceInstanceConstraintApplier extends HashBasedRotateInstanceConstraintApplier {
  private static final Logger LOGGER = LoggerFactory.getLogger(RetainedSequenceInstanceConstraintApplier.class);

  private final Map<Integer, List<String>> _existingPoolToInstancesMap;

  public RetainedSequenceInstanceConstraintApplier(String tableNameWithType,
      Map<Integer, List<String>> existingPoolToInstancesMap) {
    super(tableNameWithType);
    _existingPoolToInstancesMap = existingPoolToInstancesMap;
  }

  @Override
  public Map<Integer, List<InstanceConfig>> applyConstraint(
      Map<Integer, List<InstanceConfig>> poolToInstanceConfigsMap) {
    int tableNameHash = Math.abs(_tableNameWithType.hashCode());
    LOGGER.info("Keeping the same instance sequence for instances of table: {}", _tableNameWithType);

    for (Map.Entry<Integer, List<InstanceConfig>> entry : poolToInstanceConfigsMap.entrySet()) {
      List<InstanceConfig> instanceConfigs = entry.getValue();
      int numInstanceConfigs = instanceConfigs.size();
      Preconditions.checkState(numInstanceConfigs > 0, "No instance left in pool: %s", entry.getKey());

      if (!_existingPoolToInstancesMap.containsKey(entry.getKey())) {
        rotateInstances(instanceConfigs, tableNameHash, numInstanceConfigs);
      }
    }
    return poolToInstanceConfigsMap;
  }
}
