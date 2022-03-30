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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.MapUtils;
import org.apache.pinot.spi.config.table.assignment.InstanceConstraintConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A factory class to construct instance constraint appliers given the table name with type,
 * instance constraint configs, and the existing poolToInstancesMap.
 */
public class InstanceConstraintAppliersFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(InstanceConstraintAppliersFactory.class);

  private InstanceConstraintAppliersFactory() {
  }

  public static List<InstanceConstraintApplier> constructConstraintAppliers(String tableNameWithType,
      InstanceConstraintConfig constraintConfig, Map<Integer, List<String>> existingPoolToInstancesMap) {
    List<InstanceConstraintApplier> constraintAppliers = new ArrayList<>();
    // If there is some k-v pair in the existingPoolToInstancesMap, retain the instance sequence for existing pools.
    // For new pools, rotation will be applied just as the default constraint applier.
    if (MapUtils.isNotEmpty(existingPoolToInstancesMap)) {
      constraintAppliers
          .add(new RetainedSequenceInstanceConstraintApplier(tableNameWithType, existingPoolToInstancesMap));
    }
    if (constraintConfig == null && constraintAppliers.isEmpty()) {
      LOGGER.info("No instance constraint is configured, using default hash-based-rotate instance constraint");
      constraintAppliers.add(new HashBasedRotateInstanceConstraintApplier(tableNameWithType));
    }
    // TODO: support more constraints
    return constraintAppliers;
  }
}
