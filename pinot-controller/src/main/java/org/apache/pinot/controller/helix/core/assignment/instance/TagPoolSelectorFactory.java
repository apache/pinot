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

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.collections.MapUtils;
import org.apache.pinot.spi.config.table.assignment.InstanceTagPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A factory class to generate the instance tag pool selector based on the table name with type,
 * instance tag pool config, and the existing poolToInstancesMap.
 */
public class TagPoolSelectorFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(TagPoolSelectorFactory.class);

  private TagPoolSelectorFactory() {
  }

  public static InstanceTagPoolSelector getInstanceTagPoolSelector(String tableNameWithType,
      InstanceTagPoolConfig tagPoolConfig, @Nullable Map<Integer, List<String>> existingPoolToInstancesMap) {
    if (MapUtils.isEmpty(existingPoolToInstancesMap)) {
      LOGGER.info("Constructing {} for table: {}", InstanceTagPoolSelector.class.getName(), tableNameWithType);
      return new InstanceTagPoolSelector(tagPoolConfig, tableNameWithType);
    } else {
      LOGGER.info("Constructing {} for table: {}", RetainedSequenceBasedInstanceTagPoolSelector.class.getName(),
          tableNameWithType);
      return new RetainedSequenceBasedInstanceTagPoolSelector(tagPoolConfig, tableNameWithType,
          existingPoolToInstancesMap);
    }
  }
}
