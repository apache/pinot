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
package org.apache.pinot.controller.workload.selector;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.workload.NodeConfig;


/**
 * The TablePropagationInstanceSelector is responsible for selecting instances hosting the tables
 * defined in the {@link org.apache.pinot.spi.config.workload.PropagationScheme}.
 * For non-leaf nodes, select all the broker instances that serve the given tables.
 * For leaf nodes, select all the server instances that serve the given tables.
 */
public class TableInstanceSelector implements InstanceSelector {

  private final PinotHelixResourceManager _pinotHelixResourceManager;

  public TableInstanceSelector(PinotHelixResourceManager pinotHelixResourceManager) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
  }

  @Override
  public Set<String> resolveInstances(NodeConfig.Type nodeType, NodeConfig nodeConfig) {
    List<String> tables = nodeConfig.getPropagationScheme().getValues();
    Set<String> instances = new HashSet<>();
    switch (nodeType) {
      case NON_LEAF_NODE:
        for (String table : tables) {
          instances.addAll(_pinotHelixResourceManager.getBrokerInstancesFor(table));
        }
        break;
      case LEAF_NODE:
        for (String table : tables) {
          instances.addAll(_pinotHelixResourceManager.getServerInstancesFor(table));
        }
        break;
      default:
        throw new IllegalArgumentException("Unsupported node type: " + nodeType);
    }
    return instances;
  }
}
