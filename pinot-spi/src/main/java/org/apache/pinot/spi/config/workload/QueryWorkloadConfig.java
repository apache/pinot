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
package org.apache.pinot.spi.config.workload;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import java.util.List;
import org.apache.pinot.spi.config.BaseJsonConfig;

/**
 * Represents the configuration for a named query workload in a Pinot Helix cluster.
 * <p>
 * A QueryWorkload groups a set of nodes and associated configuration parameters under a single workload name.
 * Workloads are applied at the cluster level: individual queries specify the workload they belong to by
 * providing the workload name in their query options. This allows us to manage and enforce isolation by limiting
 * the resources available to queries based on their workload classification.
 * </p>
 * <p><strong>Example:</strong></p>
 * <pre>{@code
 * {
 *   "queryWorkloadName": "analytics",
 *   "nodeConfigs": [
 *     {
 *       "nodeType": "brokerNode",
 *       "enforcementProfile": {
 *        "cpuCostNs": 1000000,
 *        "memoryCostBytes": 10000000
 *       },
 *       "propagationScheme": {
 *         "propagationType": "TABLE",
 *         "values": ["airlineStats"]
 *       }
 *      },
 *     {
 *       "nodeType": "serverNode",
 *       "enforcementProfile": {
 *        "cpuCostNs": 2000000,
 *        "memoryCostBytes": 20000000
 *       },
 *       "propagationScheme": {
 *        "propagationType": "TENANT",
 *        "values": ["tenantA", "tenantB"]
 *       }
 *     }
 *   ]
 * }
 * }</pre>
 *
 * @see NodeConfig
 */
public class QueryWorkloadConfig extends BaseJsonConfig {

  public static final String QUERY_WORKLOAD_NAME = "queryWorkloadName";
  public static final String NODE_CONFIGS = "nodeConfigs";

  @JsonPropertyDescription("Describes the name for the query workload, this should be unique across the zk cluster")
  private String _queryWorkloadName;

  @JsonPropertyDescription("Describes the node configs for the query workload")
  private List<NodeConfig> _nodeConfigs;

  @JsonCreator
  /**
   * Constructs a new QueryWorkloadConfig instance.
   *
   * @param queryWorkloadName unique name identifying this workload across the cluster
   * @param nodeConfigs list of per-node configurations for this workload
   */
  public QueryWorkloadConfig(@JsonProperty(QUERY_WORKLOAD_NAME) String queryWorkloadName,
      @JsonProperty(NODE_CONFIGS) List<NodeConfig> nodeConfigs) {
    _queryWorkloadName = queryWorkloadName;
    _nodeConfigs = nodeConfigs;
  }

  /**
   * Returns the unique name of this query workload.
   *
   * @return the workload name used by queries to specify this workload
   */
  public String getQueryWorkloadName() {
    return _queryWorkloadName;
  }

  /**
   * Returns the list of node-specific configurations for this workload.
   *
   * @return list of NodeConfig objects for this workload
   */
  public List<NodeConfig> getNodeConfigs() {
    return _nodeConfigs;
  }
}
