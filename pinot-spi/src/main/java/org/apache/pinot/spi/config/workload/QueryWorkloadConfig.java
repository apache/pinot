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
import java.util.Map;
import org.apache.pinot.spi.config.BaseJsonConfig;


public class QueryWorkloadConfig extends BaseJsonConfig {

  public static final String QUERY_WORKLOAD_NAME = "queryWorkloadName";
  public static final String NODE_CONFIGS = "nodeConfigs";

  @JsonPropertyDescription("Describes the name for the query workload, this should be unique across the zk cluster")
  private String _queryWorkloadName;

  @JsonPropertyDescription("Describes the node configs for the query workload")
  private Map<NodeConfig.Type, NodeConfig> _nodeConfigs;

  @JsonCreator
  public QueryWorkloadConfig(@JsonProperty(QUERY_WORKLOAD_NAME) String queryWorkloadName,
      @JsonProperty(NODE_CONFIGS) Map<NodeConfig.Type, NodeConfig> nodeConfigs) {
    _queryWorkloadName = queryWorkloadName;
    _nodeConfigs = nodeConfigs;
  }

  public String getQueryWorkloadName() {
    return _queryWorkloadName;
  }

  public Map<NodeConfig.Type, NodeConfig> getNodeConfigs() {
    return _nodeConfigs;
  }
}
