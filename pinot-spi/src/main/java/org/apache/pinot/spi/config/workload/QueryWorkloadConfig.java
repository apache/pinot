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
import org.apache.pinot.spi.config.BaseJsonConfig;


public class QueryWorkloadConfig extends BaseJsonConfig {

  private static final String WORKLOAD_NAME = "workloadName";
  public static final String LEAF_NODE = "leafNode";
  public static final String NON_LEAF_NODE = "nonLeafNode";

  @JsonPropertyDescription("Describes the workload name for the query workload")
  private String _workloadName;

  @JsonPropertyDescription("Describes the leaf node")
  private NodeConfig _leafNode;

  @JsonPropertyDescription("Describes the non-leaf node")
  private NodeConfig _nonLeafNode;

  @JsonCreator
  public QueryWorkloadConfig(@JsonProperty(WORKLOAD_NAME) String workloadName,
      @JsonProperty(LEAF_NODE) NodeConfig leafNode, @JsonProperty(NON_LEAF_NODE) NodeConfig nonLeafNode) {
    _workloadName = workloadName;
    _leafNode = leafNode;
    _nonLeafNode = nonLeafNode;
  }

  public String getWorkloadName() {
    return _workloadName;
  }

  public NodeConfig getLeafNode() {
    return _leafNode;
  }

  public NodeConfig getNonLeafNode() {
    return _nonLeafNode;
  }

  public void setWorkloadName(String workloadName) {
    _workloadName = workloadName;
  }

  public void setLeafNode(NodeConfig leafNode) {
    _leafNode = leafNode;
  }

  public void setNonLeafNode(NodeConfig nonLeafNode) {
    _nonLeafNode = nonLeafNode;
  }
}
