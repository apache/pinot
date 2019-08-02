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
package org.apache.pinot.common.config.instance;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.pinot.common.config.ConfigDoc;
import org.apache.pinot.common.config.ConfigKey;
import org.apache.pinot.common.config.NestedConfig;


@JsonIgnoreProperties(ignoreUnknown = true)
public class InstanceAssignmentConfig {

  @ConfigKey("tagPoolConfig")
  @ConfigDoc(value = "Configuration for the instance tag and pool of the instance assignment", mandatory = true)
  @NestedConfig
  private InstanceTagPoolConfig _tagPoolConfig;

  @ConfigKey("constraintConfig")
  @ConfigDoc("Configuration for the instance constraints of the instance assignment, which filters out unqualified instances and sorts instances for picking priority")
  @NestedConfig
  private InstanceConstraintConfig _constraintConfig;

  @ConfigKey("replicaPartitionConfig")
  @ConfigDoc(value = "Configuration for the instance replica and partition of the instance assignment", mandatory = true)
  @NestedConfig
  private InstanceReplicaPartitionConfig _replicaPartitionConfig;

  @JsonProperty
  public InstanceTagPoolConfig getTagPoolConfig() {
    return _tagPoolConfig;
  }

  @JsonProperty
  public void setTagPoolConfig(InstanceTagPoolConfig tagPoolConfig) {
    _tagPoolConfig = tagPoolConfig;
  }

  @JsonProperty
  public InstanceConstraintConfig getConstraintConfig() {
    return _constraintConfig;
  }

  @JsonProperty
  public void setConstraintConfig(InstanceConstraintConfig constraintConfig) {
    _constraintConfig = constraintConfig;
  }

  @JsonProperty
  public InstanceReplicaPartitionConfig getReplicaPartitionConfig() {
    return _replicaPartitionConfig;
  }

  @JsonProperty
  public void setReplicaPartitionConfig(InstanceReplicaPartitionConfig replicaPartitionConfig) {
    _replicaPartitionConfig = replicaPartitionConfig;
  }
}
