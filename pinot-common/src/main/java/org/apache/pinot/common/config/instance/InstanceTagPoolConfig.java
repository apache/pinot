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
import java.util.List;
import org.apache.pinot.common.config.ConfigDoc;
import org.apache.pinot.common.config.ConfigKey;


@JsonIgnoreProperties(ignoreUnknown = true)
public class InstanceTagPoolConfig {

  @ConfigKey("tag")
  @ConfigDoc(value = "Tag of the instances to select", mandatory = true)
  private String _tag;

  @ConfigKey("poolBased")
  @ConfigDoc("Whether to use pool based selection, false by default")
  private boolean _poolBased;

  @ConfigKey("numPools")
  @ConfigDoc("Number of pools to select for pool based selection, contradict to pools, select all pools if neither of them are specified")
  private int _numPools;

  @ConfigKey("pools")
  @ConfigDoc("Pools to select for pool based selection, contradict to numPools, select all pools if neither of them are specified")
  private List<Integer> _pools;

  @JsonProperty
  public String getTag() {
    return _tag;
  }

  @JsonProperty
  public void setTag(String tag) {
    _tag = tag;
  }

  @JsonProperty
  public boolean isPoolBased() {
    return _poolBased;
  }

  @JsonProperty
  public void setPoolBased(boolean poolBased) {
    _poolBased = poolBased;
  }

  @JsonProperty
  public int getNumPools() {
    return _numPools;
  }

  @JsonProperty
  public void setNumPools(int numPools) {
    _numPools = numPools;
  }

  @JsonProperty
  public List<Integer> getPools() {
    return _pools;
  }

  @JsonProperty
  public void setPools(List<Integer> pools) {
    _pools = pools;
  }
}
