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
package org.apache.pinot.spi.config.table.assignment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.google.common.base.Preconditions;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;


public class InstanceTagPoolConfig extends BaseJsonConfig {

  @JsonPropertyDescription("Tag of the instances to select (mandatory)")
  private final String _tag;

  @JsonPropertyDescription("Whether to use pool based selection, false by default")
  private final boolean _poolBased;

  @JsonPropertyDescription("Number of pools to select for pool based selection, contradict to pools, select all pools if neither of them are specified")
  private final int _numPools;

  @JsonPropertyDescription("Pools to select for pool based selection, contradict to numPools, select all pools if neither of them are specified")
  private final List<Integer> _pools;

  @JsonCreator
  public InstanceTagPoolConfig(@JsonProperty(value = "tag", required = true) String tag,
      @JsonProperty("poolBased") boolean poolBased, @JsonProperty("numPools") int numPools,
      @JsonProperty("pools") @Nullable List<Integer> pools) {
    Preconditions.checkArgument(tag != null, "'tag' must be configured");
    _tag = tag;
    _poolBased = poolBased;
    _numPools = numPools;
    _pools = pools;
  }

  public String getTag() {
    return _tag;
  }

  public boolean isPoolBased() {
    return _poolBased;
  }

  public int getNumPools() {
    return _numPools;
  }

  @Nullable
  public List<Integer> getPools() {
    return _pools;
  }
}
