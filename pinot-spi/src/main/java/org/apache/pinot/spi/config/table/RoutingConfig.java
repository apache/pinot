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
package org.apache.pinot.spi.config.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.pinot.spi.config.BaseJsonConfig;

import java.util.List;
import javax.annotation.Nullable;


public class RoutingConfig extends BaseJsonConfig {
  public static final String PARTITION_SEGMENT_PRUNER_TYPE = "partition";
  public static final String REPLICA_GROUP_INSTANCE_SELECTOR_TYPE = "replicaGroup";

  // Replaced by _segmentPrunerTypes and _instanceSelectorType
  @Deprecated
  private final String _routingTableBuilderName;

  private final List<String> _segmentPrunerTypes;
  private final String _instanceSelectorType;

  @JsonCreator
  public RoutingConfig(@JsonProperty("routingTableBuilderName") @Nullable String routingTableBuilderName,
      @JsonProperty("segmentPrunerTypes") @Nullable List<String> segmentPrunerTypes,
      @JsonProperty("instanceSelectorType") @Nullable String instanceSelectorType) {
    _routingTableBuilderName = routingTableBuilderName;
    _segmentPrunerTypes = segmentPrunerTypes;
    _instanceSelectorType = instanceSelectorType;
  }

  @Nullable
  public String getRoutingTableBuilderName() {
    return _routingTableBuilderName;
  }

  @Nullable
  public List<String> getSegmentPrunerTypes() {
    return _segmentPrunerTypes;
  }

  @Nullable
  public String getInstanceSelectorType() {
    return _instanceSelectorType;
  }
}
