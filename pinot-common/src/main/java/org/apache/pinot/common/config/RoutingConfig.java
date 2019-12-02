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
package org.apache.pinot.common.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import javax.annotation.Nullable;


public class RoutingConfig extends BaseJsonConfig {
  public static final String ENABLE_DYNAMIC_COMPUTING_KEY = "enableDynamicComputing";

  private final String _routingTableBuilderName;
  private final Map<String, String> _routingTableBuilderOptions;

  @JsonCreator
  public RoutingConfig(@JsonProperty("routingTableBuilderName") @Nullable String routingTableBuilderName,
      @JsonProperty("routingTableBuilderOptions") @Nullable Map<String, String> routingTableBuilderOptions) {
    _routingTableBuilderName = routingTableBuilderName;
    _routingTableBuilderOptions = routingTableBuilderOptions;
  }

  @Nullable
  public String getRoutingTableBuilderName() {
    return _routingTableBuilderName;
  }

  @Nullable
  public Map<String, String> getRoutingTableBuilderOptions() {
    return _routingTableBuilderOptions;
  }
}
