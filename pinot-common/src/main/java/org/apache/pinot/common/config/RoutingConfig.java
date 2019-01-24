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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.utils.EqualityUtils;


@JsonIgnoreProperties(ignoreUnknown = true)
public class RoutingConfig {
  public static final String ENABLE_DYNAMIC_COMPUTING_KEY = "enableDynamicComputing";

  @ConfigKey("routingTableBuilderName")
  private String _routingTableBuilderName;

  private Map<String, String> _routingTableBuilderOptions = new HashMap<>();

  public String getRoutingTableBuilderName() {
    return _routingTableBuilderName;
  }

  public void setRoutingTableBuilderName(String routingTableBuilderName) {
    _routingTableBuilderName = routingTableBuilderName;
  }

  public Map<String, String> getRoutingTableBuilderOptions() {
    return _routingTableBuilderOptions;
  }

  public void setRoutingTableBuilderOptions(Map<String, String> routingTableBuilderOptions) {
    _routingTableBuilderOptions = routingTableBuilderOptions;
  }

  @Override
  public boolean equals(Object o) {
    if (EqualityUtils.isSameReference(this, o)) {
      return true;
    }

    if (EqualityUtils.isNullOrNotSameClass(this, o)) {
      return false;
    }

    RoutingConfig that = (RoutingConfig) o;

    return EqualityUtils.isEqual(_routingTableBuilderName, that._routingTableBuilderName) && EqualityUtils
        .isEqual(_routingTableBuilderOptions, that._routingTableBuilderOptions);
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(_routingTableBuilderName);
    result = EqualityUtils.hashCodeOf(result, _routingTableBuilderOptions);
    return result;
  }
}
