/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.config;

import com.linkedin.pinot.common.utils.EqualityUtils;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@JsonIgnoreProperties(ignoreUnknown = true)
public class RoutingConfig {

  private static final Logger LOGGER = LoggerFactory.getLogger(RoutingConfig.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private String _routingTableBuilderName;

  private Map<String,String> _routingTableBuilderOptions = new HashMap<>();


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

  public String toString() {
    try {
      return OBJECT_MAPPER.writeValueAsString(this);
    } catch (IOException e) {
      //ignore
    }
    return "";
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

    return EqualityUtils.isEqual(_routingTableBuilderName, that._routingTableBuilderName) && EqualityUtils.isEqual(
        _routingTableBuilderOptions, that._routingTableBuilderOptions);
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(_routingTableBuilderName);
    result = EqualityUtils.hashCodeOf(result, _routingTableBuilderOptions);
    return result;
  }
}
