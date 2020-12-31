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
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.spi.config.BaseJsonConfig;


public class H3IndexColumn extends BaseJsonConfig {
  private static final String RESOLUTIONS_NAME = "resolutions";
  // the column to build h3 index
  private final String _name;
  // the index resolutions
  private final List<Integer> _resolutions;

  @JsonCreator
  public H3IndexColumn(@JsonProperty(value = "name", required = true) String name,
      @JsonProperty(value = RESOLUTIONS_NAME, required = true) List<Integer> resolutions) {
    Preconditions.checkArgument(CollectionUtils.isNotEmpty(resolutions), "'resolutions must be configured'");
    _name = name;
    _resolutions = resolutions;
  }

  public H3IndexColumn(String name, Map<String, String> properties) {
    _name = name;
    Preconditions.checkArgument(properties.containsKey(RESOLUTIONS_NAME), "Properties must contain resolutions");
    String resolutionsStr = properties.get(RESOLUTIONS_NAME);
    List<Integer> resolutions = new ArrayList<>();
    try {
      for (String resolution : resolutionsStr.split(",")) {
        resolutions.add(Integer.parseInt(resolution));
      }
      _resolutions = resolutions;
    } catch (NumberFormatException e) {
      throw new RuntimeException("H3 index resolutions must be a list of integers, separated by comma", e);
    }
  }

  public List<Integer> getResolutions() {
    return _resolutions;
  }

  public String getName() {
    return _name;
  }
}
