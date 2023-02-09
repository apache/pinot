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
package org.apache.pinot.segment.spi.index.creator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.segment.spi.index.reader.H3IndexResolution;
import org.apache.pinot.spi.config.table.IndexConfig;


public class H3IndexConfig extends IndexConfig {
  public static final H3IndexConfig DISABLED = new H3IndexConfig(false, null);
  public static final String RESOLUTIONS_KEY = "resolutions";

  private final H3IndexResolution _resolution;

  public H3IndexConfig(H3IndexResolution resolution) {
    this(true, resolution);
  }

  @JsonCreator
  public H3IndexConfig(@JsonProperty("enabled") @Nullable Boolean enabled,
      @JsonProperty("resolution") H3IndexResolution resolution) {
    super(enabled != null && enabled);
    _resolution = resolution;
  }

  // Used to read from older configs
  public H3IndexConfig(@Nullable Map<String, String> properties) {
    super(true);
    Preconditions.checkArgument(properties != null && properties.containsKey(RESOLUTIONS_KEY),
        "Properties must contain H3 resolutions");
    List<Integer> resolutions = new ArrayList<>();
    try {
      for (String resolution : StringUtils.split(properties.get(RESOLUTIONS_KEY), ',')) {
        resolutions.add(Integer.parseInt(resolution));
      }
      _resolution = new H3IndexResolution(resolutions);
    } catch (NumberFormatException e) {
      throw new RuntimeException("H3 index resolutions must be a list of integers, separated by comma", e);
    }
  }

  public H3IndexResolution getResolution() {
    return _resolution;
  }
}
