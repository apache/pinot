/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.thirdeye.api;

import java.util.Properties;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DimensionSpec {
  private String name;
  private String alias;
  private DimensionType type;
  private Properties config;

  public DimensionSpec() {
  }

  public DimensionSpec(String name) {
    this.name = name;
  }

  public DimensionSpec(String name, DimensionType type, Properties config) {
    this.name = name;
    this.type = type;
    this.config = config;
  }

  @JsonProperty
  public String getName() {
    return name;
  }

  @JsonProperty
  public String getAlias() {
    return alias;
  }

  @JsonProperty
  public DimensionType getType() {
    return type;
  }

  @JsonProperty
  public Properties getConfig() {
    return config;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof DimensionSpec)) {
      return false;
    }

    DimensionSpec d = (DimensionSpec) o;

    return name.equals(d.getName());
  }
}
