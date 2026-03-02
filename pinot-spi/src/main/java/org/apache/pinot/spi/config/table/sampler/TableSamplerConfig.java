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
package org.apache.pinot.spi.config.table.sampler;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;


/**
 * Configuration for a table sampler.
 *
 * Samplers are defined in {@link org.apache.pinot.spi.config.table.TableConfig} and can be selected at query time
 * via a query option. The sampler type can be one of the built-in types, a fully qualified class name, or an alias
 * discovered via broker-side annotation scanning. Additional annotation packages can be configured via
 * {@code pinot.broker.table.sampler.annotation.packages}.
 */
public class TableSamplerConfig extends BaseJsonConfig {
  private final String _name;
  private final String _type;
  private final Map<String, String> _properties;

  @JsonCreator
  public TableSamplerConfig(@JsonProperty(value = "name", required = true) String name,
      @JsonProperty(value = "type", required = true) String type,
      @JsonProperty("properties") @Nullable Map<String, String> properties) {
    _name = name;
    _type = type;
    _properties = properties;
  }

  public String getName() {
    return _name;
  }

  public String getType() {
    return _type;
  }

  @Nullable
  public Map<String, String> getProperties() {
    return _properties;
  }
}
