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
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;


public class TenantConfig extends BaseJsonConfig {

  @JsonPropertyDescription("Broker tag prefix used by this table")
  private final String _broker;

  @JsonPropertyDescription("Server tag prefix used by this table")
  private final String _server;

  @JsonPropertyDescription("Overrides for tags")
  private final TagOverrideConfig _tagOverrideConfig;

  @JsonCreator
  public TenantConfig(@JsonProperty("broker") @Nullable String broker, @JsonProperty("server") @Nullable String server,
      @JsonProperty("tagOverrideConfig") @Nullable TagOverrideConfig tagOverrideConfig) {
    _broker = broker;
    _server = server;
    _tagOverrideConfig = tagOverrideConfig;
  }

  @Nullable
  public String getBroker() {
    return _broker;
  }

  @Nullable
  public String getServer() {
    return _server;
  }

  @Nullable
  public TagOverrideConfig getTagOverrideConfig() {
    return _tagOverrideConfig;
  }
}
