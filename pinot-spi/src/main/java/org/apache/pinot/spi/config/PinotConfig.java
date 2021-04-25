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
package org.apache.pinot.spi.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * Wrapper for all configs of a table, which include the offline table config, realtime table config and schema.
 * This helps look at and operate on the pinot configs as a whole unit.
 */
public class PinotConfig {
  private final String _configName;
  private final TableConfig _offlineTableConfig;
  private final TableConfig _realtimeTableConfig;
  private final Schema _schema;

  @JsonCreator
  public PinotConfig(@JsonProperty(value = "configName", required = true) String configName,
      @JsonProperty(value = "offlineTableConfig") @Nullable TableConfig offlineTableConfig,
      @JsonProperty(value = "realtimeTableConfig") @Nullable TableConfig realtimeTableConfig,
      @JsonProperty(value = "schema", required = true) Schema schema) {
    _configName = configName;
    _offlineTableConfig = offlineTableConfig;
    _realtimeTableConfig = realtimeTableConfig;
    _schema = schema;
  }

  public String getConfigName() {
    return _configName;
  }

  @Nullable
  public TableConfig getOfflineTableConfig() {
    return _offlineTableConfig;
  }

  @Nullable
  public TableConfig getRealtimeTableConfig() {
    return _realtimeTableConfig;
  }

  public Schema getSchema() {
    return _schema;
  }

  public String toJsonString() {
    try {
      return JsonUtils.objectToPrettyString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
