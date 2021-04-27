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
 * This helps look at and operate on the pinot table configs as a whole unit.
 */
public class TableConfigs extends BaseJsonConfig {
  private final String _tableName;
  private final Schema _schema;
  private final TableConfig _offline;
  private final TableConfig _realtime;

  @JsonCreator
  public TableConfigs(@JsonProperty(value = "tableName", required = true) String tableName,
      @JsonProperty(value = "schema", required = true) Schema schema,
      @JsonProperty(value = "offline") @Nullable TableConfig offline,
      @JsonProperty(value = "realtime") @Nullable TableConfig realtime) {
    _tableName = tableName;
    _schema = schema;
    _offline = offline;
    _realtime = realtime;
  }

  public String getTableName() {
    return _tableName;
  }

  public Schema getSchema() {
    return _schema;
  }

  @Nullable
  public TableConfig getOffline() {
    return _offline;
  }

  @Nullable
  public TableConfig getRealtime() {
    return _realtime;
  }

  public String toPrettyJsonString() {
    try {
      return JsonUtils.objectToPrettyString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
