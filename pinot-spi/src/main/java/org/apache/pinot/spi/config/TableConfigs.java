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
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;


/**
 * Wrapper for all configs of a table, which include the offline table config, realtime table config and schema.
 * This helps look at and operate on the pinot table configs as a whole unit.
 */
public class TableConfigs extends BaseJsonConfig {
  private String _tableName;
  private final Schema _schema;
  private final TableConfig _offline;
  private final TableConfig _realtime;

  @JsonCreator
  public TableConfigs(@JsonProperty(value = "tableName", required = true) String tableName,
      @JsonProperty(value = "schema", required = true) Schema schema,
      @JsonProperty(value = "offline") @Nullable TableConfig offline,
      @JsonProperty(value = "realtime") @Nullable TableConfig realtime) {
    Preconditions.checkState(StringUtils.isNotBlank(tableName), "'tableName' cannot be null or empty in TableConfigs");
    Preconditions.checkNotNull(schema, "'schema' cannot be null in TableConfigs");
    _tableName = tableName;
    _schema = schema;
    _offline = offline;
    _realtime = realtime;
  }

  public String getTableName() {
    return _tableName;
  }

  public void setTableName(String rawTableName) {
    _tableName = rawTableName;
    _schema.setSchemaName(rawTableName);
    if (_offline != null) {
      _offline.setTableName(TableNameBuilder.OFFLINE.tableNameWithType(rawTableName));
    }
    if (_realtime != null) {
      _realtime.setTableName(TableNameBuilder.REALTIME.tableNameWithType(rawTableName));
    }
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

  private ObjectNode toJsonObject() {
    ObjectNode tableConfigsObjectNode = JsonUtils.newObjectNode();
    tableConfigsObjectNode.put("tableName", _tableName);
    tableConfigsObjectNode.set("schema", _schema.toJsonObject());
    if (_offline != null) {
      tableConfigsObjectNode.set("offline", _offline.toJsonNode());
    }
    if (_realtime != null) {
      tableConfigsObjectNode.set("realtime", _realtime.toJsonNode());
    }
    return tableConfigsObjectNode;
  }

  @Override
  public String toJsonString() {
    try {
      return JsonUtils.objectToString(toJsonObject());
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public String toPrettyJsonString() {
    try {
      return JsonUtils.objectToPrettyString(toJsonObject());
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
