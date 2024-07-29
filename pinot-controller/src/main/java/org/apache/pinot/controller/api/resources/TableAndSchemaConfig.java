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
package org.apache.pinot.controller.api.resources;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.TableConfigs;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * Wrapper for TableConfig and Schema used in validation API
 * @deprecated Use {@link TableConfigs} instead.
 */
@Deprecated
public class TableAndSchemaConfig {
  private final TableConfig _tableConfig;
  private final Schema _schema;

  @JsonCreator
  public TableAndSchemaConfig(@JsonProperty(value = "tableConfig", required = true) TableConfig tableConfig,
      @JsonProperty("schema") @Nullable Schema schema) {
    _tableConfig = tableConfig;
    _schema = schema;
  }

  public TableConfig getTableConfig() {
    return _tableConfig;
  }

  @Nullable
  public Schema getSchema() {
    return _schema;
  }

  public String toJsonString() {
    try {
      return JsonUtils.objectToString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
