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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.controller.helix.core.WatermarkInductionResult;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class CopyTableResponse {
  @JsonProperty("msg")
  private String _msg;

  @JsonProperty("status")
  private String _status;

  @JsonProperty("schema")
  private Schema _schema;

  @JsonProperty("realtimeTableConfig")
  private TableConfig _tableConfig;

  @JsonProperty("watermarkInductionResult")
  private WatermarkInductionResult _watermarkInductionResult;

  @JsonProperty("deprecationWarnings")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private List<String> _deprecationWarnings;

  @JsonCreator
  public CopyTableResponse(@JsonProperty("status") String status, @JsonProperty("msg") String msg,
      @JsonProperty("schema") @Nullable Schema schema,
      @JsonProperty("realtimeTableConfig") @Nullable TableConfig tableConfig,
      @JsonProperty("watermarkInductionResult") @Nullable WatermarkInductionResult watermarkInductionResult) {
    this(status, msg, schema, tableConfig, watermarkInductionResult, null);
  }

  public CopyTableResponse(String status, String msg, @Nullable Schema schema, @Nullable TableConfig tableConfig,
      @Nullable WatermarkInductionResult watermarkInductionResult, @Nullable List<String> deprecationWarnings) {
    _status = status;
    _msg = msg;
    _schema = schema;
    _tableConfig = tableConfig;
    _watermarkInductionResult = watermarkInductionResult;
    _deprecationWarnings = deprecationWarnings == null ? List.of() : deprecationWarnings;
  }

  public String getMsg() {
    return _msg;
  }

  public void setMsg(String msg) {
    _msg = msg;
  }

  public String getStatus() {
    return _status;
  }

  public void setStatus(String status) {
    _status = status;
  }

  public Schema getSchema() {
    return _schema;
  }

  public void setSchema(Schema schema) {
    _schema = schema;
  }

  public TableConfig getTableConfig() {
    return _tableConfig;
  }

  public void setTableConfig(TableConfig tableConfig) {
    _tableConfig = tableConfig;
  }

  public WatermarkInductionResult getWatermarkInductionResult() {
    return _watermarkInductionResult;
  }

  public void setWatermarkInductionResult(
      WatermarkInductionResult watermarkInductionResult) {
    _watermarkInductionResult = watermarkInductionResult;
  }

  public List<String> getDeprecationWarnings() {
    return _deprecationWarnings;
  }

  public void setDeprecationWarnings(@Nullable List<String> deprecationWarnings) {
    _deprecationWarnings = deprecationWarnings == null ? List.of() : deprecationWarnings;
  }
}
