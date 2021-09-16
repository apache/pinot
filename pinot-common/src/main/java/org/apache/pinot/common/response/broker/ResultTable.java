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
package org.apache.pinot.common.response.broker;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.util.List;
import org.apache.pinot.spi.utils.DataSchema;


/**
 * A tabular structure for representing result rows
 */
@JsonPropertyOrder({"dataSchema", "rows"})
public class ResultTable {
  private final DataSchema _dataSchema;
  private final List<Object[]> _rows;

  @JsonCreator
  public ResultTable(@JsonProperty("dataSchema") DataSchema dataSchema, @JsonProperty("rows") List<Object[]> rows) {
    _dataSchema = dataSchema;
    _rows = rows;
  }

  @JsonProperty("dataSchema")
  public DataSchema getDataSchema() {
    return _dataSchema;
  }

  @JsonProperty("rows")
  public List<Object[]> getRows() {
    return _rows;
  }
}
