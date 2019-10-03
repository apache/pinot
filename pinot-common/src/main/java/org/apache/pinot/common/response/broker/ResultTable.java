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
import java.io.Serializable;
import java.util.List;


/**
 * Holds the results in a standard tabular structure
 *
 * FIXME: Column types and Multi-value support are missing, and deserialize might not work properly
 */
@JsonPropertyOrder({"columns", "rows"})
public class ResultTable {
  final private List<String> _columns;
  final private List<Serializable[]> _rows;

  @JsonCreator
  public ResultTable(@JsonProperty("columns") List<String> columns,
      @JsonProperty("rows") List<Serializable[]> rows) {
    _columns = columns;
    _rows = rows;
  }

  @JsonProperty("columns")
  public List<String> getColumns() {
    return _columns;
  }

  @JsonProperty("results")
  public List<Serializable[]> getRows() {
    return _rows;
  }
}
