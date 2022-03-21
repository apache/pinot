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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.pinot.spi.config.BaseJsonConfig;


public class PreAggregationConfig extends BaseJsonConfig {

  private final String _srcColumn;
  private final String _aggregationFunctionType;
  private final String _destColumn;

  public PreAggregationConfig(@JsonProperty(value = "srcColumn", required = true) String srcColumn,
      @JsonProperty(value = "aggregationFunctionType", required = true) String aggregationFunctionType,
      @JsonProperty(value = "destColumn", required = true) String destColumn) {
    Preconditions.checkArgument(srcColumn.length() > 0, "'srcColumn' must be a non-empty string");
    Preconditions.checkArgument(aggregationFunctionType.length() > 0,
        "'aggregationFunctionType' must be a non-empty string");
    Preconditions.checkArgument(destColumn.length() > 0, "'destColumn' must be a non-empty string");
    _srcColumn = srcColumn;
    _aggregationFunctionType = aggregationFunctionType;
    _destColumn = destColumn;
  }
}
