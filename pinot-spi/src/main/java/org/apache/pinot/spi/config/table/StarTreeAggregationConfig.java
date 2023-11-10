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
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;
import org.apache.pinot.spi.config.table.FieldConfig.CompressionCodec;


public class StarTreeAggregationConfig extends BaseJsonConfig {
  private final String _columnName;
  private final String _aggregationFunction;
  private final CompressionCodec _compressionCodec;

  @JsonCreator
  public StarTreeAggregationConfig(@JsonProperty(value = "columnName", required = true) String columnName,
      @JsonProperty(value = "aggregationFunction", required = true) String aggregationFunction,
      @JsonProperty(value = "compressionCodec") @Nullable CompressionCodec compressionCodec) {
    _columnName = columnName;
    _aggregationFunction = aggregationFunction;
    _compressionCodec = compressionCodec != null ? compressionCodec : CompressionCodec.PASS_THROUGH;
  }

  public String getColumnName() {
    return _columnName;
  }

  public String getAggregationFunction() {
    return _aggregationFunction;
  }

  public CompressionCodec getCompressionCodec() {
    return _compressionCodec;
  }
}
