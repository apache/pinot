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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;
import org.apache.pinot.spi.config.table.FieldConfig.CompressionCodec;
import org.apache.pinot.spi.utils.DataSizeUtils;


public class StarTreeAggregationConfig extends BaseJsonConfig {
  private final String _columnName;
  private final String _aggregationFunction;
  private final Map<String, Object> _functionParameters;
  private final CompressionCodec _compressionCodec;
  private final Boolean _deriveNumDocsPerChunk;
  private final Integer _indexVersion;
  private final String _targetMaxChunkSize;
  private final Integer _targetMaxChunkSizeBytes;
  private final Integer _targetDocsPerChunk;

  @VisibleForTesting
  public StarTreeAggregationConfig(String columnName, String aggregationFunction) {
    this(columnName, aggregationFunction, null, null, null, null, null, null);
  }

  @JsonCreator
  public StarTreeAggregationConfig(@JsonProperty(value = "columnName", required = true) String columnName,
      @JsonProperty(value = "aggregationFunction", required = true) String aggregationFunction,
      @JsonProperty(value = "functionParameters") @Nullable Map<String, Object> functionParameters,
      @JsonProperty(value = "compressionCodec") @Nullable CompressionCodec compressionCodec,
      @JsonProperty(value = "deriveNumDocsPerChunk") @Nullable Boolean deriveNumDocsPerChunk,
      @JsonProperty(value = "indexVersion") @Nullable Integer indexVersion,
      @JsonProperty(value = "targetMaxChunkSize") @Nullable String targetMaxChunkSize,
      @JsonProperty(value = "targetDocsPerChunk") @Nullable Integer targetDocsPerChunk) {
    _columnName = columnName;
    _aggregationFunction = aggregationFunction;
    _functionParameters = functionParameters;
    _compressionCodec = compressionCodec;
    _deriveNumDocsPerChunk = deriveNumDocsPerChunk;
    _indexVersion = indexVersion;
    _targetMaxChunkSize = targetMaxChunkSize;
    _targetMaxChunkSizeBytes = targetMaxChunkSize != null ? (int) DataSizeUtils.toBytes(targetMaxChunkSize) : null;
    _targetDocsPerChunk = targetDocsPerChunk;
  }

  public String getColumnName() {
    return _columnName;
  }

  public String getAggregationFunction() {
    return _aggregationFunction;
  }

  @Nullable
  public Map<String, Object> getFunctionParameters() {
    return _functionParameters;
  }

  @Nullable
  public CompressionCodec getCompressionCodec() {
    return _compressionCodec;
  }

  @Nullable
  public Boolean getDeriveNumDocsPerChunk() {
    return _deriveNumDocsPerChunk;
  }

  @Nullable
  public Integer getIndexVersion() {
    return _indexVersion;
  }

  @Nullable
  public String getTargetMaxChunkSize() {
    return _targetMaxChunkSize;
  }

  @JsonIgnore
  @Nullable
  public Integer getTargetMaxChunkSizeBytes() {
    return _targetMaxChunkSizeBytes;
  }

  @Nullable
  public Integer getTargetDocsPerChunk() {
    return _targetDocsPerChunk;
  }
}
