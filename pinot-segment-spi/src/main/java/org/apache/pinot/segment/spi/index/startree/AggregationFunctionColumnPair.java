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
package org.apache.pinot.segment.spi.index.startree;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import org.apache.commons.configuration.Configuration;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.spi.config.table.StarTreeAggregationConfig;


public class AggregationFunctionColumnPair implements Comparable<AggregationFunctionColumnPair> {
  public static final String DELIMITER = "__";
  public static final String STAR = "*";

  public static final AggregationFunctionColumnPair COUNT_STAR =
      new AggregationFunctionColumnPair(AggregationFunctionType.COUNT, STAR);

  private final AggregationFunctionType _functionType;
  private final String _column;
  private final ChunkCompressionType _chunkCompressionType;

  public AggregationFunctionColumnPair(AggregationFunctionType functionType, String column,
      ChunkCompressionType compressionType) {
    _functionType = functionType;
    if (functionType == AggregationFunctionType.COUNT) {
      _column = STAR;
    } else {
      _column = column;
    }
    _chunkCompressionType = compressionType;
  }

  public AggregationFunctionColumnPair(AggregationFunctionType functionType, String column) {
    this(functionType, column, ChunkCompressionType.PASS_THROUGH);
  }

  public static AggregationFunctionColumnPair fromConfiguration(Configuration metadataProperties) {
    if (metadataProperties == null) {
      return null;
    }
    AggregationFunctionType functionType = AggregationFunctionType.getAggregationFunctionType(
        metadataProperties.getString(StarTreeV2Constants.MetadataKey.FUNCTION_TYPE));
    String columnName = metadataProperties.getString(StarTreeV2Constants.MetadataKey.COLUMN_NAME);
    ChunkCompressionType compressionType =
        ChunkCompressionType.valueOf(metadataProperties.getString(StarTreeV2Constants.MetadataKey.COMPRESSION_TYPE));
    return new AggregationFunctionColumnPair(functionType, columnName, compressionType);
  }

  // Adds current object to Configuration by reference
  public void addToConfiguration(Configuration configuration) {
    String configPrefix = StarTreeV2Constants.MetadataKey.AGGREGATION_CONFIG + "." + this.toColumnName();
    configuration.setProperty(configPrefix + "." + StarTreeV2Constants.MetadataKey.FUNCTION_TYPE,
        _functionType.getName());
    configuration.setProperty(configPrefix + "." + StarTreeV2Constants.MetadataKey.COLUMN_NAME, _column);
    configuration.setProperty(configPrefix + "." + StarTreeV2Constants.MetadataKey.COMPRESSION_TYPE,
        _chunkCompressionType);
  }

  public AggregationFunctionType getFunctionType() {
    return _functionType;
  }

  public String getColumn() {
    return _column;
  }

  public String toColumnName() {
    return toColumnName(_functionType, _column);
  }

  public ChunkCompressionType getChunkCompressionType() {
    return _chunkCompressionType;
  }

  public static String toColumnName(AggregationFunctionType functionType, String column) {
    return functionType.getName() + DELIMITER + column;
  }

  public static AggregationFunctionColumnPair fromColumnName(String columnName) {
    String[] parts = columnName.split(DELIMITER, 2);
    AggregationFunctionType functionType = AggregationFunctionType.getAggregationFunctionType(parts[0]);
    if (functionType == AggregationFunctionType.COUNT) {
      return COUNT_STAR;
    } else {
      return new AggregationFunctionColumnPair(functionType, parts[1]);
    }
  }

  public static AggregationFunctionColumnPair fromStarTreeAggregationConfigs(
      StarTreeAggregationConfig starTreeAggregationConfig) {
    String chunkCompressionType = starTreeAggregationConfig.getChunkCompressionType();
    AggregationFunctionType aggregationFunctionType;
    try {
      ChunkCompressionType.valueOf(chunkCompressionType);
      aggregationFunctionType =
          AggregationFunctionType.getAggregationFunctionType(starTreeAggregationConfig.getAggregationFunction());
    } catch (Exception e) {
      throw new IllegalStateException(
          "Invalid aggregationConfig : " + chunkCompressionType + ". Must be one of " + Arrays.toString(
              ChunkCompressionType.values()) + ".");
    }

    return new AggregationFunctionColumnPair(aggregationFunctionType, starTreeAggregationConfig.getColumnName(),
        ChunkCompressionType.valueOf(chunkCompressionType));
  }

  @Override
  public int hashCode() {
    return Objects.hash(_functionType.hashCode(), _column.hashCode(), _chunkCompressionType.hashCode());
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof AggregationFunctionColumnPair) {
      AggregationFunctionColumnPair anotherPair = (AggregationFunctionColumnPair) obj;
      return _functionType == anotherPair._functionType && _column.equals(anotherPair._column)
          && _chunkCompressionType.equals(anotherPair._chunkCompressionType);
    }
    return false;
  }

  @Override
  public String toString() {
    return toColumnName();
  }

  @Override
  public int compareTo(AggregationFunctionColumnPair other) {
    return Comparator.comparing((AggregationFunctionColumnPair o) -> o._column)
        .thenComparing((AggregationFunctionColumnPair o) -> o._functionType)
        .thenComparing((AggregationFunctionColumnPair o) -> o._chunkCompressionType).compare(this, other);
  }
}
