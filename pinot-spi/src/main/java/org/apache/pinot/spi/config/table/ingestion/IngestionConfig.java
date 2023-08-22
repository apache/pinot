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
package org.apache.pinot.spi.config.table.ingestion;

import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;


/**
 * Class representing table ingestion configuration i.e. all configs related to the data source and the ingestion
 * properties and operations
 */
public class IngestionConfig extends BaseJsonConfig {

  @JsonPropertyDescription("Config related to the batch data sources")
  private BatchIngestionConfig _batchIngestionConfig;

  @JsonPropertyDescription("Config related to the stream data sources")
  private StreamIngestionConfig _streamIngestionConfig;

  @JsonPropertyDescription("Config related to filtering records during ingestion")
  private FilterConfig _filterConfig;

  @JsonPropertyDescription("Configs related to record transformation functions applied during ingestion")
  private List<TransformConfig> _transformConfigs;

  @JsonPropertyDescription("Config related to handling complex type")
  private ComplexTypeConfig _complexTypeConfig;

  @JsonPropertyDescription("Config related to the SchemaConformingTransformer")
  private SchemaConformingTransformerConfig _schemaConformingTransformerConfig;

  @JsonPropertyDescription("Configs related to record aggregation function applied during ingestion")
  private List<AggregationConfig> _aggregationConfigs;

  @JsonPropertyDescription("Configs related to skip any row which has error and continue during ingestion")
  private boolean _continueOnError;

  @JsonPropertyDescription("Configs related to validate time value for each record during ingestion")
  private boolean _rowTimeValueCheck;

  @JsonPropertyDescription("Configs related to check time value for segment")
  private boolean _segmentTimeValueCheck = true;

  @Deprecated
  public IngestionConfig(@Nullable BatchIngestionConfig batchIngestionConfig,
      @Nullable StreamIngestionConfig streamIngestionConfig, @Nullable FilterConfig filterConfig,
      @Nullable List<TransformConfig> transformConfigs, @Nullable ComplexTypeConfig complexTypeConfig,
      @Nullable SchemaConformingTransformerConfig schemaConformingTransformerConfig,
      @Nullable List<AggregationConfig> aggregationConfigs) {
    _batchIngestionConfig = batchIngestionConfig;
    _streamIngestionConfig = streamIngestionConfig;
    _filterConfig = filterConfig;
    _transformConfigs = transformConfigs;
    _complexTypeConfig = complexTypeConfig;
    _schemaConformingTransformerConfig = schemaConformingTransformerConfig;
    _aggregationConfigs = aggregationConfigs;
  }

  public IngestionConfig() {
  }

  @Nullable
  public BatchIngestionConfig getBatchIngestionConfig() {
    return _batchIngestionConfig;
  }

  @Nullable
  public StreamIngestionConfig getStreamIngestionConfig() {
    return _streamIngestionConfig;
  }

  @Nullable
  public FilterConfig getFilterConfig() {
    return _filterConfig;
  }

  @Nullable
  public List<TransformConfig> getTransformConfigs() {
    return _transformConfigs;
  }

  @Nullable
  public ComplexTypeConfig getComplexTypeConfig() {
    return _complexTypeConfig;
  }

  @Nullable
  public SchemaConformingTransformerConfig getSchemaConformingTransformerConfig() {
    return _schemaConformingTransformerConfig;
  }

  @Nullable
  public List<AggregationConfig> getAggregationConfigs() {
    return _aggregationConfigs;
  }

  public boolean isContinueOnError() {
    return _continueOnError;
  }

  public boolean isRowTimeValueCheck() {
    return _rowTimeValueCheck;
  }

  public boolean isSegmentTimeValueCheck() {
    return _segmentTimeValueCheck;
  }

  public void setBatchIngestionConfig(BatchIngestionConfig batchIngestionConfig) {
    _batchIngestionConfig = batchIngestionConfig;
  }

  public void setStreamIngestionConfig(StreamIngestionConfig streamIngestionConfig) {
    _streamIngestionConfig = streamIngestionConfig;
  }

  public void setFilterConfig(FilterConfig filterConfig) {
    _filterConfig = filterConfig;
  }

  public void setTransformConfigs(List<TransformConfig> transformConfigs) {
    _transformConfigs = transformConfigs;
  }

  public void setComplexTypeConfig(ComplexTypeConfig complexTypeConfig) {
    _complexTypeConfig = complexTypeConfig;
  }

  public void setSchemaConformingTransformerConfig(
      SchemaConformingTransformerConfig schemaConformingTransformerConfig) {
    _schemaConformingTransformerConfig = schemaConformingTransformerConfig;
  }

  public void setAggregationConfigs(List<AggregationConfig> aggregationConfigs) {
    _aggregationConfigs = aggregationConfigs;
  }

  public void setContinueOnError(boolean continueOnError) {
    _continueOnError = continueOnError;
  }

  public void setRowTimeValueCheck(boolean rowTimeValueCheck) {
    _rowTimeValueCheck = rowTimeValueCheck;
  }

  public void setSegmentTimeValueCheck(boolean segmentTimeValueCheck) {
    _segmentTimeValueCheck = segmentTimeValueCheck;
  }
}
