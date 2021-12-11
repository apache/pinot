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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
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
  private final FilterConfig _filterConfig;

  @JsonPropertyDescription("Configs related to record transformation functions applied during ingestion")
  private final List<TransformConfig> _transformConfigs;

  @JsonPropertyDescription("Config related to handling complex type")
  private final ComplexTypeConfig _complexTypeConfig;

  @JsonCreator
  public IngestionConfig(@JsonProperty("batchIngestionConfig") @Nullable BatchIngestionConfig batchIngestionConfig,
      @JsonProperty("streamIngestionConfig") @Nullable StreamIngestionConfig streamIngestionConfig,
      @JsonProperty("filterConfig") @Nullable FilterConfig filterConfig,
      @JsonProperty("transformConfigs") @Nullable List<TransformConfig> transformConfigs,
      @JsonProperty("complexTypeConfig") @Nullable ComplexTypeConfig complexTypeConfig) {
    _batchIngestionConfig = batchIngestionConfig;
    _streamIngestionConfig = streamIngestionConfig;
    _filterConfig = filterConfig;
    _transformConfigs = transformConfigs;
    _complexTypeConfig = complexTypeConfig;
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

  public void setBatchIngestionConfig(BatchIngestionConfig batchIngestionConfig) {
    _batchIngestionConfig = batchIngestionConfig;
  }

  public void setStreamIngestionConfig(StreamIngestionConfig streamIngestionConfig) {
    _streamIngestionConfig = streamIngestionConfig;
  }
}
