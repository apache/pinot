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
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;


/**
 * Contains all configs related to the batch sources for ingestion.
 */
public class BatchIngestionConfig extends BaseJsonConfig {

  @JsonPropertyDescription("Configs for all the batch sources to ingest from")
  private final List<Map<String, String>> _batchConfigMaps;

  @JsonPropertyDescription("Ingestion type APPEND or REFRESH")
  private final String _segmentIngestionType;

  @JsonPropertyDescription("Ingestion frequency HOURLY or DAILY")
  private final String _segmentIngestionFrequency;

  @JsonCreator
  public BatchIngestionConfig(@JsonProperty("batchConfigMaps") @Nullable List<Map<String, String>> batchConfigMaps,
      @JsonProperty("segmentIngestionType") String segmentIngestionType,
      @JsonProperty("segmentIngestionFrequency") String segmentIngestionFrequency) {
    _batchConfigMaps = batchConfigMaps;
    _segmentIngestionType = segmentIngestionType;
    _segmentIngestionFrequency = segmentIngestionFrequency;
  }

  @Nullable
  public List<Map<String, String>> getBatchConfigMaps() {
    return _batchConfigMaps;
  }

  public String getSegmentIngestionType() {
    return _segmentIngestionType;
  }

  public String getSegmentIngestionFrequency() {
    return _segmentIngestionFrequency;
  }
}
