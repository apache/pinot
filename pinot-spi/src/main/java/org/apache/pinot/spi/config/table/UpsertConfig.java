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
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;


/** Class representing upsert configuration of a table. */
public class UpsertConfig extends BaseJsonConfig {

  public enum Mode {
    FULL, PARTIAL, NONE
  }

  public enum Strategy {
    // Todo: add CUSTOM strategies
    APPEND, IGNORE, INCREMENT, OVERWRITE, UNION
  }

  @JsonPropertyDescription("Upsert mode.")
  private Mode _mode;

  @JsonPropertyDescription("Function to hash the primary key.")
  private HashFunction _hashFunction = HashFunction.NONE;

  @JsonPropertyDescription("Partial update strategies.")
  private Map<String, Strategy> _partialUpsertStrategies;

  @JsonPropertyDescription("default upsert strategy for partial mode")
  private Strategy _defaultPartialUpsertStrategy;

  @JsonPropertyDescription("Column for upsert comparison, default to time column")
  private String _comparisonColumn;

  @JsonPropertyDescription("Whether to use snapshot for fast upsert metadata recovery")
  private boolean _enableSnapshot;

  @JsonPropertyDescription("Custom class for upsert metadata manager")
  private String _metadataManagerClass;

  @JsonPropertyDescription("Custom configs for upsert metadata manager")
  private Map<String, String> _metadataManagerConfigs;

  @Deprecated
  public UpsertConfig(@JsonProperty(value = "mode", required = true) Mode mode,
      @JsonProperty("partialUpsertStrategies") @Nullable Map<String, Strategy> partialUpsertStrategies,
      @JsonProperty("defaultPartialUpsertStrategy") @Nullable Strategy defaultPartialUpsertStrategy,
      @JsonProperty("comparisonColumn") @Nullable String comparisonColumn,
      @JsonProperty("hashFunction") @Nullable HashFunction hashFunction) {
    Preconditions.checkArgument(mode != null, "Upsert mode must be configured");
    _mode = mode;

    if (mode == Mode.PARTIAL) {
      _partialUpsertStrategies = partialUpsertStrategies != null ? partialUpsertStrategies : new HashMap<>();
      _defaultPartialUpsertStrategy =
          defaultPartialUpsertStrategy != null ? defaultPartialUpsertStrategy : Strategy.OVERWRITE;
    } else {
      _partialUpsertStrategies = null;
      _defaultPartialUpsertStrategy = null;
    }

    _comparisonColumn = comparisonColumn;
    _hashFunction = hashFunction == null ? HashFunction.NONE : hashFunction;
  }

  public UpsertConfig(Mode mode) {
    _mode = mode;
  }

  // Do not use this constructor. This is needed for JSON deserialization.
  public UpsertConfig() {
  }

  public Mode getMode() {
    return _mode;
  }

  public HashFunction getHashFunction() {
    return _hashFunction;
  }

  @Nullable
  public Map<String, Strategy> getPartialUpsertStrategies() {
    return _partialUpsertStrategies;
  }

  public Strategy getDefaultPartialUpsertStrategy() {
    return _defaultPartialUpsertStrategy;
  }

  public String getComparisonColumn() {
    return _comparisonColumn;
  }

  public boolean isEnableSnapshot() {
    return _enableSnapshot;
  }

  @Nullable
  public String getMetadataManagerClass() {
    return _metadataManagerClass;
  }

  @Nullable
  public Map<String, String> getMetadataManagerConfigs() {
    return _metadataManagerConfigs;
  }

  public void setHashFunction(HashFunction hashFunction) {
    _hashFunction = hashFunction;
  }

  /**
   * PartialUpsertStrategies maintains the mapping of merge strategies per column.
   * Each key in the map is a columnName, value is a partial upsert merging strategy.
   * Supported strategies are {OVERWRITE|INCREMENT|APPEND|UNION|IGNORE}.
   */
  public void setPartialUpsertStrategies(Map<String, Strategy> partialUpsertStrategies) {
    _partialUpsertStrategies = partialUpsertStrategies;
  }

  /**
   * If strategy is not specified for a column, the merger on that column will be "defaultPartialUpsertStrategy".
   * The default value of defaultPartialUpsertStrategy is OVERWRITE.
   */
  public void setDefaultPartialUpsertStrategy(Strategy defaultPartialUpsertStrategy) {
    _defaultPartialUpsertStrategy = defaultPartialUpsertStrategy;
  }

  /**
   * By default, Pinot uses the value in the time column to determine the latest record. For two records with the
   * same primary key, the record with the larger value of the time column is picked as the
   * latest update.
   * However, there are cases when users need to use another column to determine the order.
   * In such case, you can use option comparisonColumn to override the column used for comparison.
   */
  public void setComparisonColumn(String comparisonColumn) {
    _comparisonColumn = comparisonColumn;
  }

  public void setEnableSnapshot(boolean enableSnapshot) {
    _enableSnapshot = enableSnapshot;
  }

  public void setMetadataManagerClass(String metadataManagerClass) {
    _metadataManagerClass = metadataManagerClass;
  }

  public void setMetadataManagerConfigs(Map<String, String> metadataManagerConfigs) {
    _metadataManagerConfigs = metadataManagerConfigs;
  }
}
