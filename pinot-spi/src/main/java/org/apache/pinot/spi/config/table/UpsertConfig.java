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
    APPEND, INCREMENT, OVERWRITE, UNION
  }

  public enum HashFunction {
    NONE, MD5, MURMUR3
  }

  @JsonPropertyDescription("Upsert mode.")
  private final Mode _mode;

  @JsonPropertyDescription("Function to hash the primary key.")
  private final HashFunction _hashFunction;

  @JsonPropertyDescription("Partial update strategies.")
  private final Map<String, Strategy> _partialUpsertStrategies;

  @JsonPropertyDescription("Column for upsert comparison, default to time column")
  private final String _comparisonColumn;

  @JsonCreator
  public UpsertConfig(@JsonProperty(value = "mode", required = true) Mode mode,
      @JsonProperty("partialUpsertStrategies") @Nullable Map<String, Strategy> partialUpsertStrategies,
      @JsonProperty("comparisonColumn") @Nullable String comparisonColumn,
      @JsonProperty("hashFunction") @Nullable HashFunction hashFunction) {
    Preconditions.checkArgument(mode != null, "Upsert mode must be configured");
    _mode = mode;

    if (mode == Mode.PARTIAL) {
      _partialUpsertStrategies = partialUpsertStrategies != null ? partialUpsertStrategies : new HashMap<>();
    } else {
      _partialUpsertStrategies = null;
    }

    _comparisonColumn = comparisonColumn;
    _hashFunction = hashFunction == null ? HashFunction.NONE : hashFunction;
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

  public String getComparisonColumn() {
    return _comparisonColumn;
  }
}
