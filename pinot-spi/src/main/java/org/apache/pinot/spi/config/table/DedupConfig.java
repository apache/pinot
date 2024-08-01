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
import org.apache.pinot.spi.config.BaseJsonConfig;

public class DedupConfig extends BaseJsonConfig {
  @JsonPropertyDescription("Whether dedup is enabled or not.")
  private final boolean _dedupEnabled;
  @JsonPropertyDescription("Function to hash the primary key.")
  private final HashFunction _hashFunction;
  @JsonPropertyDescription("Custom class for dedup metadata manager.")
  private final String _metadataManagerClass;
  @JsonPropertyDescription("When larger than 0, use it for dedup metadata cleanup, it uses the same unit as the "
      + "metadata time column. The metadata will be cleaned up when the metadata time is older than the current time "
      + "minus metadata TTL. Notice that the metadata may not be cleaned up immediately after the TTL, it depends on "
      + "the cleanup schedule.")
  private final double _metadataTTL;
  @JsonPropertyDescription("Time column used to calculate metadata TTL.")
  private final String _metadataTimeColumn;

  public DedupConfig(@JsonProperty(value = "dedupEnabled", required = true) boolean dedupEnabled,
      @JsonProperty(value = "hashFunction") HashFunction hashFunction) {
    this(dedupEnabled, hashFunction, null, 0, null);
  }

  @JsonCreator
  public DedupConfig(@JsonProperty(value = "dedupEnabled", required = true) boolean dedupEnabled,
      @JsonProperty(value = "hashFunction") HashFunction hashFunction,
      @JsonProperty(value = "metadataManagerClass") String metadataManagerClass,
      @JsonProperty(value = "metadataTTL") double metadataTTL,
      @JsonProperty(value = "metadataTimeColumn") String metadataTimeColumn) {
    _dedupEnabled = dedupEnabled;
    _hashFunction = hashFunction == null ? HashFunction.NONE : hashFunction;
    _metadataManagerClass = metadataManagerClass;
    _metadataTTL = metadataTTL;
    _metadataTimeColumn = metadataTimeColumn;
  }

  public HashFunction getHashFunction() {
    return _hashFunction;
  }

  public boolean isDedupEnabled() {
    return _dedupEnabled;
  }

  public String getMetadataManagerClass() {
    return _metadataManagerClass;
  }

  public double getMetadataTTL() {
    return _metadataTTL;
  }

  public String getMetadataTimeColumn() {
    return _metadataTimeColumn;
  }
}
