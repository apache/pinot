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

import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.google.common.base.Preconditions;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;
import org.apache.pinot.spi.utils.Enablement;


public class DedupConfig extends BaseJsonConfig {
  // TODO: Consider removing this field. It should always be true when DedupConfig is present.
  @JsonPropertyDescription("Whether dedup is enabled or not.")
  private boolean _dedupEnabled = true;

  @JsonPropertyDescription("Function to hash the primary key.")
  private HashFunction _hashFunction = HashFunction.NONE;

  @JsonPropertyDescription("When larger than 0, use it for dedup metadata cleanup, it uses the same unit as the "
      + "dedup time column. The metadata will be cleaned up when the dedup time is older than the current time "
      + "minus metadata TTL. Notice that the metadata may not be cleaned up immediately after the TTL, it depends on "
      + "the cleanup schedule.")
  private double _metadataTTL;

  @JsonPropertyDescription("Time column used to calculate dedup metadata TTL. When it is not specified, the time column"
      + " from the table config will be used.")
  @Nullable
  private String _dedupTimeColumn;

  @JsonPropertyDescription("Whether to preload segments for fast dedup metadata recovery. Available values are "
      + "ENABLE, DISABLE and DEFAULT (use instance level default behavior).")
  private Enablement _preload = Enablement.DEFAULT;

  @JsonPropertyDescription("Custom class for dedup metadata manager. If not specified, the default implementation "
      + "ConcurrentMapTableDedupMetadataManager will be used.")
  @Nullable
  private String _metadataManagerClass;

  @JsonPropertyDescription("Custom configs for dedup metadata manager.")
  @Nullable
  private Map<String, String> _metadataManagerConfigs;

  /// @deprecated use {@link #_preload} instead. This is kept here for backward compatibility.
  @Deprecated
  @JsonPropertyDescription("Whether to preload segments for fast dedup metadata recovery.")
  private boolean _enablePreload;

  /// @deprecated use {@link org.apache.pinot.spi.config.table.ingestion.ParallelSegmentConsumptionPolicy)} instead.
  @Deprecated
  @JsonPropertyDescription("Whether to pause dedup table's partition consumption during commit")
  private boolean _allowDedupConsumptionDuringCommit;

  public DedupConfig() {
  }

  @Deprecated
  public DedupConfig(boolean dedupEnabled, @Nullable HashFunction hashFunction) {
    _dedupEnabled = dedupEnabled;
    _hashFunction = hashFunction != null ? hashFunction : HashFunction.NONE;
  }

  @Deprecated
  public DedupConfig(boolean dedupEnabled, @Nullable HashFunction hashFunction, @Nullable String metadataManagerClass,
      @Nullable Map<String, String> metadataManagerConfigs, double metadataTTL, @Nullable String dedupTimeColumn,
      @Nullable boolean enablePreload) {
    _dedupEnabled = dedupEnabled;
    _hashFunction = hashFunction != null ? hashFunction : HashFunction.NONE;
    _metadataManagerClass = metadataManagerClass;
    _metadataManagerConfigs = metadataManagerConfigs;
    _metadataTTL = metadataTTL;
    _dedupTimeColumn = dedupTimeColumn;
    _preload = enablePreload ? Enablement.ENABLE : Enablement.DEFAULT;
  }

  public boolean isDedupEnabled() {
    return _dedupEnabled;
  }

  public void setDedupEnabled(boolean dedupEnabled) {
    _dedupEnabled = dedupEnabled;
  }

  public HashFunction getHashFunction() {
    return _hashFunction;
  }

  public void setHashFunction(HashFunction hashFunction) {
    Preconditions.checkArgument(hashFunction != null, "Hash function cannot be null");
    _hashFunction = hashFunction;
  }

  public double getMetadataTTL() {
    return _metadataTTL;
  }

  public void setMetadataTTL(double metadataTTL) {
    _metadataTTL = metadataTTL;
  }

  @Nullable
  public String getDedupTimeColumn() {
    return _dedupTimeColumn;
  }

  public void setDedupTimeColumn(@Nullable String dedupTimeColumn) {
    _dedupTimeColumn = dedupTimeColumn;
  }

  public Enablement getPreload() {
    return _preload;
  }

  public void setPreload(Enablement preload) {
    Preconditions.checkArgument(preload != null, "Preload cannot be null, must be one of ENABLE, DISABLE or DEFAULT");
    _preload = preload;
  }

  @Nullable
  public String getMetadataManagerClass() {
    return _metadataManagerClass;
  }

  public void setMetadataManagerClass(@Nullable String metadataManagerClass) {
    _metadataManagerClass = metadataManagerClass;
  }

  @Nullable
  public Map<String, String> getMetadataManagerConfigs() {
    return _metadataManagerConfigs;
  }

  public void setMetadataManagerConfigs(@Nullable Map<String, String> metadataManagerConfigs) {
    _metadataManagerConfigs = metadataManagerConfigs;
  }

  @Deprecated
  public boolean isEnablePreload() {
    return _enablePreload;
  }

  @Deprecated
  public void setEnablePreload(boolean enablePreload) {
    _enablePreload = enablePreload;
    if (enablePreload) {
      _preload = Enablement.ENABLE;
    }
  }

  @Deprecated
  public boolean isAllowDedupConsumptionDuringCommit() {
    return _allowDedupConsumptionDuringCommit;
  }

  @Deprecated
  public void setAllowDedupConsumptionDuringCommit(boolean allowDedupConsumptionDuringCommit) {
    _allowDedupConsumptionDuringCommit = allowDedupConsumptionDuringCommit;
  }
}
