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
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;


/**
 * Config for the tiered storage and the segments which will move to that tier
 */
public class TierConfig extends BaseJsonConfig {
  @JsonPropertyDescription("Name of the tier with format TIER<number>")
  private final String _name;

  @JsonPropertyDescription("The strategy for selecting segments")
  private final String _segmentSelectorType;

  @JsonPropertyDescription("For 'TIME' segment selector, the period after which to select segments for this tier")
  private final String _segmentAge;

  @JsonPropertyDescription("For 'FIXED' segment selector, the list of segments to select for this tier")
  private final List<String> _segmentList;

  @JsonPropertyDescription("The type of storage")
  private final String _storageType;

  @JsonPropertyDescription("The tag with which to identify servers for this tier.")
  private final String _serverTag;

  @JsonPropertyDescription("The backend FS to use for this tier. Default is 'local'.")
  private final String _tierBackend;

  @JsonPropertyDescription("Properties for the tier backend")
  private final Map<String, String> _tierBackendProperties;

  // TODO: only "serverTag" is supported currently. In next iteration, "InstanceAssignmentConfig
  //  _instanceAssignmentConfig" will be added
  //  here

  public TierConfig(@JsonProperty(value = "name", required = true) String name,
      @JsonProperty(value = "segmentSelectorType", required = true) String segmentSelectorType,
      @JsonProperty("segmentAge") @Nullable String segmentAge,
      @JsonProperty("segmentList") @Nullable List<String> segmentList,
      @JsonProperty(value = "storageType", required = true) String storageType,
      @JsonProperty("serverTag") @Nullable String serverTag,
      @JsonProperty("tierBackend") @Nullable String tierBackend,
      @JsonProperty("tierBackendProperties") @Nullable Map<String, String> tierBackendProperties) {
    Preconditions.checkArgument(name != null, "Must provide non-null 'name' in tierConfig");
    Preconditions
        .checkArgument(segmentSelectorType != null, "Must provide non-null 'segmentSelectorType' in tierConfig");
    Preconditions.checkArgument(storageType != null, "Must provide non-null 'storageType' in tierConfig");
    _name = name;
    _segmentSelectorType = segmentSelectorType;
    _segmentAge = segmentAge;
    _segmentList = segmentList;
    _storageType = storageType;
    _serverTag = serverTag;
    _tierBackend = tierBackend;
    _tierBackendProperties = tierBackendProperties;
  }

  public String getName() {
    return _name;
  }

  public String getSegmentSelectorType() {
    return _segmentSelectorType;
  }

  @Nullable
  public String getSegmentAge() {
    return _segmentAge;
  }

  @Nullable
  public List<String> getSegmentList() {
    return _segmentList;
  }

  public String getStorageType() {
    return _storageType;
  }

  public String getServerTag() {
    return _serverTag;
  }

  @Nullable
  public String getTierBackend() {
    return _tierBackend;
  }

  @Nullable
  public Map<String, String> getTierBackendProperties() {
    return _tierBackendProperties;
  }
}
