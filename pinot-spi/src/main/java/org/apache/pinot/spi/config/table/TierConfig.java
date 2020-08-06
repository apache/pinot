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

  @JsonPropertyDescription("For 'timeBased' segment selector, the period after which to select segments for this tier")
  private final String _segmentAge;

  @JsonPropertyDescription("The type of storage storage")
  private final String _storageType;

  @JsonPropertyDescription("For 'pinotServer' storageSelector, the tag with which to identify servers for this tier.")
  private final String _serverTag;

  // TODO: only "serverTag" is supported currently. In next iteration, "InstanceAssignmentConfig _instanceAssignmentConfig" will be added here

  public TierConfig(@JsonProperty(value = "name", required = true) String name,
      @JsonProperty(value = "segmentSelectorType", required = true) String segmentSelectorType,
      @JsonProperty("segmentAge") @Nullable String segmentAge,
      @JsonProperty(value = "storageType", required = true) String storageType,
      @JsonProperty("serverTag") @Nullable String serverTag) {
    Preconditions.checkArgument(name != null, "Must provide non-null 'name' in tierConfig");
    Preconditions
        .checkArgument(segmentSelectorType != null, "Must provide non-null 'segmentSelectorType' in tierConfig");
    Preconditions.checkArgument(storageType != null, "Must provide non-null 'storageType' in tierConfig");
    _name = name;
    _segmentSelectorType = segmentSelectorType;
    _segmentAge = segmentAge;
    _storageType = storageType;
    _serverTag = serverTag;
  }

  public String getName() {
    return _name;
  }

  public String getSegmentSelectorType() {
    return _segmentSelectorType;
  }

  public String getSegmentAge() {
    return _segmentAge;
  }

  public String getStorageType() {
    return _storageType;
  }

  public String getServerTag() {
    return _serverTag;
  }

}
