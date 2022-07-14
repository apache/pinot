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
package org.apache.pinot.common.restlet.resources;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import java.util.Map;
import java.util.Set;


@JsonIgnoreProperties(ignoreUnknown = true)
public class TableTierInfo {
  @JsonPropertyDescription("Name of table to look for segment storage tiers")
  private final String _tableName;

  @JsonPropertyDescription("Storage tiers of segments hosted on the server")
  private final Map<String, String> _segmentTiers;

  @JsonPropertyDescription("Segments that's not immutable and has no storage tier")
  private final Set<String> _mutableSegments;

  @JsonCreator
  public TableTierInfo(@JsonProperty("tableName") String tableName,
      @JsonProperty("segmentTiers") Map<String, String> segmentTiers,
      @JsonProperty("mutableSegments") Set<String> mutableSegments) {
    _tableName = tableName;
    _segmentTiers = segmentTiers;
    _mutableSegments = mutableSegments;
  }

  public String getTableName() {
    return _tableName;
  }

  public Map<String, String> getSegmentTiers() {
    return _segmentTiers;
  }

  public Set<String> getMutableSegments() {
    return _mutableSegments;
  }
}
