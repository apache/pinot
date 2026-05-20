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
package org.apache.pinot.controller.api.resources;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.util.List;
import java.util.Map;

/// Field order is fixed via [JsonPropertyOrder] so the wire shape is deterministic across Jackson versions.
/// `unrecognizedProperties` comes first to match the pre-deprecationWarnings response layout that existing
/// clients and integration tests assert byte-exactly.
@JsonPropertyOrder({"unrecognizedProperties", "deprecationWarnings", "status"})
public final class ConfigSuccessResponse extends SuccessResponse {
  private final Map<String, Object> _unrecognizedProperties;
  private final List<String> _deprecationWarnings;

  public ConfigSuccessResponse(String status, Map<String, Object> unrecognizedProperties) {
    this(status, unrecognizedProperties, List.of());
  }

  public ConfigSuccessResponse(String status, Map<String, Object> unrecognizedProperties,
      List<String> deprecationWarnings) {
    super(status);
    _unrecognizedProperties = unrecognizedProperties == null ? Map.of() : unrecognizedProperties;
    _deprecationWarnings = deprecationWarnings == null ? List.of() : deprecationWarnings;
  }

  public Map<String, Object> getUnrecognizedProperties() {
    return _unrecognizedProperties;
  }

  /// `@JsonInclude(NON_EMPTY)` is on the getter so the empty-list case is elided from the response. Older clients
  /// that strict-parse this DTO continue to see the original (pre-deprecationWarnings) shape when no warnings fire.
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<String> getDeprecationWarnings() {
    return _deprecationWarnings;
  }
}
