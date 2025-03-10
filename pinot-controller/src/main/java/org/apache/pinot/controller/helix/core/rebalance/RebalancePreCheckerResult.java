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
package org.apache.pinot.controller.helix.core.rebalance;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import javax.annotation.Nullable;


/**
 * Holds the pre-check result for each pre-check performed as part of RebalancePreChecker
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RebalancePreCheckerResult {

  @JsonInclude(JsonInclude.Include.NON_NULL)
  private final PreCheckStatus _preCheckStatus;
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private final String _message;

  /**
   * Constructor for RebalancePreCheckerResult
   * @param preCheckStatus server related summary information
   * @param message segment related summary information
   */
  @JsonCreator
  public RebalancePreCheckerResult(@JsonProperty("preCheckStatus") PreCheckStatus preCheckStatus,
      @JsonProperty("message") @Nullable String message) {
    _preCheckStatus = preCheckStatus;
    _message = message;
  }

  @JsonProperty
  public PreCheckStatus getPreCheckStatus() {
    return _preCheckStatus;
  }

  @JsonProperty
  public String getMessage() {
    return _message;
  }

  public enum PreCheckStatus {
    // PASS if the pre-check status is considered safe for rebalance;
    // WARNING if the pre-check status is a warning and should be double-checked for rebalance;
    // ERROR if the pre-check status has failed and should be addressed prior to rebalance;
    PASS, WARNING, ERROR
  }
}
