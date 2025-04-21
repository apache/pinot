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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Set;
import org.apache.pinot.spi.config.table.PauseState;


@JsonInclude(JsonInclude.Include.NON_NULL)
public class PauseStatusDetails {
  private boolean _pauseFlag;
  private Set<String> _consumingSegments;
  private PauseState.ReasonCode _reasonCode;
  private String _comment;
  private String _timestamp;

  @JsonCreator
  public PauseStatusDetails(@JsonProperty("pauseFlag") boolean pauseFlag,
      @JsonProperty("consumingSegments") Set<String> consumingSegments,
      @JsonProperty("reasonCode") PauseState.ReasonCode reasonCode,
      @JsonProperty("comment") String comment,
      @JsonProperty("timestamp") String timestamp) {
    _pauseFlag = pauseFlag;
    _consumingSegments = consumingSegments;
    _reasonCode = reasonCode;
    _comment = comment != null ? comment : pauseFlag ? "Table is paused."
        : "Table is unpaused.";
    _timestamp = timestamp;
  }

  public boolean getPauseFlag() {
    return _pauseFlag;
  }

  public Set<String> getConsumingSegments() {
    return _consumingSegments;
  }

  public PauseState.ReasonCode getReasonCode() {
    return _reasonCode;
  }

  public String getComment() {
    return _comment;
  }

  public String getTimestamp() {
    return _timestamp;
  }

  @Override
  public String toString() {
    return "PauseStatusDetails{" + "_pauseFlag=" + _pauseFlag + ", _consumingSegments="
        + _consumingSegments + ", _reasonCode=" + _reasonCode + ", _comment='" + _comment
        + '\'' + ", _timestamp='" + _timestamp + '\'' + '}';
  }
}
