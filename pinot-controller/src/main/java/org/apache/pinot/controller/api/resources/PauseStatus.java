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
import org.apache.pinot.spi.config.table.TablePauseStatus;


@JsonInclude(JsonInclude.Include.NON_NULL)
public class PauseStatus {
  private boolean _pauseFlag;
  private int _consumingSegmentsCount;
  private TablePauseStatus.ReasonCode _reasonCode;
  private String _comment;
  private long _timeInMillis;

  @JsonCreator
  public PauseStatus(@JsonProperty("pauseFlag") boolean pauseFlag,
      @JsonProperty("consumingSegmentsCount") int consumingSegmentsCount,
      @JsonProperty("reasonCode") TablePauseStatus.ReasonCode reasonCode,
      @JsonProperty("comment") String comment,
      @JsonProperty("timeInMillis") long timeInMillis) {
    _pauseFlag = pauseFlag;
    _consumingSegmentsCount = consumingSegmentsCount;
    _reasonCode = reasonCode;
    _comment = comment != null ? comment : pauseFlag ? "Table is paused." : "Table is unpaused.";
    _timeInMillis = timeInMillis;
  }

  public boolean getPauseFlag() {
    return _pauseFlag;
  }

  public int getConsumingSegmentsCount() {
    return _consumingSegmentsCount;
  }

  public TablePauseStatus.ReasonCode getReasonCode() {
    return _reasonCode;
  }

  public String getComment() {
    return _comment;
  }

  public long getTimeInMillis() {
    return _timeInMillis;
  }
}
