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
package org.apache.pinot.core.periodictask;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;


/**
 * Periodic tasks transitions from {@code AWAITING_START} to {@code STOP} states during its lifecycle.
 * Upon initialization, the task can be either in {@code RUNNING} or {@code IDLE} states. The task should NOT be
 * executed after the task transitions to {@code STOPPED} state.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public enum PeriodicTaskState {
  @JsonProperty("AWAITING_START") AWAITING_START,
  @JsonProperty("INITIALIZED") INIT,
  @JsonProperty("RUNNING") RUNNING,
  @JsonProperty("IDLE") IDLE,
  @JsonProperty("STOPPED") STOPPED;

  private PeriodicTaskState() {

  }
}
