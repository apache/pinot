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
package org.apache.pinot.integration.tests.realtime.utils;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;


public class FailureInjectingTableConfig {
  private final boolean _failCommit;
  private final boolean _failConsumingSegment;
  private final int _maxFailures;

  @JsonCreator
  public FailureInjectingTableConfig(@JsonProperty("failCommit") boolean failCommit,
      @JsonProperty("failConsumingSegment") boolean failConsumingSegment,
      @JsonProperty("maxFailures") int maxFailures) {
    _failCommit = failCommit;
    _failConsumingSegment = failConsumingSegment;
    _maxFailures = maxFailures;
  }

  public boolean isFailConsumingSegment() {
    return _failConsumingSegment;
  }

  public boolean isFailCommit() {
    return _failCommit;
  }

  public int getMaxFailures() {
    return _maxFailures;
  }

  public String toJson() {
    try {
      return new ObjectMapper().writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String toString() {
    return "FailureInjectingTableConfig{"
        + "_failCommit=" + _failCommit
        + ", _failConsumingSegment=" + _failConsumingSegment
        + ", _maxFailures=" + _maxFailures
        + '}';
  }
}
