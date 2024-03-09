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
package org.apache.pinot.common.utils;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;


public class TestClock extends Clock {

  private Instant _currentTime;
  private final ZoneId _zoneId;

  public TestClock(Instant startTime, ZoneId zone) {
    _currentTime = startTime;
    _zoneId = zone;
  }

  @Override
  public ZoneId getZone() {
    return _zoneId;
  }

  @Override
  public Clock withZone(ZoneId zone) {
    return new TestClock(_currentTime, zone);
  }

  @Override
  public Instant instant() {
    return _currentTime;
  }

  // Method to fast-forward time
  public void fastForward(Duration duration) {
    _currentTime = _currentTime.plus(duration);
  }
}
