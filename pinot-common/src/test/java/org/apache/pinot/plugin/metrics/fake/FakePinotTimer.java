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
package org.apache.pinot.plugin.metrics.fake;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pinot.spi.metrics.PinotTimer;


public class FakePinotTimer implements PinotTimer {
  private final AtomicLong _count = new AtomicLong();
  // Accumulated duration expressed in _durationUnit.
  private final AtomicLong _sumDurationMs = new AtomicLong();
  private final TimeUnit _rateUnit;

  public FakePinotTimer(TimeUnit durationUnit, TimeUnit rateUnit) {
    _rateUnit = rateUnit;
  }

  @Override
  public void update(long duration, TimeUnit unit) {
    _count.incrementAndGet();
    _sumDurationMs.addAndGet(unit.toMillis(duration));
  }

  @Override
  public Object getTimer() {
    return this;
  }

  @Override
  public Object getMetered() {
    return this;
  }

  @Override
  public Object getMetric() {
    return this;
  }

  @Override
  public TimeUnit rateUnit() {
    return _rateUnit;
  }

  @Override
  public String eventType() {
    return "calls";
  }

  @Override
  public long count() {
    return _count.get();
  }

  /** Total elapsed time recorded in milliseconds. */
  public long sumMs() {
    return _sumDurationMs.get();
  }

  @Override
  public double fifteenMinuteRate() {
    return 0;
  }

  @Override
  public double fiveMinuteRate() {
    return 0;
  }

  @Override
  public double meanRate() {
    return 0;
  }

  @Override
  public double oneMinuteRate() {
    return 0;
  }
}
