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
import org.apache.pinot.spi.metrics.PinotMeter;


public class FakePinotMeter implements PinotMeter {
  private final AtomicLong _count = new AtomicLong();
  private final String _eventType;
  private final TimeUnit _rateUnit;

  public FakePinotMeter(String eventType, TimeUnit rateUnit) {
    _eventType = eventType;
    _rateUnit = rateUnit;
  }

  @Override
  public void mark() {
    _count.incrementAndGet();
  }

  @Override
  public void mark(long unitCount) {
    _count.addAndGet(unitCount);
  }

  @Override
  public long count() {
    return _count.get();
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
    return _eventType;
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
