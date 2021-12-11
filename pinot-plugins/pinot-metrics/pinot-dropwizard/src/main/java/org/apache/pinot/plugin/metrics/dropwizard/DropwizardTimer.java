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
package org.apache.pinot.plugin.metrics.dropwizard;

import com.codahale.metrics.Timer;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.metrics.PinotTimer;


public class DropwizardTimer implements PinotTimer {
  private final Timer _timer;

  public DropwizardTimer(Timer timer) {
    _timer = timer;
  }

  @Override
  public void update(long duration, TimeUnit unit) {
    _timer.update(duration, unit);
  }

  @Override
  public Object getTimer() {
    return _timer;
  }

  @Override
  public Object getMetered() {
    return _timer;
  }

  @Override
  public Object getMetric() {
    return _timer;
  }

  @Override
  public TimeUnit rateUnit() {
    return TimeUnit.NANOSECONDS;
  }

  @Override
  public String eventType() {
    return _timer.getClass().getName();
  }

  @Override
  public long count() {
    return _timer.getCount();
  }

  @Override
  public double fifteenMinuteRate() {
    return _timer.getFifteenMinuteRate();
  }

  @Override
  public double fiveMinuteRate() {
    return _timer.getFiveMinuteRate();
  }

  @Override
  public double meanRate() {
    return _timer.getMeanRate();
  }

  @Override
  public double oneMinuteRate() {
    return _timer.getOneMinuteRate();
  }
}
