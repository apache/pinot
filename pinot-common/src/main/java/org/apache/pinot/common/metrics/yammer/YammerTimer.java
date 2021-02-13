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
package org.apache.pinot.common.metrics.yammer;

import com.yammer.metrics.core.Timer;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metrics.base.PinotMetricName;
import org.apache.pinot.common.metrics.base.PinotMetricProcessor;
import org.apache.pinot.common.metrics.base.PinotTimer;


public class YammerTimer implements PinotTimer {
  private Timer _timer;

  public YammerTimer(Timer timer) {
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
  public TimeUnit rateUnit() {
    return _timer.rateUnit();
  }

  @Override
  public String eventType() {
    return _timer.eventType();
  }

  @Override
  public long count() {
    return _timer.count();
  }

  @Override
  public double fifteenMinuteRate() {
    return _timer.fifteenMinuteRate();
  }

  @Override
  public double fiveMinuteRate() {
    return _timer.fiveMinuteRate();
  }

  @Override
  public double meanRate() {
    return _timer.meanRate();
  }

  @Override
  public double oneMinuteRate() {
    return _timer.oneMinuteRate();
  }

  @Override
  public <T> void processWith(PinotMetricProcessor<T> processor, PinotMetricName name, T context)
      throws Exception {
    processor.processTimer(name, this, context);
  }
}
