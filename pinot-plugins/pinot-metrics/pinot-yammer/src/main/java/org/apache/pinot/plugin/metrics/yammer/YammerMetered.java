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
package org.apache.pinot.plugin.metrics.yammer;

import com.yammer.metrics.core.Metered;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.metrics.PinotMetered;


public abstract class YammerMetered implements PinotMetered {
  private final Metered _metered;

  public YammerMetered(Metered metered) {
    _metered = metered;
  }

  @Override
  public Object getMetered() {
    return _metered;
  }

  @Override
  public TimeUnit rateUnit() {
    return _metered.rateUnit();
  }

  @Override
  public String eventType() {
    return _metered.eventType();
  }

  @Override
  public long count() {
    return _metered.count();
  }

  @Override
  public double fifteenMinuteRate() {
    return _metered.fifteenMinuteRate();
  }

  @Override
  public double fiveMinuteRate() {
    return _metered.fiveMinuteRate();
  }

  @Override
  public double meanRate() {
    return _metered.meanRate();
  }

  @Override
  public double oneMinuteRate() {
    return _metered.oneMinuteRate();
  }

  @Override
  public Object getMetric() {
    return _metered;
  }
}
