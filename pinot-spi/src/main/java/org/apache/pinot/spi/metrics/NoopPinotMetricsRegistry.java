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
package org.apache.pinot.spi.metrics;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;


public class NoopPinotMetricsRegistry implements PinotMetricsRegistry {
  @Override
  public void removeMetric(PinotMetricName name) {
  }

  public <T> PinotGauge<T> newGauge() {
    return new PinotGauge<T>() {
      @Override
      public T value() {
        return null;
      }

      @Override
      public Object getGauge() {
        return null;
      }

      @Override
      public Object getMetric() {
        return null;
      }

      @Override
      public void setValue(T value) {
      }

      @Override
      public void setValueSupplier(Supplier<T> valueSupplier) {
      }
    };
  }

  @Override
  public <T> PinotGauge<T> newGauge(PinotMetricName name, PinotGauge<T> gauge) {
    return newGauge();
  }

  @Override
  public PinotMeter newMeter(PinotMetricName name, String eventType, TimeUnit unit) {
    return new PinotMeter() {
      @Override
      public void mark() {
      }

      @Override
      public void mark(long unitCount) {
      }

      @Override
      public long count() {
        return 0;
      }

      @Override
      public Object getMetered() {
        return null;
      }

      @Override
      public TimeUnit rateUnit() {
        return null;
      }

      @Override
      public String eventType() {
        return "";
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

      @Override
      public Object getMetric() {
        return null;
      }
    };
  }

  @Override
  public PinotCounter newCounter(PinotMetricName name) {
    return new PinotCounter() {
      @Override
      public Object getCounter() {
        return null;
      }

      @Override
      public Object getMetric() {
        return null;
      }
    };
  }

  @Override
  public PinotTimer newTimer(PinotMetricName name, TimeUnit durationUnit, TimeUnit rateUnit) {
    return new PinotTimer() {
      @Override
      public void update(long duration, TimeUnit unit) {
      }

      @Override
      public Object getTimer() {
        return null;
      }

      @Override
      public Object getMetered() {
        return null;
      }

      @Override
      public TimeUnit rateUnit() {
        return null;
      }

      @Override
      public String eventType() {
        return "";
      }

      @Override
      public long count() {
        return 0;
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

      @Override
      public Object getMetric() {
        return null;
      }
    };
  }

  @Override
  public PinotHistogram newHistogram(PinotMetricName name, boolean biased) {
    return new PinotHistogram() {
      @Override
      public Object getHistogram() {
        return null;
      }

      @Override
      public Object getMetric() {
        return null;
      }
    };
  }

  @Override
  public Map<PinotMetricName, PinotMetric> allMetrics() {
    return Collections.emptyMap();
  }

  @Override
  public void addListener(PinotMetricsRegistryListener listener) {
  }

  @Override
  public Object getMetricsRegistry() {
    return new Object();
  }

  @Override
  public void shutdown() {
  }
}
