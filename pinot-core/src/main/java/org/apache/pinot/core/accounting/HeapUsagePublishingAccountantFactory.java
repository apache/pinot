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
package org.apache.pinot.core.accounting;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.accounting.QueryResourceTracker;
import org.apache.pinot.spi.accounting.ThreadAccountant;
import org.apache.pinot.spi.accounting.ThreadAccountantFactory;
import org.apache.pinot.spi.accounting.ThreadResourceTracker;
import org.apache.pinot.spi.accounting.TrackingScope;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.ResourceUsageUtils;


/**
 * Accountant task that is used to publish heap usage metrics.
 */
public class HeapUsagePublishingAccountantFactory implements ThreadAccountantFactory {

  @Override
  public ThreadAccountant init(PinotConfiguration config, String instanceId, InstanceType instanceType) {
    int period = config.getProperty(CommonConstants.Accounting.CONFIG_OF_HEAP_USAGE_PUBLISHING_PERIOD_MS,
        CommonConstants.Accounting.DEFAULT_HEAP_USAGE_PUBLISH_PERIOD);
    return new HeapUsagePublishingAccountant(period);
  }

  public static class HeapUsagePublishingAccountant implements ThreadAccountant {
    private final int _period;
    private final Timer _timer;
    private final ServerMetrics _serverMetrics = ServerMetrics.get();

    public HeapUsagePublishingAccountant(int period) {
      _period = period;
      _timer = new Timer("HeapUsagePublishingAccountant", true);
    }

    public void publishHeapUsageMetrics() {
      _serverMetrics.setValueOfGlobalGauge(ServerGauge.JVM_HEAP_USED_BYTES, ResourceUsageUtils.getUsedHeapSize());
    }

    @Override
    public void setupTask(QueryThreadContext threadContext) {
    }

    @Override
    public void sampleUsage() {
    }

    @Override
    public void clear() {
    }

    @Override
    public void updateUntrackedResourceUsage(String identifier, long cpuTimeNs, long allocatedBytes,
        TrackingScope trackingScope) {
    }

    @Override
    public void startWatcherTask() {
      _timer.schedule(new TimerTask() {
        @Override
        public void run() {
          publishHeapUsageMetrics();
        }
      }, _period, _period);
    }

    @Override
    public void stopWatcherTask() {
      _timer.cancel();
    }

    @Override
    public Collection<? extends ThreadResourceTracker> getThreadResources() {
      return List.of();
    }

    @Override
    public Map<String, ? extends QueryResourceTracker> getQueryResources() {
      return Map.of();
    }
  }
}
