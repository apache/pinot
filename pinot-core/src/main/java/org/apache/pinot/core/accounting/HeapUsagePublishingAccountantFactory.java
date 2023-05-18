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

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.Timer;
import java.util.TimerTask;
import org.apache.pinot.common.metrics.ServerGauge;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.accounting.ThreadAccountantFactory;
import org.apache.pinot.spi.accounting.ThreadResourceUsageAccountant;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * Accountant task that is used to publish heap usage metrics in addition to
 * the functionality of DefaultThreadAccountant
 */
public class HeapUsagePublishingAccountantFactory implements ThreadAccountantFactory {

  @Override
  public ThreadResourceUsageAccountant init(PinotConfiguration config, String instanceId) {
    int period = config.getProperty(CommonConstants.Accounting.CONFIG_OF_HEAP_USAGE_PUBLISHING_PERIOD_MS,
        CommonConstants.Accounting.DEFAULT_HEAP_USAGE_PUBLISH_PERIOD);
    return new HeapUsagePublishingResourceUsageAccountant(period);
  }

  public static class HeapUsagePublishingResourceUsageAccountant extends Tracing.DefaultThreadResourceUsageAccountant {
    static final MemoryMXBean MEMORY_MX_BEAN = ManagementFactory.getMemoryMXBean();
    private final Timer _timer;
    private final int _period;

    public HeapUsagePublishingResourceUsageAccountant(int period) {
      _period = period;
      _timer = new Timer("HeapUsagePublishingAccountant", true);
    }

    public void publishHeapUsageMetrics() {
      ServerMetrics.get()
          .setValueOfGlobalGauge(ServerGauge.JVM_HEAP_USED_BYTES, MEMORY_MX_BEAN.getHeapMemoryUsage().getUsed());
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
  }
}
