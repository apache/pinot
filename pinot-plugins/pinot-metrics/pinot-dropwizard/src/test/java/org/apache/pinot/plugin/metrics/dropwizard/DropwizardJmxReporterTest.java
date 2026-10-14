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

import com.codahale.metrics.Clock;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.spi.metrics.PinotMeter;
import org.apache.pinot.spi.metrics.PinotTimer;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Verifies the JMX contract used by existing Yammer monitoring configurations.
public class DropwizardJmxReporterTest {
  private static final MBeanServer MBEAN_SERVER = ManagementFactory.getPlatformMBeanServer();
  private static final Set<String> METER_ATTRIBUTES = Set.of("Count", "EventType", "RateUnit", "MeanRate",
      "OneMinuteRate", "FiveMinuteRate", "FifteenMinuteRate");
  private static final Set<String> TIMER_ATTRIBUTES = Set.of("Count", "EventType", "RateUnit", "MeanRate",
      "OneMinuteRate", "FiveMinuteRate", "FifteenMinuteRate", "LatencyUnit", "Min", "Max", "Mean", "StdDev",
      "50thPercentile", "75thPercentile", "95thPercentile", "98thPercentile", "99thPercentile", "999thPercentile");

  @Test
  public void testGaugeObjectNameAndAttributes()
      throws Exception {
    DropwizardMetricsRegistry registry = new DropwizardMetricsRegistry();
    String metricName = "pinot.broker.compatibilityGauge,=:\"*?\\\n";
    ObjectName objectName = objectName(BrokerMetrics.class, metricName);
    registry.newGauge(new DropwizardMetricName(BrokerMetrics.class, metricName),
        new DropwizardGauge<>(new DropwizardSettableGauge<>(7L)));
    try (DropwizardJmxReporter reporter = new DropwizardJmxReporter(registry, null)) {
      reporter.start();
      reporter.start();
      assertTrue(MBEAN_SERVER.isRegistered(objectName));
      assertEquals(attributeNames(objectName), Set.of("Value"));
      assertEquals(MBEAN_SERVER.getAttribute(objectName, "Value"), 7L);
    } finally {
      registry.shutdown();
    }
    assertFalse(MBEAN_SERVER.isRegistered(objectName));
  }

  @Test
  public void testMeterAndTimerUnits()
      throws Exception {
    DropwizardMetricsRegistry registry = new DropwizardMetricsRegistry();
    AtomicLong ticks = new AtomicLong();
    Meter rawMeter = new Meter(new Clock() {
      @Override
      public long getTick() {
        return ticks.get();
      }
    });
    String meterName = "pinot.server.compatibilityMeter";
    ((MetricRegistry) registry.getMetricsRegistry()).register(meterName, rawMeter);
    PinotMeter meter = registry.newMeter(new DropwizardMetricName(ServerMetrics.class, meterName), "requests",
        TimeUnit.MINUTES);
    meter.mark(7L);
    ticks.set(TimeUnit.SECONDS.toNanos(10));
    try (DropwizardJmxReporter reporter = new DropwizardJmxReporter(registry, null)) {
      reporter.start();
      ObjectName meterObjectName = objectName(ServerMetrics.class, meterName);
      assertEquals(attributeNames(meterObjectName), METER_ATTRIBUTES);
      assertEquals(MBEAN_SERVER.getAttribute(meterObjectName, "Count"), 7L);
      assertEquals(MBEAN_SERVER.getAttribute(meterObjectName, "EventType"), "requests");
      assertEquals(MBEAN_SERVER.getAttribute(meterObjectName, "RateUnit"), TimeUnit.MINUTES);
      assertEquals((Double) MBEAN_SERVER.getAttribute(meterObjectName, "MeanRate"), rawMeter.getMeanRate() * 60, 0.0);
      assertEquals((Double) MBEAN_SERVER.getAttribute(meterObjectName, "OneMinuteRate"),
          rawMeter.getOneMinuteRate() * 60, 0.0);
      assertEquals((Double) MBEAN_SERVER.getAttribute(meterObjectName, "FiveMinuteRate"),
          rawMeter.getFiveMinuteRate() * 60, 0.0);
      assertEquals((Double) MBEAN_SERVER.getAttribute(meterObjectName, "FifteenMinuteRate"),
          rawMeter.getFifteenMinuteRate() * 60, 0.0);

      String timerName = "pinot.server.compatibilityTimer";
      PinotTimer timer = registry.newTimer(new DropwizardMetricName(ServerMetrics.class, timerName),
          TimeUnit.MICROSECONDS, TimeUnit.HOURS);
      timer.update(123L, TimeUnit.MICROSECONDS);
      ObjectName timerObjectName = objectName(ServerMetrics.class, timerName);
      assertEquals(attributeNames(timerObjectName), TIMER_ATTRIBUTES);
      assertEquals(MBEAN_SERVER.getAttribute(timerObjectName, "Count"), 1L);
      assertEquals(MBEAN_SERVER.getAttribute(timerObjectName, "EventType"), "calls");
      assertEquals(MBEAN_SERVER.getAttribute(timerObjectName, "RateUnit"), TimeUnit.HOURS);
      assertEquals(MBEAN_SERVER.getAttribute(timerObjectName, "LatencyUnit"), TimeUnit.MICROSECONDS);
      for (String attribute : Set.of("Min", "Max", "Mean", "50thPercentile", "75thPercentile", "95thPercentile",
          "98thPercentile", "99thPercentile", "999thPercentile")) {
        assertEquals((Double) MBEAN_SERVER.getAttribute(timerObjectName, attribute), 123.0, 0.0, attribute);
      }
      assertEquals((double[]) MBEAN_SERVER.invoke(timerObjectName, "values", new Object[0], new String[0]),
          new double[]{123.0});
    } finally {
      registry.shutdown();
    }
  }

  @Test
  public void testRegistrationMetadataAndShutdown()
      throws Exception {
    DropwizardMetricsRegistry registry = new DropwizardMetricsRegistry();
    String metricName = "pinot.compatibilityRegistration";
    DropwizardMetricName brokerName = new DropwizardMetricName(BrokerMetrics.class, metricName);
    DropwizardMetricName serverName = new DropwizardMetricName(ServerMetrics.class, metricName);
    ObjectName brokerObjectName = objectName(BrokerMetrics.class, metricName);
    ObjectName serverObjectName = objectName(ServerMetrics.class, metricName);
    try (DropwizardJmxReporter reporter = new DropwizardJmxReporter(registry, null)) {
      registry.newMeter(brokerName, "requests", TimeUnit.MINUTES);
      reporter.start();
      registry.newMeter(serverName, "other", TimeUnit.HOURS);
      assertTrue(MBEAN_SERVER.isRegistered(brokerObjectName));
      assertFalse(MBEAN_SERVER.isRegistered(serverObjectName));
      assertEquals(MBEAN_SERVER.getAttribute(brokerObjectName, "EventType"), "requests");
      assertEquals(MBEAN_SERVER.getAttribute(brokerObjectName, "RateUnit"), TimeUnit.MINUTES);

      registry.removeMetric(serverName);
      assertFalse(MBEAN_SERVER.isRegistered(brokerObjectName));
      registry.newMeter(serverName, "other", TimeUnit.HOURS);
      assertTrue(MBEAN_SERVER.isRegistered(serverObjectName));
      assertEquals(MBEAN_SERVER.getAttribute(serverObjectName, "EventType"), "other");
      assertEquals(MBEAN_SERVER.getAttribute(serverObjectName, "RateUnit"), TimeUnit.HOURS);

      registry.shutdown();
      assertFalse(MBEAN_SERVER.isRegistered(serverObjectName));
      registry.newCounter(serverName);
      assertFalse(MBEAN_SERVER.isRegistered(serverObjectName));
      try (DropwizardJmxReporter restartedReporter = new DropwizardJmxReporter(registry, null)) {
        restartedReporter.start();
        assertFalse(MBEAN_SERVER.isRegistered(serverObjectName));
      }
    } finally {
      registry.shutdown();
    }
  }

  private static ObjectName objectName(Class<?> metricClass, String metricName)
      throws Exception {
    return new ObjectName(ObjectName.quote(metricClass.getPackageName()) + ":type="
        + ObjectName.quote(metricClass.getSimpleName()) + ",name=" + ObjectName.quote(metricName));
  }

  private static Set<String> attributeNames(ObjectName objectName)
      throws Exception {
    return Arrays.stream(MBEAN_SERVER.getMBeanInfo(objectName).getAttributes()).map(MBeanAttributeInfo::getName)
        .collect(Collectors.toSet());
  }
}
