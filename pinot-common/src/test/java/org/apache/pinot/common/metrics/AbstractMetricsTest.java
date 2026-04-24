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
package org.apache.pinot.common.metrics;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.IntConsumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMeter;
import org.apache.pinot.spi.metrics.PinotMetricName;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.CONFIG_OF_METRICS_FACTORY_CLASS_NAME;


/**
 * Base test for {@link AbstractMetrics} behavior. Concrete subclasses supply the {@link PinotMetricsRegistry}
 * implementation, a matching {@link MetricsInspector}, and a gauge-value reader — allowing the same test suite to run
 * against the in-memory fake registry (in pinot-common) and against real yammer/dropwizard registries (in their
 * respective plugin modules).
 *
 * <p>Subclasses must:
 * <ul>
 *   <li>Implement {@link #metricsFactoryClassName()} with the fully-qualified class name of a
 *       {@link org.apache.pinot.spi.annotations.metrics.PinotMetricsFactory}.</li>
 *   <li>Implement {@link #buildRegistry()} to return a fresh registry per test invocation.</li>
 *   <li>Implement {@link #createInspector(PinotMetricsRegistry)} with a {@link MetricsInspector} that wires against
 *       the registry returned above.</li>
 *   <li>Implement {@link #getGaugeValue(AbstractMetrics, String)} — reading supplier-registered gauges requires a
 *       registry-level read, not {@code AbstractMetrics.getGaugeValue()} (which only covers gauges that populated
 *       {@code _gaugeValues}).</li>
 * </ul>
 */
public abstract class AbstractMetricsTest {

  protected abstract String metricsFactoryClassName();

  protected abstract PinotMetricsRegistry buildRegistry();

  protected abstract MetricsInspector createInspector(PinotMetricsRegistry registry);

  protected abstract long getGaugeValue(AbstractMetrics<?, ?, ?, ?> metrics, String metricName);

  protected ControllerMetrics buildTestMetrics() {
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(CONFIG_OF_METRICS_FACTORY_CLASS_NAME, metricsFactoryClassName());
    PinotMetricUtils.init(config);
    return new ControllerMetrics(buildRegistry());
  }

  /**
   * Tear down the PinotMetricUtils static factory after each subclass finishes. The factory installs a
   * {@link org.apache.pinot.spi.metrics.PinotJmxReporter}; leaving it registered can cause cross-test JMX collisions
   * (e.g. duplicate MBean names) when multiple subclasses run in the same JVM.
   */
  @AfterClass
  public void cleanUpMetricsFactory() {
    PinotMetricUtils.cleanUp();
  }

  @Test
  public void testAddOrUpdateGauge() {
    ControllerMetrics controllerMetrics = buildTestMetrics();
    String metricName = "test";

    controllerMetrics.setOrUpdateGauge(metricName, () -> 1L);
    Assert.assertEquals(getGaugeValue(controllerMetrics, metricName), 1);

    controllerMetrics.setOrUpdateGauge(metricName, () -> 2L);
    Assert.assertEquals(getGaugeValue(controllerMetrics, metricName), 2);

    controllerMetrics.removeGauge(metricName);
    Assert.assertTrue(controllerMetrics.getMetricsRegistry().allMetrics().isEmpty());
  }

  @Test
  public void testMultipleUpdatesToGauge() {
    ControllerMetrics controllerMetrics = buildTestMetrics();
    String metricName = "testMultipleUpdates";

    IntStream.range(0, 1000).forEach(i -> controllerMetrics.setOrUpdateGauge(metricName, () -> (long) i));

    Assert.assertEquals(getGaugeValue(controllerMetrics, metricName), 999);
    controllerMetrics.removeGauge(metricName);
    Assert.assertTrue(controllerMetrics.getMetricsRegistry().allMetrics().isEmpty());
  }

  @Test
  public void testRemoveNonExistentGauge() {
    ControllerMetrics controllerMetrics = buildTestMetrics();
    controllerMetrics.removeGauge("testNonExistent");
    Assert.assertTrue(controllerMetrics.getMetricsRegistry().allMetrics().isEmpty());
  }

  @Test
  public void testMultipleGauges() {
    ControllerMetrics controllerMetrics = buildTestMetrics();

    controllerMetrics.setOrUpdateGauge("testMultiple1", () -> 1L);
    controllerMetrics.setOrUpdateGauge("testMultiple2", () -> 2L);

    Assert.assertEquals(getGaugeValue(controllerMetrics, "testMultiple1"), 1);
    Assert.assertEquals(getGaugeValue(controllerMetrics, "testMultiple2"), 2);

    controllerMetrics.removeGauge("testMultiple1");
    controllerMetrics.removeGauge("testMultiple2");
    Assert.assertTrue(controllerMetrics.getMetricsRegistry().allMetrics().isEmpty());
  }

  @Test
  public void testQueryPhases() {
    ControllerMetrics testMetrics = buildTestMetrics();
    MetricsInspector inspector = createInspector(testMetrics.getMetricsRegistry());

    AbstractMetrics.QueryPhase testPhase = () -> "testPhase";
    Assert.assertEquals(testPhase.getDescription(), "");
    Assert.assertEquals(testPhase.getQueryPhaseName(), "testPhase");
    String testTableName = "tbl_testQueryPhases";
    String testTableName2 = "tbl2_testQueryPhases";

    testMetrics.addPhaseTiming(testTableName, testPhase, 1, TimeUnit.SECONDS);
    PinotMetricName tbl1Metric = inspector.lastMetric();
    Assert.assertEquals(inspector.getTimerSumMs(tbl1Metric), 1000);

    testMetrics.addPhaseTiming(testTableName, testPhase, 444000000 /* nanoseconds */);
    Assert.assertEquals(inspector.getTimerSumMs(tbl1Metric), 1444);

    testMetrics.addPhaseTiming(testTableName2, testPhase, 22, TimeUnit.MILLISECONDS);
    PinotMetricName tbl2Metric = inspector.lastMetric();
    Assert.assertEquals(inspector.getTimerSumMs(tbl2Metric), 22);
    Assert.assertEquals(inspector.getTimerSumMs(tbl1Metric), 1444);

    testMetrics.removePhaseTiming(testTableName, testPhase);
    testMetrics.removePhaseTiming(testTableName2, testPhase);
    Assert.assertTrue(testMetrics.getMetricsRegistry().allMetrics().isEmpty());
  }

  @Test
  public void testTimerMetrics() {
    ControllerMetrics testMetrics = buildTestMetrics();
    MetricsInspector inspector = createInspector(testMetrics.getMetricsRegistry());
    String tableName = "tbl_testTimerMetrics";
    String keyName = "keyName";
    ControllerTimer timer = ControllerTimer.IDEAL_STATE_UPDATE_TIME_MS;

    testMetrics.addTimedTableValue(tableName, timer, 6, TimeUnit.SECONDS);
    PinotMetricName t1Metric = inspector.lastMetric();
    Assert.assertEquals(inspector.getTimerSumMs(t1Metric), 6000);
    testMetrics.addTimedTableValue(tableName, keyName, timer, 500, TimeUnit.MILLISECONDS);
    PinotMetricName t2Metric = inspector.lastMetric();
    Assert.assertEquals(inspector.getTimerSumMs(t2Metric), 500);

    testMetrics.addTimedValue(timer, 40, TimeUnit.MILLISECONDS);
    PinotMetricName t3Metric = inspector.lastMetric();
    Assert.assertEquals(inspector.getTimerSumMs(t3Metric), 40);
    testMetrics.addTimedValue(keyName, timer, 3, TimeUnit.MILLISECONDS);
    PinotMetricName t4Metric = inspector.lastMetric();
    Assert.assertEquals(inspector.getTimerSumMs(t4Metric), 3);

    Assert.assertEquals(testMetrics.getMetricsRegistry().allMetrics().size(), 4);
    testMetrics.getMetricsRegistry().allMetrics().keySet().forEach(testMetrics.getMetricsRegistry()::removeMetric);
    Assert.assertTrue(testMetrics.getMetricsRegistry().allMetrics().isEmpty());
  }

  @Test
  public void testMeteredMetrics() {
    ControllerMetrics testMetrics = buildTestMetrics();
    MetricsInspector inspector = createInspector(testMetrics.getMetricsRegistry());
    String tableName = "tbl_testMeteredMetrics";
    String keyName = "keyName";
    ControllerMeter meter = ControllerMeter.CONTROLLER_INSTANCE_POST_ERROR;
    ControllerMeter meter2 = ControllerMeter.CONTROLLER_PERIODIC_TASK_ERROR;

    PinotMetricName[] currentMetric = new PinotMetricName[1];
    Runnable expectNewMetric = () -> {
      Assert.assertNotEquals(inspector.lastMetric(), currentMetric[0]);
      currentMetric[0] = inspector.lastMetric();
    };
    IntConsumer expectMeteredCount = expected -> {
      Assert.assertEquals(inspector.getMeteredCount(currentMetric[0]), expected);
      Assert.assertEquals(currentMetric[0], inspector.lastMetric());
    };

    testMetrics.addMeteredGlobalValue(meter, 5);
    expectNewMetric.run();
    expectMeteredCount.accept(5);
    testMetrics.addMeteredGlobalValue(meter, 4, testMetrics.getMeteredValue(meter));
    expectMeteredCount.accept(9);

    testMetrics.addMeteredValue(keyName, meter, 9);
    expectNewMetric.run();
    expectMeteredCount.accept(9);
    PinotMeter reusedMeter = testMetrics.addMeteredValue(keyName, meter2, 13, null);
    expectNewMetric.run();
    expectMeteredCount.accept(13);
    testMetrics.addMeteredValue(keyName, meter2, 6, reusedMeter);
    expectMeteredCount.accept(19);

    testMetrics.addMeteredTableValue(tableName, meter, 15);
    expectNewMetric.run();
    expectMeteredCount.accept(15);
    testMetrics.addMeteredTableValue(tableName, meter2, 3, testMetrics.getMeteredTableValue(tableName, meter));
    expectMeteredCount.accept(18);

    testMetrics.addMeteredTableValue(tableName, keyName, meter, 21);
    expectNewMetric.run();
    expectMeteredCount.accept(21);
    reusedMeter = testMetrics.addMeteredTableValue(tableName, keyName, meter2, 23, null);
    expectNewMetric.run();
    expectMeteredCount.accept(23);
    testMetrics.addMeteredTableValue(tableName, keyName, meter2, 5, reusedMeter);
    expectMeteredCount.accept(28);

    Assert.assertEquals(testMetrics.getMetricsRegistry().allMetrics().size(), 6);
    testMetrics.removeTableMeter(tableName, meter);
    Assert.assertEquals(testMetrics.getMetricsRegistry().allMetrics().size(), 5);
    testMetrics.getMetricsRegistry().allMetrics().keySet().forEach(testMetrics.getMetricsRegistry()::removeMetric);
    Assert.assertTrue(testMetrics.getMetricsRegistry().allMetrics().isEmpty());
  }

  private void testAsyncAddRemove(Runnable addAction, Runnable removeAction)
      throws ExecutionException, InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    long runtimeMs = 10;
    long endTime = System.currentTimeMillis() + runtimeMs;

    ExecutorService executorService = Executors.newFixedThreadPool(2);
    Future<?> addFuture = executorService.submit(() -> {
      while (System.currentTimeMillis() < endTime + runtimeMs) {
        addAction.run();
      }
      try {
        latch.await();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      addAction.run();
    });
    Future<?> removeFuture = executorService.submit(() -> {
      while (System.currentTimeMillis() < endTime) {
        removeAction.run();
      }
      latch.countDown();
    });

    addFuture.get();
    removeFuture.get();
    executorService.shutdown();
    Assert.assertTrue(executorService.awaitTermination(1, TimeUnit.SECONDS));
  }

  @Test
  public void testGlobalGaugeMetricsAsyncAddRemove() throws ExecutionException, InterruptedException {
    ControllerMetrics controllerMetrics = buildTestMetrics();
    testAsyncAddRemove(
        () -> controllerMetrics.addValueToGlobalGauge(ControllerGauge.VERSION, 1L),
        () -> controllerMetrics.removeGauge(ControllerGauge.VERSION.getGaugeName()));

    Assert.assertFalse(controllerMetrics.getMetricsRegistry().allMetrics().isEmpty());
    Long gaugeValue = controllerMetrics.getGaugeValue(ControllerGauge.VERSION.getGaugeName());
    Assert.assertNotNull(gaugeValue);
    Assert.assertTrue(gaugeValue > 0);
  }

  @Test
  public void testTableGaugeMetricsAsyncAddRemove() throws ExecutionException, InterruptedException {
    ControllerMetrics controllerMetrics = buildTestMetrics();
    testAsyncAddRemove(
        () -> controllerMetrics.addValueToTableGauge("test_table", ControllerGauge.VERSION, 1L),
        () -> controllerMetrics.removeTableGauge("test_table", ControllerGauge.VERSION));

    Assert.assertFalse(controllerMetrics.getMetricsRegistry().allMetrics().isEmpty());
    Long gaugeValue = controllerMetrics.getGaugeValue(ControllerGauge.VERSION.getGaugeName() + ".test_table");
    Assert.assertNotNull(gaugeValue);
    Assert.assertTrue(gaugeValue > 0);
  }

  @Test
  public void testSetValueOfGaugeAsyncAddRemove() throws ExecutionException, InterruptedException {
    ControllerMetrics controllerMetrics = buildTestMetrics();
    testAsyncAddRemove(
        () -> controllerMetrics.setValueOfGauge(1L, ControllerGauge.VERSION.getGaugeName()),
        () -> controllerMetrics.removeGauge(ControllerGauge.VERSION.getGaugeName()));

    Assert.assertFalse(controllerMetrics.getMetricsRegistry().allMetrics().isEmpty());
    Long gaugeValue = controllerMetrics.getGaugeValue(ControllerGauge.VERSION.getGaugeName());
    Assert.assertNotNull(gaugeValue);
    Assert.assertTrue(gaugeValue > 0);
  }

  @Test
  public void testInitializeGlobalMeters() {
    ControllerMetrics controllerMetrics = buildTestMetrics();

    controllerMetrics.initializeGlobalMeters();
    Assert.assertFalse(controllerMetrics.getMetricsRegistry().allMetrics().isEmpty());

    for (ControllerMeter meter : controllerMetrics.getMeters()) {
      if (meter.isGlobal()) {
        Assert.assertEquals(0, controllerMetrics.getMeteredValue(meter).count());
      }
    }
    for (ControllerGauge gauge : controllerMetrics.getGauges()) {
      if (gauge.isGlobal()) {
        Assert.assertEquals(0, (long) controllerMetrics.getGaugeValue(gauge.getGaugeName()));
      }
    }
  }

  @Test
  public void testSetOrUpdateGlobalGauges() {
    ControllerMetrics controllerMetrics = buildTestMetrics();

    controllerMetrics.setOrUpdateGlobalGauge(ControllerGauge.VERSION, () -> 1L);
    Assert.assertEquals(getGaugeValue(controllerMetrics, ControllerGauge.VERSION.getGaugeName()), 1);

    controllerMetrics.setOrUpdateGlobalGauge(ControllerGauge.VERSION, (Supplier<Long>) () -> 2L);
    Assert.assertEquals(getGaugeValue(controllerMetrics, ControllerGauge.VERSION.getGaugeName()), 2);

    controllerMetrics.setValueOfGlobalGauge(ControllerGauge.OFFLINE_TABLE_COUNT, "suffix", 3L);
    Assert.assertEquals(
        getGaugeValue(controllerMetrics, ControllerGauge.OFFLINE_TABLE_COUNT.getGaugeName() + ".suffix"), 3);

    controllerMetrics.setValueOfGlobalGauge(ControllerGauge.OFFLINE_TABLE_COUNT, 4L);
    Assert.assertEquals(getGaugeValue(controllerMetrics, ControllerGauge.OFFLINE_TABLE_COUNT.getGaugeName()), 4);

    controllerMetrics.removeGauge(ControllerGauge.VERSION.getGaugeName());
    controllerMetrics.removeGauge(ControllerGauge.OFFLINE_TABLE_COUNT.getGaugeName());
    controllerMetrics.removeGlobalGauge("suffix", ControllerGauge.OFFLINE_TABLE_COUNT);
    Assert.assertTrue(controllerMetrics.getMetricsRegistry().allMetrics().isEmpty());
  }

  @Test
  public void testSetOrUpdateTableGauges() {
    ControllerMetrics controllerMetrics = buildTestMetrics();
    String table = "test_table";
    String key = "key";

    controllerMetrics.setOrUpdateTableGauge(table, key, ControllerGauge.VERSION, () -> 1L);
    Assert.assertEquals(
        getGaugeValue(controllerMetrics, ControllerGauge.VERSION.getGaugeName() + "." + table + "." + key), 1);

    controllerMetrics.setOrUpdateTableGauge(table, key, ControllerGauge.VERSION, 2L);
    Assert.assertEquals(
        getGaugeValue(controllerMetrics, ControllerGauge.VERSION.getGaugeName() + "." + table + "." + key), 2);

    controllerMetrics.setOrUpdateTableGauge(table, ControllerGauge.OFFLINE_TABLE_COUNT, 3L);
    Assert.assertEquals(
        getGaugeValue(controllerMetrics, ControllerGauge.OFFLINE_TABLE_COUNT.getGaugeName() + "." + table), 3);

    controllerMetrics.setOrUpdateTableGauge(table, ControllerGauge.OFFLINE_TABLE_COUNT, () -> 4L);
    Assert.assertEquals(
        getGaugeValue(controllerMetrics, ControllerGauge.OFFLINE_TABLE_COUNT.getGaugeName() + "." + table), 4);

    controllerMetrics.setValueOfTableGauge(table, ControllerGauge.OFFLINE_TABLE_COUNT, 5L);
    Assert.assertEquals(
        getGaugeValue(controllerMetrics, ControllerGauge.OFFLINE_TABLE_COUNT.getGaugeName() + "." + table), 5);

    controllerMetrics.removeTableGauge(table, key, ControllerGauge.VERSION);
    controllerMetrics.removeTableGauge(table, ControllerGauge.OFFLINE_TABLE_COUNT);
    Assert.assertTrue(controllerMetrics.getMetricsRegistry().allMetrics().isEmpty());
  }

  @Test
  public void testPartitionGauges() {
    ControllerMetrics controllerMetrics = buildTestMetrics();
    String table = "test_table";
    int partitionId = 1024;

    controllerMetrics.setValueOfPartitionGauge(table, partitionId, ControllerGauge.VERSION, 1L);
    Assert.assertEquals(
        getGaugeValue(controllerMetrics,
            ControllerGauge.VERSION.getGaugeName() + "." + table + "." + partitionId), 1);

    controllerMetrics.setOrUpdatePartitionGauge(table, partitionId, ControllerGauge.VERSION, () -> 2L);
    Assert.assertEquals(
        getGaugeValue(controllerMetrics,
            ControllerGauge.VERSION.getGaugeName() + "." + table + "." + partitionId), 2);

    controllerMetrics.removePartitionGauge(table, partitionId, ControllerGauge.VERSION);
    Assert.assertTrue(controllerMetrics.getMetricsRegistry().allMetrics().isEmpty());
  }

  @Test
  public void testAddCallbackGauges() {
    ControllerMetrics controllerMetrics = buildTestMetrics();
    String table = "test_table";

    controllerMetrics.addCallbackTableGaugeIfNeeded(table, ControllerGauge.VERSION, () -> 10L);
    Assert.assertEquals(
        getGaugeValue(controllerMetrics, ControllerGauge.VERSION.getGaugeName() + "." + table), 10);
  }
}
