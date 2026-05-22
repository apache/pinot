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
package org.apache.pinot.controller.helix.core.minion;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.helix.task.TaskState;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.controller.helix.core.minion.generator.PinotTaskGenerator;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Integration tests for the concurrent task scheduling dispatch in
 * {@link PinotTaskManager#scheduleTasks(TaskSchedulingContext)}. Boots a single controller,
 * registers a custom task generator whose {@code generateTasks} method rendezvouses N callers
 * (via a {@link CountDownLatch}) before returning, and fires two parallel {@code scheduleTasks}
 * calls for two different tables:
 * <ul>
 *   <li>When both tables opt into the concurrent path via
 *       {@link TableTaskConfig#getConcurrentSchedulingEnabled()} = {@code true}, the generator
 *       observes {@code maxInFlight == 2} — both threads entered generation simultaneously,
 *       proving the controller-wide {@code synchronized(this)} was not held.</li>
 *   <li>With the cluster default ({@code false}) and no per-table override, the generator
 *       observes {@code maxInFlight == 1} — the legacy path serialized the two calls on the
 *       controller monitor.</li>
 * </ul>
 * The rendezvous has a bounded timeout so the legacy-path test still completes; it does not
 * deadlock — the first thread times out waiting for the second, exits, then the second thread
 * runs. The observation is purely on {@code maxInFlight}, not wall-clock time.
 */
public class PinotTaskManagerConcurrentSchedulingIntegrationTest extends ControllerTest {

  private static final String RAW_TABLE_NAME_A = "concurrentTableA";
  private static final String RAW_TABLE_NAME_B = "concurrentTableB";
  private static final String TEST_TASK_TYPE = "TestConcurrentSchedulingTaskType";

  // Bounded so the legacy-path test still completes (the first thread times out alone, then
  // releases the monitor so the second thread can proceed). Sized generously so the concurrent
  // path is not flaky under CI load — pre-rendezvous work (ZK round-trips, lock attempts) can
  // take several seconds on a loaded host.
  private static final long RENDEZVOUS_TIMEOUT_MS = 10_000L;
  private static final long SCHEDULE_COMPLETE_TIMEOUT_SECONDS = 60L;

  @BeforeMethod
  public void setUpMethod()
      throws Exception {
    startZk();
  }

  @AfterMethod
  public void tearDownMethod() {
    try {
      if (_controllerStarter != null) {
        try {
          Map<String, TaskState> taskStates =
              _controllerStarter.getHelixTaskResourceManager().getTaskStates(TEST_TASK_TYPE);
          for (String taskName : taskStates.keySet()) {
            try {
              _controllerStarter.getHelixTaskResourceManager().deleteTask(taskName, true);
            } catch (Exception ignored) {
            }
          }
          Thread.sleep(300);
        } catch (Exception ignored) {
        }
        stopController();
      }
    } catch (Exception ignored) {
    }
    try {
      stopFakeInstances();
    } catch (Exception ignored) {
    }
    stopZk();
  }

  /**
   * When both target tables explicitly opt into concurrent scheduling, two parallel
   * {@code scheduleTasks} calls must be able to run {@code generateTasks} concurrently
   * (no controller-wide monitor held).
   */
  @Test
  public void testConcurrentPathEnabledAllowsParallelGeneration()
      throws Exception {
    startController(controllerProperties());
    addFakeInstances();

    RendezvousTaskGenerator generator = registerRendezvousGenerator(2);

    createTable(RAW_TABLE_NAME_A, Boolean.TRUE);
    createTable(RAW_TABLE_NAME_B, Boolean.TRUE);

    runTwoParallelScheduleTasks();

    assertEquals(generator.getMaxInFlight(), 2,
        "Both scheduleTasks threads should enter generateTasks concurrently when the table "
            + "opts into concurrent scheduling");
    assertEquals(generator.getGenerationCount(), 2,
        "Both scheduleTasks calls should have produced exactly one generation each");

    dropTables();
  }

  /**
   * With the cluster default ({@code false}) and no per-table override, two parallel
   * {@code scheduleTasks} calls must serialize on the controller monitor.
   */
  @Test
  public void testDefaultPathSerializesGeneration()
      throws Exception {
    startController(controllerProperties());
    addFakeInstances();

    RendezvousTaskGenerator generator = registerRendezvousGenerator(2);

    // `null` = inherit cluster default, which stays at `false`.
    createTable(RAW_TABLE_NAME_A, null);
    createTable(RAW_TABLE_NAME_B, null);

    runTwoParallelScheduleTasks();

    assertEquals(generator.getMaxInFlight(), 1,
        "scheduleTasks threads should serialize on the controller monitor when the "
            + "concurrent path is disabled (cluster default)");
    assertEquals(generator.getGenerationCount(), 2,
        "Both scheduleTasks calls should still have produced exactly one generation each, just serially");

    dropTables();
  }

  private Map<String, Object> controllerProperties() {
    Map<String, Object> props = getDefaultControllerConfiguration();
    // Disable the Quartz cron scheduler so only the test threads fire scheduleTasks. Leave
    // `controller.task.concurrentSchedulingEnabled` at its default (false) and rely on the
    // per-table TableTaskConfig flag to opt in or not.
    props.put(ControllerConf.ControllerPeriodicTasksConf.PINOT_TASK_MANAGER_SCHEDULER_ENABLED, false);
    return props;
  }

  private void addFakeInstances()
      throws Exception {
    addFakeBrokerInstancesToAutoJoinHelixCluster(1, true);
    addFakeServerInstancesToAutoJoinHelixCluster(1, true);
    addFakeMinionInstancesToAutoJoinHelixCluster(1);
  }

  private RendezvousTaskGenerator registerRendezvousGenerator(int expectedParties)
      throws Exception {
    RendezvousTaskGenerator generator =
        new RendezvousTaskGenerator(TEST_TASK_TYPE, expectedParties, RENDEZVOUS_TIMEOUT_MS);
    generator.init(Mockito.mock(ClusterInfoAccessor.class));
    _controllerStarter.getTaskManager().registerTaskGenerator(generator);
    _controllerStarter.getHelixTaskResourceManager().ensureTaskQueueExists(TEST_TASK_TYPE);
    return generator;
  }

  private void createTable(String rawTableName, Boolean concurrentFlag)
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(rawTableName)
        .addSingleValueDimension("testCol", FieldSpec.DataType.STRING).build();
    addSchema(schema);
    TableTaskConfig taskConfig = new TableTaskConfig(
        Map.of(TEST_TASK_TYPE, Map.of("schedule", "0 */10 * ? * * *")), concurrentFlag);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(rawTableName)
        .setTaskConfig(taskConfig)
        .build();
    addTableConfig(tableConfig);
  }

  private void dropTables() {
    try {
      dropOfflineTable(RAW_TABLE_NAME_A);
    } catch (Exception ignored) {
    }
    try {
      dropOfflineTable(RAW_TABLE_NAME_B);
    } catch (Exception ignored) {
    }
  }

  private void runTwoParallelScheduleTasks()
      throws Exception {
    PinotTaskManager taskManager = _controllerStarter.getTaskManager();
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(2);
    List<Throwable> workerFailures = new CopyOnWriteArrayList<>();

    for (String rawTableName : List.of(RAW_TABLE_NAME_A, RAW_TABLE_NAME_B)) {
      String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(rawTableName);
      executor.submit(() -> {
        try {
          startLatch.await();
          TaskSchedulingContext context = new TaskSchedulingContext()
              .setTablesToSchedule(Set.of(tableNameWithType))
              .setLeader(true);
          taskManager.scheduleTasks(context);
        } catch (Throwable t) {
          workerFailures.add(t);
        } finally {
          doneLatch.countDown();
        }
      });
    }

    startLatch.countDown();
    assertTrue(doneLatch.await(SCHEDULE_COMPLETE_TIMEOUT_SECONDS, TimeUnit.SECONDS),
        "Both scheduleTasks calls should complete within " + SCHEDULE_COMPLETE_TIMEOUT_SECONDS + "s");
    executor.shutdownNow();
    assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "Executor should terminate");
    assertTrue(workerFailures.isEmpty(),
        "scheduleTasks workers should not throw; observed failures: " + workerFailures);
  }

  /**
   * Task generator that rendezvouses N callers inside {@code generateTasks} via a
   * {@link CountDownLatch} countdown, then records the maximum number of callers observed
   * concurrently. The rendezvous has a bounded timeout so a caller that enters alone (e.g.,
   * because the controller serialized callers) proceeds after the timeout instead of blocking
   * forever.
   */
  private static class RendezvousTaskGenerator implements PinotTaskGenerator {
    private final String _taskType;
    private final long _rendezvousTimeoutMs;
    private final CountDownLatch _entered;
    private final AtomicInteger _inFlight = new AtomicInteger(0);
    private final AtomicInteger _maxInFlight = new AtomicInteger(0);
    private final AtomicInteger _generationCount = new AtomicInteger(0);

    RendezvousTaskGenerator(String taskType, int expectedParties, long rendezvousTimeoutMs) {
      _taskType = taskType;
      _rendezvousTimeoutMs = rendezvousTimeoutMs;
      _entered = new CountDownLatch(expectedParties);
    }

    int getMaxInFlight() {
      return _maxInFlight.get();
    }

    int getGenerationCount() {
      return _generationCount.get();
    }

    @Override
    public String getTaskType() {
      return _taskType;
    }

    @Override
    public void init(ClusterInfoAccessor clusterInfoAccessor) {
    }

    private void observeAndRendezvous() {
      int current = _inFlight.incrementAndGet();
      _maxInFlight.updateAndGet(prev -> Math.max(prev, current));
      _entered.countDown();
      try {
        _entered.await(_rendezvousTimeoutMs, TimeUnit.MILLISECONDS);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      } finally {
        _inFlight.decrementAndGet();
      }
    }

    private Map<String, String> mutableConfig(String tableName) {
      Map<String, String> cfg = new HashMap<>();
      cfg.put("tableName", tableName);
      return cfg;
    }

    @Override
    public List<PinotTaskConfig> generateTasks(List<TableConfig> tableConfigs) {
      observeAndRendezvous();
      _generationCount.incrementAndGet();
      List<PinotTaskConfig> tasks = new ArrayList<>();
      for (TableConfig cfg : tableConfigs) {
        tasks.add(new PinotTaskConfig(_taskType, mutableConfig(cfg.getTableName())));
      }
      return tasks;
    }

    @Override
    public List<PinotTaskConfig> generateTasks(TableConfig tableConfig, Map<String, String> taskConfigs) {
      observeAndRendezvous();
      _generationCount.incrementAndGet();
      return Collections.singletonList(
          new PinotTaskConfig(_taskType, mutableConfig(tableConfig.getTableName())));
    }

    @Override
    public void generateTasks(List<TableConfig> tableConfigs, List<PinotTaskConfig> pinotTaskConfigs) {
      observeAndRendezvous();
      _generationCount.incrementAndGet();
      pinotTaskConfigs.add(
          new PinotTaskConfig(_taskType, mutableConfig(tableConfigs.get(0).getTableName())));
    }
  }
}
