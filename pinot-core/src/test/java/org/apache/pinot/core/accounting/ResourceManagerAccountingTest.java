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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.accounting.PerQueryCPUMemAccountantFactory.PerQueryCPUMemResourceUsageAccountant;
import org.apache.pinot.core.common.datablock.DataBlockTestUtils;
import org.apache.pinot.core.data.table.IndexedTable;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.core.data.table.SimpleIndexedTable;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.query.scheduler.SchedulerGroupAccountant;
import org.apache.pinot.core.query.scheduler.resources.QueryExecutorService;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.segment.local.realtime.impl.json.MutableJsonIndexImpl;
import org.apache.pinot.segment.local.segment.creator.impl.inv.json.OffHeapJsonIndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.json.ImmutableJsonIndexReader;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.creator.JsonIndexCreator;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.accounting.ThreadExecutionContext;
import org.apache.pinot.spi.accounting.ThreadResourceUsageAccountant;
import org.apache.pinot.spi.accounting.ThreadResourceUsageProvider;
import org.apache.pinot.spi.accounting.WorkloadBudgetManager;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.config.table.JsonIndexConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.EarlyTerminationException;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.CommonConstants;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class ResourceManagerAccountingTest {

  public static final Logger LOGGER = LoggerFactory.getLogger(ResourceManagerAccountingTest.class);
  private static final int NUM_ROWS = 1_000_000;

  /**
   * Test thread cpu usage tracking in multithread environment, add @Test to run.
   * Default to unused as this is a proof of concept and will take a long time to run.
   * The last occurrence of `Finished task mem: {q%d=...}` (%d in 0, 1, ..., 29) in log should
   * have the value of around 150000000 ~ 210000000
   */
  @SuppressWarnings("unused")
  public void testCPUtimeProvider()
      throws Exception {
    LogManager.getLogger(PerQueryCPUMemResourceUsageAccountant.class).setLevel(Level.DEBUG);
    LogManager.getLogger(ThreadResourceUsageProvider.class).setLevel(Level.DEBUG);
    ThreadResourceUsageProvider.setThreadCpuTimeMeasurementEnabled(true);
    ThreadResourceUsageProvider.setThreadMemoryMeasurementEnabled(true);
    HashMap<String, Object> configs = new HashMap<>();
    ServerMetrics.register(Mockito.mock(ServerMetrics.class));
    configs.put(CommonConstants.Accounting.CONFIG_OF_ALARMING_LEVEL_HEAP_USAGE_RATIO, 0.00f);
    configs.put(CommonConstants.Accounting.CONFIG_OF_FACTORY_NAME,
        "org.apache.pinot.core.accounting.PerQueryCPUMemAccountantFactory");
    configs.put(CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_MEMORY_SAMPLING, false);
    configs.put(CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_CPU_SAMPLING, true);
    PinotConfiguration pinotCfg = new PinotConfiguration(configs);
    ThreadResourceUsageAccountant accountant = Tracing.ThreadAccountantOps.createThreadAccountant(pinotCfg,
        "testCPUtimeProvider", InstanceType.SERVER);
    Tracing.ThreadAccountantOps.startThreadAccountant();

    ResourceManager rm = getResourceManager(20, 40, 1, 1, configs, accountant);
    Future[] futures = new Future[2000];
    AtomicInteger atomicInteger = new AtomicInteger();

    for (int k = 0; k < 30; k++) {
      int finalK = k;
      rm.getQueryRunners().submit(() -> {
        String queryId = "q" + finalK;
        Tracing.ThreadAccountantOps.setupRunner(queryId, CommonConstants.Accounting.DEFAULT_WORKLOAD_NAME);
        Thread thread = Thread.currentThread();
        CountDownLatch countDownLatch = new CountDownLatch(10);
        ThreadExecutionContext threadExecutionContext = Tracing.getThreadAccountant().getThreadExecutionContext();
        for (int j = 0; j < 10; j++) {
          int finalJ = j;
          rm.getQueryWorkers().submit(() -> {
            Tracing.ThreadAccountantOps.setupWorker(finalJ, threadExecutionContext);
            for (int i = 0; i < (finalJ + 1) * 10; i++) {
              Tracing.ThreadAccountantOps.sample();
              for (int m = 0; m < 1000; m++) {
                atomicInteger.getAndAccumulate(m % 178123, Integer::sum);
              }
              try {
                Thread.sleep(200);
              } catch (InterruptedException ignored) {
              }
            }
            Tracing.ThreadAccountantOps.clear();
            countDownLatch.countDown();
          });
        }
        try {
          countDownLatch.await();
          Thread.sleep(10000);
        } catch (InterruptedException ignored) {
        }
        Tracing.ThreadAccountantOps.clear();
      });
    }
    Thread.sleep(1000000);
  }

  /**
   * Test thread memory usage tracking in multithread environment, add @Test to run.
   * Default to unused as this is a proof of concept and will take a long time to run.
   * The last occurrence of `Finished task mem: {q%d=...}` (%d in 0, 1, ..., 29) in log should
   * have the value of around 4416400 (550 * 1000 * 8 + some overhead).
   */
  @SuppressWarnings("unused")
  public void testThreadMemoryAccounting()
      throws Exception {
    LogManager.getLogger(PerQueryCPUMemResourceUsageAccountant.class).setLevel(Level.DEBUG);
    LogManager.getLogger(ThreadResourceUsageProvider.class).setLevel(Level.DEBUG);
    ThreadResourceUsageProvider.setThreadCpuTimeMeasurementEnabled(true);
    ThreadResourceUsageProvider.setThreadMemoryMeasurementEnabled(true);
    HashMap<String, Object> configs = new HashMap<>();
    ServerMetrics.register(Mockito.mock(ServerMetrics.class));
    configs.put(CommonConstants.Accounting.CONFIG_OF_ALARMING_LEVEL_HEAP_USAGE_RATIO, 0.00f);
    configs.put(CommonConstants.Accounting.CONFIG_OF_FACTORY_NAME,
        "org.apache.pinot.core.accounting.PerQueryCPUMemAccountantFactory");
    configs.put(CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_MEMORY_SAMPLING, true);
    configs.put(CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_CPU_SAMPLING, false);
    PinotConfiguration pinotCfg = new PinotConfiguration(configs);
    ThreadResourceUsageAccountant accountant = Tracing.ThreadAccountantOps.createThreadAccountant(pinotCfg,
        "testCPUtimeProvider", InstanceType.SERVER);
    Tracing.ThreadAccountantOps.startThreadAccountant();

    ResourceManager rm = getResourceManager(20, 40, 1, 1, configs, accountant);

    for (int k = 0; k < 30; k++) {
      int finalK = k;
      rm.getQueryRunners().submit(() -> {
        String queryId = "q" + finalK;
        Tracing.ThreadAccountantOps.setupRunner(queryId, CommonConstants.Accounting.DEFAULT_WORKLOAD_NAME);
        Thread thread = Thread.currentThread();
        CountDownLatch countDownLatch = new CountDownLatch(10);
        ThreadExecutionContext threadExecutionContext = Tracing.getThreadAccountant().getThreadExecutionContext();
        for (int j = 0; j < 10; j++) {
          int finalJ = j;
          rm.getQueryWorkers().submit(() -> {
            Tracing.ThreadAccountantOps.setupWorker(finalJ, threadExecutionContext);
            long[][] a = new long[1000][];
            for (int i = 0; i < (finalJ + 1) * 10; i++) {
              Tracing.ThreadAccountantOps.sample();
              a[i] = new long[1000];
              try {
                Thread.sleep(200);
              } catch (InterruptedException ignored) {
              }
            }
            Tracing.ThreadAccountantOps.clear();
            Assert.assertEquals(a[0][0], 0);
            countDownLatch.countDown();
          });
        }
        try {
          countDownLatch.await();
          Thread.sleep(10000);
        } catch (InterruptedException ignored) {
        }
        Tracing.ThreadAccountantOps.clear();
      });
    }
    Thread.sleep(1000000);
  }


  /**
   * Test thread memory usage tracking for a workload in a multithread environment, add @Test to run.
   * Default to unused as this is a proof of concept and will take a long time to run.
   * Each runner thread allocates about 800KB of memory (80KB per worker task). So setting limit of 27MB for 30 runner
   * threads.
   */
  @SuppressWarnings("unused")
  public void testWorkloadLevelThreadMemoryAccounting()
      throws Exception {
    // Logger initialization.
    LogManager.getLogger(ResourceManagerAccountingTest.class).setLevel(Level.INFO);
    LogManager.getLogger(ResourceUsageAccountantFactory.class).setLevel(Level.DEBUG);
    LogManager.getLogger(QueryAggregator.class).setLevel(Level.INFO);
    LogManager.getLogger(WorkloadAggregator.class).setLevel(Level.INFO);
    LogManager.getLogger(ThreadResourceUsageProvider.class).setLevel(Level.DEBUG);

    ThreadResourceUsageProvider.setThreadCpuTimeMeasurementEnabled(true);
    ThreadResourceUsageProvider.setThreadMemoryMeasurementEnabled(true);

    HashMap<String, Object> configs = new HashMap<>();
    configs.put(CommonConstants.Accounting.CONFIG_OF_FACTORY_NAME,
        "org.apache.pinot.core.accounting.ResourceUsageAccountantFactory");
    configs.put(CommonConstants.Accounting.CONFIG_OF_ALARMING_LEVEL_HEAP_USAGE_RATIO, 0.00f);
    configs.put(CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_MEMORY_SAMPLING, true);
    configs.put(CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_CPU_SAMPLING, false);
    configs.put(CommonConstants.Accounting.CONFIG_OF_WORKLOAD_ENABLE_COST_COLLECTION, true);
    configs.put(CommonConstants.Accounting.CONFIG_OF_WORKLOAD_ENABLE_COST_ENFORCEMENT, true);
    configs.put(CommonConstants.Accounting.CONFIG_OF_WORKLOAD_ENFORCEMENT_WINDOW_MS, 100_000_000L);
    configs.put(CommonConstants.Accounting.CONFIG_OF_SLEEP_TIME_MS, 1_000);
    configs.put(CommonConstants.Accounting.CONFIG_OF_WORKLOAD_SLEEP_TIME_MS, 1);

    String workloadName = CommonConstants.Accounting.DEFAULT_WORKLOAD_NAME;
    PinotConfiguration pinotCfg = new PinotConfiguration(configs);
    ThreadResourceUsageAccountant accountant = Tracing.ThreadAccountantOps.createThreadAccountant(pinotCfg,
        "testWorkloadMemoryAccounting", InstanceType.SERVER);
    Tracing.ThreadAccountantOps.startThreadAccountant();
    WorkloadBudgetManager workloadBudgetManager =
        Tracing.ThreadAccountantOps.getWorkloadBudgetManager();
    workloadBudgetManager.addOrUpdateWorkload(workloadName, 88_000_000, 27_000_000);
    ResourceManager rm = getResourceManager(20, 40, 1, 1, configs, accountant);

    for (int k = 0; k < 30; k++) {
      int finalK = k;
      rm.getQueryRunners().submit(() -> {
        try {
          String queryId = "q" + finalK;
          Tracing.ThreadAccountantOps.setupRunner(queryId, workloadName);
          CountDownLatch countDownLatch = new CountDownLatch(10);
          Tracing.ThreadAccountantOps.sample();
          ThreadExecutionContext threadExecutionContext = Tracing.getThreadAccountant().getThreadExecutionContext();
          LOGGER.info("RunnerThread: Queueing tasks");

          for (int j = 0; j < 10; j++) {
            int finalJ = j;
            rm.getQueryWorkers().submit(() -> {
              try {
                Tracing.ThreadAccountantOps.setupWorker(finalJ, threadExecutionContext);
                long[][] a = new long[1000][];
                for (int i = 0; i < 10; i++) {
                  a[i] = new long[1000];
                  Tracing.ThreadAccountantOps.sample();
                  Thread.sleep(100);
                }

                Tracing.ThreadAccountantOps.clear();
                Assert.assertEquals(a[0][0], 0);
                countDownLatch.countDown();
              } catch (Exception e) {
                LOGGER.error("====Worker Thread:{} task={} interrupted. Working", Thread.currentThread(), finalJ, e);
                Tracing.ThreadAccountantOps.clear();
              }
            });
          }
          LOGGER.info("RunnerThread. Queued all tasks: {}", queryId);
          Thread.sleep(10000);
          countDownLatch.await();
          Tracing.ThreadAccountantOps.clear();
        } catch (InterruptedException e) {
          LOGGER.error("====Runner Thread:{} interrupted. Working", Thread.currentThread(), e);
          Tracing.ThreadAccountantOps.clear();
        }
      });
    }
    Thread.sleep(1000_000);
  }

  /**
   * Test instrumentation during {@link DataTable} creation
   */
  @Test(dataProvider = "accountantFactories")
  public void testGetDataTableOOMSelect(String accountantFactoryClass)
      throws Exception {

    // generate random rows
    String[] columnNames = {
        "int", "long", "float", "double", "big_decimal", "string", "bytes", "int_array", "long_array", "float_array",
        "double_array", "string_array"
    };
    DataSchema.ColumnDataType[] columnDataTypes = {
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.FLOAT,
        DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.BIG_DECIMAL, DataSchema.ColumnDataType.STRING,
        DataSchema.ColumnDataType.BYTES, DataSchema.ColumnDataType.INT_ARRAY, DataSchema.ColumnDataType.LONG_ARRAY,
        DataSchema.ColumnDataType.FLOAT_ARRAY, DataSchema.ColumnDataType.DOUBLE_ARRAY,
        DataSchema.ColumnDataType.STRING_ARRAY
    };
    DataSchema dataSchema = new DataSchema(columnNames, columnDataTypes);
    List<Object[]> rows = DataBlockTestUtils.getRandomRows(dataSchema, NUM_ROWS, 0);

    // set up logging and configs
    LogManager.getLogger(PerQueryCPUMemResourceUsageAccountant.class).setLevel(Level.OFF);
    LogManager.getLogger(ThreadResourceUsageProvider.class).setLevel(Level.OFF);
    ThreadResourceUsageProvider.setThreadMemoryMeasurementEnabled(true);
    HashMap<String, Object> configs = new HashMap<>();
    ServerMetrics.register(Mockito.mock(ServerMetrics.class));
    configs.put(CommonConstants.Accounting.CONFIG_OF_ALARMING_LEVEL_HEAP_USAGE_RATIO, 0.00f);
    configs.put(CommonConstants.Accounting.CONFIG_OF_CRITICAL_LEVEL_HEAP_USAGE_RATIO, 0.00f);
    configs.put(CommonConstants.Accounting.CONFIG_OF_FACTORY_NAME, accountantFactoryClass);
    configs.put(CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_MEMORY_SAMPLING, true);
    configs.put(CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_CPU_SAMPLING, false);
    configs.put(CommonConstants.Accounting.CONFIG_OF_OOM_PROTECTION_KILLING_QUERY, true);
    PinotConfiguration config = getConfig(20, 2, configs);
    // init accountant and start watcher task
    Tracing.unregisterThreadAccountant();
    ThreadResourceUsageAccountant accountant = Tracing.ThreadAccountantOps.createThreadAccountant(config,
        "testSelect", InstanceType.SERVER);
    Tracing.ThreadAccountantOps.startThreadAccountant();
    ResourceManager rm = getResourceManager(20, 2, 1, 1, configs, accountant);

    CountDownLatch latch = new CountDownLatch(100);
    AtomicBoolean earlyTerminationOccurred = new AtomicBoolean(false);

    for (int i = 0; i < 100; i++) {
      int finalI = i;
      rm.getQueryRunners().submit(() -> {
        Tracing.ThreadAccountantOps.setupRunner("testSelectQueryId" + finalI,
            CommonConstants.Accounting.DEFAULT_WORKLOAD_NAME);
        try {
          SelectionOperatorUtils.getDataTableFromRows(rows, dataSchema, false).toBytes();
        } catch (EarlyTerminationException e) {
          earlyTerminationOccurred.set(true);
          Tracing.ThreadAccountantOps.clear();
        } catch (IOException e) {
          Assert.fail(e.getMessage());
        } finally {
          latch.countDown();
        }
      });
    }
    latch.await(1, java.util.concurrent.TimeUnit.MINUTES);
    // assert that EarlyTerminationException was thrown in at least one runner thread
    Assert.assertTrue(earlyTerminationOccurred.get());
  }

  /**
   * Test instrumentation during {@link DataTable} creation
   */
  @Test (dataProvider = "accountantFactories")
  public void testGetDataTableOOMGroupBy(String accountantFactoryClass)
      throws Exception {

    // generate random indexedTable
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT SUM(m1), MAX(m2) FROM testTable GROUP BY d1, d2, d3, d4");
    DataSchema dataSchema =
        new DataSchema(new String[]{"d1", "d2", "d3", "d4", "sum(m1)", "max(m2)"}, new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.DOUBLE,
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE
        });
    List<Object[]> rows = DataBlockTestUtils.getRandomRows(dataSchema, NUM_ROWS, 0);
    IndexedTable indexedTable =
        new SimpleIndexedTable(dataSchema, false, queryContext, NUM_ROWS, Integer.MAX_VALUE, Integer.MAX_VALUE,
            CommonConstants.Server.DEFAULT_QUERY_EXECUTOR_MIN_INITIAL_INDEXED_TABLE_CAPACITY,
            Executors.newCachedThreadPool());
    for (Object[] row : rows) {
      indexedTable.upsert(new Record(row));
    }
    indexedTable.finish(false);
    // set up GroupByResultsBlock
    GroupByResultsBlock groupByResultsBlock = new GroupByResultsBlock(indexedTable, queryContext);

    // set up logging and configs
    LogManager.getLogger(PerQueryCPUMemResourceUsageAccountant.class).setLevel(Level.OFF);
    LogManager.getLogger(ThreadResourceUsageProvider.class).setLevel(Level.OFF);
    ThreadResourceUsageProvider.setThreadMemoryMeasurementEnabled(true);
    HashMap<String, Object> configs = new HashMap<>();
    ServerMetrics.register(Mockito.mock(ServerMetrics.class));
    configs.put(CommonConstants.Accounting.CONFIG_OF_ALARMING_LEVEL_HEAP_USAGE_RATIO, 0.00f);
    configs.put(CommonConstants.Accounting.CONFIG_OF_CRITICAL_LEVEL_HEAP_USAGE_RATIO, 0.00f);
    configs.put(CommonConstants.Accounting.CONFIG_OF_FACTORY_NAME, accountantFactoryClass);
    configs.put(CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_MEMORY_SAMPLING, true);
    configs.put(CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_CPU_SAMPLING, false);
    configs.put(CommonConstants.Accounting.CONFIG_OF_OOM_PROTECTION_KILLING_QUERY, true);
    PinotConfiguration config = getConfig(20, 2, configs);

    // init accountant and start watcher task
    Tracing.unregisterThreadAccountant();
    ThreadResourceUsageAccountant accountant = Tracing.ThreadAccountantOps.createThreadAccountant(config,
        "testGroupBy", InstanceType.SERVER);
    Tracing.ThreadAccountantOps.startThreadAccountant();

    ResourceManager rm = getResourceManager(20, 2, 1, 1, configs, accountant);

    CountDownLatch latch = new CountDownLatch(100);
    AtomicBoolean earlyTerminationOccurred = new AtomicBoolean(false);

    for (int i = 0; i < 100; i++) {
      int finalI = i;
      rm.getQueryRunners().submit(() -> {
        Tracing.ThreadAccountantOps.setupRunner("testGroupByQueryId" + finalI,
            CommonConstants.Accounting.DEFAULT_WORKLOAD_NAME);
        try {
          groupByResultsBlock.getDataTable().toBytes();
        } catch (EarlyTerminationException e) {
          earlyTerminationOccurred.set(true);
          Tracing.ThreadAccountantOps.clear();
        } catch (IOException e) {
          Assert.fail(e.getMessage());
        } finally {
          latch.countDown();
        }
      });
    }
    latch.await(1, java.util.concurrent.TimeUnit.MINUTES);
    // assert that EarlyTerminationException was thrown in at least one runner thread
    Assert.assertTrue(earlyTerminationOccurred.get());
  }

  /**
   * Test instrumentation in getMatchingFlattenedDocsMap() from
   * {@link org.apache.pinot.segment.spi.index.reader.JsonIndexReader}
   *
   * Since getMatchingFlattenedDocsMap() can collect a large map before processing any blocks, it is required to
   * check for OOM during map generation. This test generates a mutable and immutable json index, and generates a map
   * as would happen in json_extract_index execution.
   *
   * It is roughly equivalent to running json_extract_index(col, '$.key', 'STRING').
   */
  @Test(dataProvider = "accountantFactories")
  public void testJsonIndexExtractMapOOM(String accountantFactoryClass)
      throws Exception {
    HashMap<String, Object> configs = new HashMap<>();
    ServerMetrics.register(Mockito.mock(ServerMetrics.class));
    ThreadResourceUsageProvider.setThreadMemoryMeasurementEnabled(true);
    LogManager.getLogger(PerQueryCPUMemResourceUsageAccountant.class).setLevel(Level.OFF);
    LogManager.getLogger(ThreadResourceUsageProvider.class).setLevel(Level.OFF);
    configs.put(CommonConstants.Accounting.CONFIG_OF_ALARMING_LEVEL_HEAP_USAGE_RATIO, 0.00f);
    configs.put(CommonConstants.Accounting.CONFIG_OF_CRITICAL_LEVEL_HEAP_USAGE_RATIO, 0.00f);
    configs.put(CommonConstants.Accounting.CONFIG_OF_FACTORY_NAME, accountantFactoryClass);
    configs.put(CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_MEMORY_SAMPLING, true);
    configs.put(CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_CPU_SAMPLING, false);
    configs.put(CommonConstants.Accounting.CONFIG_OF_OOM_PROTECTION_KILLING_QUERY, true);
    configs.put(CommonConstants.Accounting.CONFIG_OF_MIN_MEMORY_FOOTPRINT_TO_KILL_RATIO, 0.00f);

    PinotConfiguration config = getConfig(2, 2, configs);
    // init accountant and start watcher task
    Tracing.unregisterThreadAccountant();
    ThreadResourceUsageAccountant accountant = Tracing.ThreadAccountantOps.createThreadAccountant(config,
        "testJsonIndexExtractMapOOM", InstanceType.SERVER);
    Tracing.ThreadAccountantOps.startThreadAccountant();

    ResourceManager rm = getResourceManager(2, 2, 1, 1, configs, accountant);

    Supplier<String> randomJsonValue = () -> {
      Random random = new Random();
      int length = random.nextInt(1000);
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < length; i++) {
        sb.append((char) (random.nextInt(26) + 'a'));
      }
      return "{\"key\":\"" + sb + "\"}";
    };

    File indexDir = new File(FileUtils.getTempDirectory(), "testJsonIndexExtractMapOOM");
    FileUtils.forceMkdir(indexDir);
    String colName = "col";
    try (JsonIndexCreator offHeapIndexCreator = new OffHeapJsonIndexCreator(indexDir, colName, "myTable_OFFLINE",
        false, new JsonIndexConfig());
        MutableJsonIndexImpl mutableJsonIndex = new MutableJsonIndexImpl(new JsonIndexConfig(), "table__0__1", "col")) {
      // build json indexes
      for (int i = 0; i < 1000000; i++) {
        String val = randomJsonValue.get();
        offHeapIndexCreator.add(val);
        mutableJsonIndex.add(val);
      }
      offHeapIndexCreator.seal();

      CountDownLatch latch = new CountDownLatch(2);
      AtomicBoolean mutableEarlyTerminationOccurred = new AtomicBoolean(false);

      // test mutable json index .getMatchingFlattenedDocsMap()
      rm.getQueryRunners().submit(() -> {
        Tracing.ThreadAccountantOps.setupRunner("testJsonExtractIndexId1",
            CommonConstants.Accounting.DEFAULT_WORKLOAD_NAME);
        try {
          mutableJsonIndex.getMatchingFlattenedDocsMap("key", null);
        } catch (EarlyTerminationException e) {
          mutableEarlyTerminationOccurred.set(true);
          Tracing.ThreadAccountantOps.clear();
        } finally {
          latch.countDown();
        }
      });

      // test immutable json index .getMatchingFlattenedDocsMap()
      File indexFile = new File(indexDir, colName + V1Constants.Indexes.JSON_INDEX_FILE_EXTENSION);
      AtomicBoolean immutableEarlyTerminationOccurred = new AtomicBoolean(false);
      rm.getQueryRunners().submit(() -> {
        Tracing.ThreadAccountantOps.setupRunner("testJsonExtractIndexId2",
            CommonConstants.Accounting.DEFAULT_WORKLOAD_NAME);
        try {
          try (PinotDataBuffer offHeapDataBuffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
              ImmutableJsonIndexReader offHeapIndexReader = new ImmutableJsonIndexReader(offHeapDataBuffer, 1000000)) {
            offHeapIndexReader.getMatchingFlattenedDocsMap("key", null);
          } catch (IOException e) {
            Assert.fail("failed .getMatchingFlattenedDocsMap for the immutable json index");
          }
        } catch (EarlyTerminationException e) {
          immutableEarlyTerminationOccurred.set(true);
          Tracing.ThreadAccountantOps.clear();
        } finally {
          latch.countDown();
        }
      });

      latch.await(1, java.util.concurrent.TimeUnit.MINUTES);
      Assert.assertTrue(mutableEarlyTerminationOccurred.get(),
          "Expected early termination reading the mutable index");
      Assert.assertTrue(immutableEarlyTerminationOccurred.get(),
          "Expected early termination reading the immutable index");
    }
  }

  /**
   * Test thread memory usage tracking and query killing in multi-thread environment， add @Test to run.
   */
  @SuppressWarnings("unused")
  public void testThreadMemory()
      throws Exception {
    LogManager.getLogger(PerQueryCPUMemResourceUsageAccountant.class).setLevel(Level.DEBUG);
    LogManager.getLogger(ThreadResourceUsageProvider.class).setLevel(Level.DEBUG);
    ThreadResourceUsageProvider.setThreadCpuTimeMeasurementEnabled(true);
    ThreadResourceUsageProvider.setThreadMemoryMeasurementEnabled(true);
    HashMap<String, Object> configs = new HashMap<>();
    ServerMetrics.register(Mockito.mock(ServerMetrics.class));
    configs.put(CommonConstants.Accounting.CONFIG_OF_ALARMING_LEVEL_HEAP_USAGE_RATIO, 0.00f);
    configs.put(CommonConstants.Accounting.CONFIG_OF_CRITICAL_LEVEL_HEAP_USAGE_RATIO, 0.9f);
    configs.put(CommonConstants.Accounting.CONFIG_OF_OOM_PROTECTION_KILLING_QUERY, true);
    configs.put(CommonConstants.Accounting.CONFIG_OF_FACTORY_NAME,
        "org.apache.pinot.core.accounting.PerQueryCPUMemAccountantFactory");
    configs.put(CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_MEMORY_SAMPLING, true);
    configs.put(CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_CPU_SAMPLING, false);
    ResourceManager rm = getResourceManager(20, 40, 1, 1, configs, new Tracing.DefaultThreadResourceUsageAccountant());
    Future[] futures = new Future[30];

    for (int k = 0; k < 4; k++) {
      int finalK = k;
      futures[finalK] = rm.getQueryRunners().submit(() -> {
        String queryId = "q" + finalK;
        Tracing.ThreadAccountantOps.setupRunner(queryId, CommonConstants.Accounting.DEFAULT_WORKLOAD_NAME);
        Thread thread = Thread.currentThread();
        CountDownLatch countDownLatch = new CountDownLatch(10);
        Future[] futuresThread = new Future[10];
        ThreadExecutionContext threadExecutionContext = Tracing.getThreadAccountant().getThreadExecutionContext();
        for (int j = 0; j < 10; j++) {
          int finalJ = j;
          futuresThread[j] = rm.getQueryWorkers().submit(() -> {
            Tracing.ThreadAccountantOps.setupWorker(finalJ, threadExecutionContext);
            long[][] a = new long[1000][];
            for (int i = 0; i < (finalK + 1) * 80; i++) {
              Tracing.ThreadAccountantOps.sample();
              if (Thread.interrupted() || thread.isInterrupted()) {
                Tracing.ThreadAccountantOps.clear();
                LOGGER.error("KilledWorker {} {}", queryId, finalJ);
                return;
              }
              a[i] = new long[200000];
              for (int m = 0; m < 10000; m++) {
                a[i][m] = m % 178123;
              }
            }
            Tracing.ThreadAccountantOps.clear();
            Assert.assertEquals(a[0][0], 0);
            countDownLatch.countDown();
          });
        }
        try {
          countDownLatch.await();
        } catch (InterruptedException e) {
          for (int i = 0; i < 10; i++) {
            futuresThread[i].cancel(true);
          }
          LOGGER.error("Killed {}", queryId);
        }
        Tracing.ThreadAccountantOps.clear();
      });
    }
    Thread.sleep(1000000);
  }

  private ResourceManager getResourceManager(int runners, int workers, final int softLimit, final int hardLimit,
      Map<String, Object> map, ThreadResourceUsageAccountant accountant) {

    return new ResourceManager(getConfig(runners, workers, map), accountant) {

      @Override
      public QueryExecutorService getExecutorService(ServerQueryRequest query, SchedulerGroupAccountant accountant) {
        return new QueryExecutorService() {
          @Override
          public void execute(Runnable command) {
            getQueryWorkers().execute(command);
          }
        };
      }

      @Override
      public int getTableThreadsHardLimit() {
        return hardLimit;
      }

      @Override
      public int getTableThreadsSoftLimit() {
        return softLimit;
      }
    };
  }

  private PinotConfiguration getConfig(int runners, int workers, Map<String, Object> map) {
    Map<String, Object> properties = new HashMap<>(map);
    properties.put(ResourceManager.QUERY_RUNNER_CONFIG_KEY, runners);
    properties.put(ResourceManager.QUERY_WORKER_CONFIG_KEY, workers);
    return new PinotConfiguration(properties);
  }

  @DataProvider(name = "accountantFactories")
  public Object[][] accountantFactories() {
    return new Object[][] {
        {"org.apache.pinot.core.accounting.PerQueryCPUMemAccountantFactory"},
        {"org.apache.pinot.core.accounting.ResourceUsageAccountantFactory"}
    };
  }
}
