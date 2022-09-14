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
package org.apache.pinot.query;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.NamedThreadFactory;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import org.apache.pinot.query.runtime.QueryRunner;
import org.apache.pinot.query.runtime.plan.DistributedStagePlan;
import org.apache.pinot.query.service.QueryConfig;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Query server enclosure for testing Pinot query planner & runtime.
 *
 * This enclosure simulates a deployable component of Pinot that serves
 *   - regular Pinot query server (that serves segment-based queries)
 *   - intermediate stage queries (such as JOIN operator that awaits data scanned from left/right tables)
 *
 * Inside this construct it runs a regular pinot QueryExecutor as well as the new runtime - WorkerExecutor
 * Depending on the query request type it gets routed to either one of the two for execution.
 *
 * It also runs a GRPC Mailbox service that runs the new transport layer protocol as the backbone for all
 * multi-stage query communication.
 */
public class QueryServerEnclosure {
  private static final int NUM_ROWS = 5;
  private static final int DEFAULT_EXECUTOR_THREAD_NUM = 5;
  private static final String[] STRING_FIELD_LIST = new String[]{"foo", "bar", "alice", "bob", "charlie"};
  private static final int[] INT_FIELD_LIST = new int[]{1, 42};

  private final ExecutorService _testExecutor;
  private final int _queryRunnerPort;
  private final Map<String, Object> _runnerConfig = new HashMap<>();
  private final Map<String, List<ImmutableSegment>> _segmentMap = new HashMap<>();
  private final InstanceDataManager _instanceDataManager;
  private final Map<String, TableDataManager> _tableDataManagers = new HashMap<>();
  private final Map<String, File> _indexDirs;

  private QueryRunner _queryRunner;

  public QueryServerEnclosure(List<String> tables, Map<String, File> indexDirs, Map<String, List<String>> segments) {
    _indexDirs = indexDirs;
    try {
      for (int i = 0; i < tables.size(); i++) {
        String tableName = tables.get(i);
        File indexDir = indexDirs.get(tableName);
        FileUtils.deleteQuietly(indexDir);
        List<ImmutableSegment> segmentList = new ArrayList<>();
        for (String segmentName : segments.get(tableName)) {
          segmentList.add(buildSegment(indexDir, tableName, segmentName));
        }
        _segmentMap.put(tableName, segmentList);
      }
      _instanceDataManager = mockInstanceDataManager();
      _queryRunnerPort = QueryEnvironmentTestUtils.getAvailablePort();
      _runnerConfig.put(QueryConfig.KEY_OF_QUERY_RUNNER_PORT, _queryRunnerPort);
      _runnerConfig.put(QueryConfig.KEY_OF_QUERY_RUNNER_HOSTNAME,
          String.format("Server_%s", QueryConfig.DEFAULT_QUERY_RUNNER_HOSTNAME));
      _queryRunner = new QueryRunner();
      _testExecutor = Executors.newFixedThreadPool(DEFAULT_EXECUTOR_THREAD_NUM,
          new NamedThreadFactory("test_query_server_enclosure_on_" + _queryRunnerPort + "_port"));
    } catch (Exception e) {
      throw new RuntimeException("Test Failed!", e);
    }
  }

  public ServerMetrics mockServiceMetrics() {
    return mock(ServerMetrics.class);
  }

  public InstanceDataManager mockInstanceDataManager() {
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    for (Map.Entry<String, List<ImmutableSegment>> e : _segmentMap.entrySet()) {
      TableDataManager tableDataManager = mockTableDataManager(e.getValue());
      _tableDataManagers.put(e.getKey(), tableDataManager);
    }
    for (Map.Entry<String, TableDataManager> e : _tableDataManagers.entrySet()) {
      when(instanceDataManager.getTableDataManager(matches(String.format("%s.*", e.getKey())))).thenReturn(
          e.getValue());
    }
    return instanceDataManager;
  }

  public TableDataManager mockTableDataManager(List<ImmutableSegment> segmentList) {
    List<SegmentDataManager> tableSegmentDataManagers =
        segmentList.stream().map(ImmutableSegmentDataManager::new).collect(Collectors.toList());
    TableDataManager tableDataManager = mock(TableDataManager.class);
    when(tableDataManager.acquireSegments(any(), any())).thenReturn(tableSegmentDataManagers);
    return tableDataManager;
  }

  public ImmutableSegment buildSegment(File indexDir, String tableName, String segmentName)
      throws Exception {
    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRow row = new GenericRow();
      row.putValue("col1", STRING_FIELD_LIST[i % STRING_FIELD_LIST.length]);
      row.putValue("col2", STRING_FIELD_LIST[i % (STRING_FIELD_LIST.length - 2)]);
      row.putValue("col3", INT_FIELD_LIST[i % INT_FIELD_LIST.length]);
      row.putValue("ts", tableName.endsWith("_O")
          ? System.currentTimeMillis() - TimeUnit.DAYS.toMillis(2) : System.currentTimeMillis());
      rows.add(row);
    }

    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName).setTimeColumnName("ts").build();
    Schema schema = QueryEnvironmentTestUtils.SCHEMA_BUILDER.setSchemaName(tableName).build();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(indexDir.getPath());
    config.setTableName(tableName);
    config.setSegmentName(segmentName);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(rows)) {
      driver.init(config, recordReader);
    }
    driver.build();
    return ImmutableSegmentLoader.load(new File(indexDir, segmentName), ReadMode.mmap);
  }

  public int getPort() {
    return _queryRunnerPort;
  }

  public void start()
      throws Exception {
    PinotConfiguration configuration = new PinotConfiguration(_runnerConfig);
    _queryRunner = new QueryRunner();
    _queryRunner.init(configuration, _instanceDataManager, mockServiceMetrics());
    _queryRunner.start();
  }

  public void shutDown() {
    _queryRunner.shutDown();
    for (Map.Entry<String, List<ImmutableSegment>> e : _segmentMap.entrySet()) {
      for (ImmutableSegment segment : e.getValue()) {
        segment.destroy();
      }
      FileUtils.deleteQuietly(_indexDirs.get(e.getKey()));
    }
  }

  public void processQuery(DistributedStagePlan distributedStagePlan, Map<String, String> requestMetadataMap) {
    _queryRunner.processQuery(distributedStagePlan, _testExecutor, requestMetadataMap);
  }
}
