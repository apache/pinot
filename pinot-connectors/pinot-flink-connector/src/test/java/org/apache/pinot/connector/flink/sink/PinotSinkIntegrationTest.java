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
package org.apache.pinot.connector.flink.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.pinot.connector.flink.common.FlinkRowGenericRowConverter;
import org.apache.pinot.integration.tests.BaseClusterIntegrationTest;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class PinotSinkIntegrationTest extends BaseClusterIntegrationTest {
  public static final String RAW_TABLE_NAME = "testTable";
  public static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME);

  private List<Row> _data;
  public RowTypeInfo _typeInfo;
  public TableConfig _tableConfig;
  public Schema _schema;

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    startZk();
    startController();
    startBroker();
    startServer();

    _data = Arrays.asList(Row.of(1, 1L, "Hi"), Row.of(2, 2L, "Hello"), Row.of(3, 3L, "Hello world"),
        Row.of(4, 4L, "Hello world!"), Row.of(5, 5L, "HelloWorld"), Row.of(6, 6L, "Hello!world!"));
    _typeInfo =
        new RowTypeInfo(new TypeInformation[]{Types.INT, Types.LONG, Types.STRING}, new String[]{"a", "b", "c"});
    Map<String, String> batchConfigs = new HashMap<>();
    batchConfigs.put(BatchConfigProperties.OUTPUT_DIR_URI, _tarDir.getAbsolutePath());
    batchConfigs.put(BatchConfigProperties.OVERWRITE_OUTPUT, "false");
    batchConfigs.put(BatchConfigProperties.PUSH_CONTROLLER_URI, _controllerBaseApiUrl);
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setBatchIngestionConfig(
        new BatchIngestionConfig(Collections.singletonList(batchConfigs), "APPEND", "HOURLY"));
    _tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setIngestionConfig(ingestionConfig)
            .build();
    _schema =
        new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME).addSingleValueDimension("a", FieldSpec.DataType.INT)
            .addSingleValueDimension("b", FieldSpec.DataType.LONG)
            .addSingleValueDimension("c", FieldSpec.DataType.STRING).setPrimaryKeyColumns(Lists.newArrayList("a"))
            .build();
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    stopServer();
    stopBroker();
    stopController();
    stopZk();

    FileUtils.deleteDirectory(_tempDir);
  }

  @BeforeMethod
  public void beforeMethod()
      throws IOException {
    addSchema(_schema);
    addTableConfig(_tableConfig);
  }

  @AfterMethod
  public void afterMethod()
      throws IOException {
    deleteSchema(RAW_TABLE_NAME);
    dropOfflineTable(OFFLINE_TABLE_NAME);
  }

  @Test
  public void testPinotSinkWrite()
      throws Exception {

    StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    execEnv.setParallelism(1);
    DataStream<Row> srcDs = execEnv.fromCollection(_data).returns(_typeInfo);
    srcDs.addSink(
        new PinotSinkFunction<>(new FlinkRowGenericRowConverter(_typeInfo), _tableConfig, _schema));
    execEnv.execute();
    Assert.assertEquals(getNumSegments(), 1);
    Assert.assertEquals(getTotalNumDocs(), 6);
  }

  @Test
  public void testPinotSinkParallelWrite()
      throws Exception {

    StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    execEnv.setParallelism(2);
    execEnv.setMaxParallelism(2);
    DataStream<Row> srcDs = execEnv.fromCollection(_data).returns(_typeInfo).keyBy(r -> r.getField(0));
    srcDs.addSink(
        new PinotSinkFunction<>(new FlinkRowGenericRowConverter(_typeInfo), _tableConfig, _schema));

    execEnv.execute();
    Assert.assertEquals(getNumSegments(), 2);
    Assert.assertEquals(getTotalNumDocs(), 6);
  }

  @Test
  public void testPinotSinkCheckpointAndRestore()
      throws Exception {
    Configuration configuration = new Configuration();
    configuration.setString(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, "full");

    MiniClusterWithClientResource cluster = new MiniClusterWithClientResource(
        new MiniClusterResourceConfiguration.Builder().setConfiguration(configuration).setNumberTaskManagers(1)
            .setNumberSlotsPerTaskManager(1).build());
    cluster.before();

    StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();

    execEnv.enableCheckpointing(10L);
    execEnv.setParallelism(1);
    execEnv.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 0L));

    Long numRows = 100000L;

    // create a stream that fails one time only after a checkpoint has been taken
    // the pinot sink should be able to resume where it left off, since the source will not restart from the beginning
    DataStream<Row> srcDs =
        execEnv.addSource(new RowGeneratingSourceFunction(numRows)).returns(_typeInfo).keyBy(r -> r.getField(0));
    srcDs.map(new FailOnceAfterCheckpoint(10));
    srcDs.addSink(new PinotSinkFunction<>(new FlinkRowGenericRowConverter(_typeInfo), _tableConfig, _schema,
        numRows, PinotSinkFunction.DEFAULT_EXECUTOR_POOL_SIZE));

    JobGraph jobGraph = execEnv.getStreamGraph().getJobGraph();

    ClusterClient<?> client = cluster.getClusterClient();

    client.submitJob(jobGraph).thenCompose(client::requestJobResult).get()
        .toJobExecutionResult(getClass().getClassLoader());

    Assert.assertTrue(FailOnceAfterCheckpoint._hasFailed);
    Assert.assertEquals(getNumSegments(), 1);
    Assert.assertEquals(Long.valueOf(getTotalNumDocs()), numRows);

    cluster.after();
  }

  // we need a source that can be checkpointed to test checkpoint and restore
  // otherwise after failure the source would start from the beginning
  private static class RowGeneratingSourceFunction extends RichParallelSourceFunction<Row>
      implements ListCheckpointed<Integer> {
    private final long _numElements;
    private int _index;
    private volatile boolean _isRunning = true;

    RowGeneratingSourceFunction(long numElements) {
      _numElements = numElements;
    }

    @Override
    public void run(SourceContext<Row> ctx)
        throws Exception {
      final Object lockingObject = ctx.getCheckpointLock();

      final int step = getRuntimeContext().getNumberOfParallelSubtasks();

      if (_index == 0) {
        _index = getRuntimeContext().getIndexOfThisSubtask();
      }

      while (_isRunning && _index < _numElements) {
        Row result = Row.of(_index, Long.valueOf(_index), "Hi " + _index);

        synchronized (lockingObject) {
          _index += step;
          ctx.collect(result);
        }
      }
    }

    @Override
    public void cancel() {
      _isRunning = false;
    }

    @Override
    public List<Integer> snapshotState(long checkpointId, long timestamp)
        throws Exception {
      return Collections.singletonList(_index);
    }

    @Override
    public void restoreState(List<Integer> state)
        throws Exception {
      if (state.isEmpty() || state.size() > 1) {
        throw new RuntimeException("Test failed due to unexpected recovered state size " + state.size());
      }
      _index = state.get(0);
    }
  }

  // this operator will fail once only after a successful checkpoint was taken and the specified number of
  // records have been processed
  private static class FailOnceAfterCheckpoint extends RichMapFunction<Row, Row> implements CheckpointListener {
    static volatile boolean _hasFailed = false;
    private long _failurePos;
    private long _count;
    private boolean _wasCheckpointed;

    FailOnceAfterCheckpoint(long failurePos) {
      _failurePos = failurePos;
    }

    @Override
    public void open(Configuration parameters) {
      _count = 0;
    }

    @Override
    public Row map(Row row)
        throws Exception {

      _count++;

      if (!_hasFailed && _wasCheckpointed && _count >= _failurePos) {
        _hasFailed = true;
        throw new Exception("Test Failure");
      }

      return null;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
      _wasCheckpointed = true;
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
    }
  }

  private int getNumSegments()
      throws IOException {
    String jsonOutputStr = sendGetRequest(
        _controllerRequestURLBuilder.forSegmentListAPI(OFFLINE_TABLE_NAME, TableType.OFFLINE.toString()));
    JsonNode array = JsonUtils.stringToJsonNode(jsonOutputStr);
    return array.get(0).get("OFFLINE").size();
  }

  private int getTotalNumDocs()
      throws IOException {
    String jsonOutputStr = sendGetRequest(
        _controllerRequestURLBuilder.forSegmentListAPI(OFFLINE_TABLE_NAME, TableType.OFFLINE.toString()));
    JsonNode array = JsonUtils.stringToJsonNode(jsonOutputStr);
    JsonNode segments = array.get(0).get("OFFLINE");
    int totalDocCount = 0;
    for (int i = 0; i < segments.size(); i++) {
      String segmentName = segments.get(i).asText();
      jsonOutputStr = sendGetRequest(_controllerRequestURLBuilder.forSegmentMetadata(OFFLINE_TABLE_NAME, segmentName));
      JsonNode metadata = JsonUtils.stringToJsonNode(jsonOutputStr);
      totalDocCount += metadata.get("segment.total.docs").asInt();
    }
    return totalDocCount;
  }
}
