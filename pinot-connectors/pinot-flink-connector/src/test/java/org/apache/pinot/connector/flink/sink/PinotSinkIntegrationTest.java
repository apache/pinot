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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


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
    batchConfigs.put(BatchConfigProperties.PUSH_CONTROLLER_URI, getControllerBaseApiUrl());
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

    addSchema(_schema);
    addTableConfig(_tableConfig);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    dropOfflineTable(OFFLINE_TABLE_NAME);
    deleteSchema(RAW_TABLE_NAME);

    stopServer();
    stopBroker();
    stopController();
    stopZk();

    FileUtils.deleteDirectory(_tempDir);
  }

  @Test
  public void testPinotSinkWrite()
      throws Exception {
    // Single-thread write
    StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    execEnv.setParallelism(1);
    DataStream<Row> srcDs = execEnv.fromCollection(_data).returns(_typeInfo);
    srcDs.addSink(new PinotSinkFunction<>(new FlinkRowGenericRowConverter(_typeInfo), _tableConfig, _schema));
    execEnv.execute();
    verifySegments(1, 6);

    dropAllSegments(RAW_TABLE_NAME, TableType.OFFLINE);

    // Parallel write
    execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    execEnv.setParallelism(2);
    srcDs = execEnv.fromCollection(_data).returns(_typeInfo).keyBy(r -> r.getField(0));
    srcDs.addSink(new PinotSinkFunction<>(new FlinkRowGenericRowConverter(_typeInfo), _tableConfig, _schema));
    execEnv.execute();
    verifySegments(2, 6);
  }

  private void verifySegments(int numSegments, int numTotalDocs)
      throws IOException {
    JsonNode segments = JsonUtils.stringToJsonNode(sendGetRequest(
            _controllerRequestURLBuilder.forSegmentListAPI(OFFLINE_TABLE_NAME, TableType.OFFLINE.toString()))).get(0)
        .get("OFFLINE");
    assertEquals(segments.size(), numSegments);
    int actualNumTotalDocs = 0;
    for (int i = 0; i < numSegments; i++) {
      String segmentName = segments.get(i).asText();
      actualNumTotalDocs += JsonUtils.stringToJsonNode(
              sendGetRequest(_controllerRequestURLBuilder.forSegmentMetadata(OFFLINE_TABLE_NAME, segmentName)))
          .get("segment.total.docs").asInt();
    }
    assertEquals(actualNumTotalDocs, numTotalDocs);
  }
}
