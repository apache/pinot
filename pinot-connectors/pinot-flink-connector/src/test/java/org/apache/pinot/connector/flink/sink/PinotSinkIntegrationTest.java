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
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.pinot.connector.flink.common.PinotRowRecordConverter;
import org.apache.pinot.connector.flink.util.PinotTestBase;
import org.apache.pinot.connector.flink.util.TestUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class PinotSinkIntegrationTest {

  private static PinotTestBase _testBase = new PinotTestBase();

  @BeforeClass
  public static void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_testBase.getTempDir(), _testBase.getSegmentDir(), _testBase.getTarDir());

    // Start the Pinot cluster
    _testBase.startCluster();
  }

  @AfterClass
  public static void tearDown()
      throws Exception {
    _testBase.stopCluster();

    FileUtils.deleteDirectory(_testBase.getTempDir());
  }

  @Test
  public void testPinotSinkWrite()
      throws Exception {
    StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    execEnv.setParallelism(1);
    DataStream<Row> srcDs = execEnv.fromCollection(_testBase.TEST_DATA).returns(_testBase.TEST_TYPE_INFO);

    TableConfig tableConfig = _testBase.createOfflineTableConfig();
    _testBase.addTableConfig(tableConfig);
    srcDs.addSink(
        new PinotSinkFunction<>(new PinotRowRecordConverter(_testBase.TEST_TYPE_INFO), tableConfig, _testBase.SCHEMA));
    execEnv.execute();
    Assert.assertEquals(getNumSegments(), 1);
    Assert.assertEquals(getTotalNumDocs(), 6);
    dropTable();
  }

  @Test
  public void testPinotSinkParallelWrite()
      throws Exception {
    StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    execEnv.setParallelism(2);
    DataStream<Row> srcDs =
        execEnv.fromCollection(_testBase.TEST_DATA).returns(_testBase.TEST_TYPE_INFO).keyBy(r -> r.getField(0));

    TableConfig tableConfig = _testBase.createOfflineTableConfig();
    _testBase.addTableConfig(tableConfig);
    srcDs.addSink(
        new PinotSinkFunction<>(new PinotRowRecordConverter(_testBase.TEST_TYPE_INFO), tableConfig, _testBase.SCHEMA));
    execEnv.execute();
    Assert.assertEquals(getNumSegments(), 2);
    Assert.assertEquals(getTotalNumDocs(), 6);
    dropTable();
  }

  private void dropTable()
      throws IOException {
    String tableNameWithType = TableNameBuilder.forType(TableType.OFFLINE).tableNameWithType(_testBase.getTableName());
    _testBase.dropOfflineTable(tableNameWithType);
  }

  private int getNumSegments()
      throws IOException {
    String tableNameWithType = TableNameBuilder.forType(TableType.OFFLINE).tableNameWithType(_testBase.getTableName());
    String jsonOutputStr = _testBase.sendGetRequest(_testBase.getControllerRequestURLBuilder()
        .forSegmentListAPIWithTableType(tableNameWithType, TableType.OFFLINE.toString()));
    JsonNode array = JsonUtils.stringToJsonNode(jsonOutputStr);
    return array.get(0).get("OFFLINE").size();
  }

  private int getTotalNumDocs()
      throws IOException {
    String tableNameWithType = TableNameBuilder.forType(TableType.OFFLINE).tableNameWithType(_testBase.getTableName());
    String jsonOutputStr = _testBase.sendGetRequest(_testBase.getControllerRequestURLBuilder()
        .forSegmentListAPIWithTableType(tableNameWithType, TableType.OFFLINE.toString()));
    JsonNode array = JsonUtils.stringToJsonNode(jsonOutputStr);
    JsonNode segments = array.get(0).get("OFFLINE");
    int totalDocCount = 0;
    for (int i = 0; i < segments.size(); i++) {
      String segmentName = segments.get(i).asText();
      jsonOutputStr = _testBase.sendGetRequest(
          _testBase.getControllerRequestURLBuilder().forSegmentMetadata(tableNameWithType, segmentName));
      JsonNode metadata = JsonUtils.stringToJsonNode(jsonOutputStr);
      totalDocCount += metadata.get("segment.total.docs").asInt();
    }
    return totalDocCount;
  }
}
