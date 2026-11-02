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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.pinot.connector.flink.common.FlinkRowGenericRowConverter;
import org.apache.pinot.integration.tests.BaseClusterIntegrationTest;
import org.apache.pinot.integration.tests.BasicAuthTestUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Runs [PinotSink] against a cluster with Basic Auth enabled on the controller.
public class PinotSinkAuthIntegrationTest extends BaseClusterIntegrationTest {
  private static final String RAW_TABLE_NAME = "authTable";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME);

  private List<Row> _data;
  private RowTypeInfo _typeInfo;
  private Schema _schema;

  @Override
  protected void overrideControllerConf(Map<String, Object> properties) {
    BasicAuthTestUtils.addControllerConfiguration(properties);
  }

  @Override
  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    BasicAuthTestUtils.addBrokerConfiguration(brokerConf);
  }

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    BasicAuthTestUtils.addServerConfiguration(serverConf);
  }

  @Override
  protected Map<String, String> getAdminClientHeaders() {
    return BasicAuthTestUtils.AUTH_HEADER;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    startZk();
    startController();
    startBroker();
    startServer();

    _data = Arrays.asList(Row.of(1, 1L, "Hi"), Row.of(2, 2L, "Hello"), Row.of(3, 3L, "Hello world"));
    _typeInfo =
        new RowTypeInfo(new TypeInformation[]{Types.INT, Types.LONG, Types.STRING}, new String[]{"a", "b", "c"});
    _schema =
        new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME).addSingleValueDimension("a", FieldSpec.DataType.INT)
            .addSingleValueDimension("b", FieldSpec.DataType.LONG)
            .addSingleValueDimension("c", FieldSpec.DataType.STRING).build();

    addSchema(_schema);
    // The table config stored in the controller has no batch config and never contains the token
    addTableConfig(new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build());
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
  public void testUploadSucceedsWithAuthToken()
      throws Exception {
    // Make sure the controller actually enforces auth, otherwise the upload below would pass without a token
    int statusCode = sendGetRequestWithStatusCode(
        getControllerRequestURLBuilder().forTableGet(RAW_TABLE_NAME), Map.of()).getLeft();
    assertEquals(statusCode, 401);

    // Same flow as the README: fetch the table config, then pass the token to the sink
    TableConfig tableConfig =
        getOrCreateAdminClient().getTableClient().getTableConfigObjectForType(RAW_TABLE_NAME, TableType.OFFLINE);
    StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    execEnv.setParallelism(1);
    execEnv.fromData(_data, _typeInfo)
        .sinkTo(new PinotSink<>(new FlinkRowGenericRowConverter(_typeInfo), tableConfig, _schema,
            getControllerBaseApiUrl(), BasicAuthTestUtils.AUTH_TOKEN));
    execEnv.execute();

    List<String> segments = getOrCreateAdminClient().getSegmentClient()
        .listSegments(OFFLINE_TABLE_NAME, TableType.OFFLINE.toString(), false);
    assertEquals(segments.size(), 1);
  }
}
