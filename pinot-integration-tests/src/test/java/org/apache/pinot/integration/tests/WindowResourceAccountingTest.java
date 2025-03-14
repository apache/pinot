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
package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.accounting.AggregateByQueryIdAccountantFactoryForTest;
import org.apache.pinot.spi.accounting.QueryResourceTracker;
import org.apache.pinot.spi.accounting.ThreadResourceUsageAccountant;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.ControllerRequestURLBuilder;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class WindowResourceAccountingTest extends BaseClusterIntegrationTest {

  private static final String DEFAULT_TABLE_NAME = "WindowFunnelTest";
  private static final String URL_COLUMN = "url";
  private static final String TIMESTAMP_COLUMN = "timestampCol";
  private static final String USER_ID_COLUMN = "userId";
  private static long _countStarResult = 0;

  @Override
  protected long getCountStarResult() {
    return _countStarResult;
  }

  protected void overrideServerConf(PinotConfiguration serverConf) {
    serverConf.setProperty(
        CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "." + CommonConstants.Accounting.CONFIG_OF_FACTORY_NAME,
        AggregateByQueryIdAccountantFactoryForTest.class.getCanonicalName());
    serverConf.setProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
        + CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_MEMORY_SAMPLING, true);
    serverConf.setProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
        + CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_CPU_SAMPLING, false);
    serverConf.setProperty(CommonConstants.Server.CONFIG_OF_ENABLE_THREAD_ALLOCATED_BYTES_MEASUREMENT, true);
  }

  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    brokerConf.setProperty(
        CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "." + CommonConstants.Accounting.CONFIG_OF_INSTANCE_TYPE,
        InstanceType.BROKER);
    brokerConf.setProperty(
        CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "." + CommonConstants.Accounting.CONFIG_OF_FACTORY_NAME,
        AggregateByQueryIdAccountantFactoryForTest.class.getCanonicalName());
    brokerConf.setProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
        + CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_MEMORY_SAMPLING, true);
    brokerConf.setProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
        + CommonConstants.Accounting.CONFIG_OF_OOM_PROTECTION_KILLING_QUERY, true);
    brokerConf.setProperty(CommonConstants.Broker.CONFIG_OF_ENABLE_THREAD_ALLOCATED_BYTES_MEASUREMENT, true);
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);
    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

    if (_controllerRequestURLBuilder == null) {
      _controllerRequestURLBuilder =
          ControllerRequestURLBuilder.baseUrl("http://localhost:" + getControllerPort());
    }
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);
    // create & upload schema AND table config
    Schema schema = createSchema();
    addSchema(schema);

    List<File> avroFiles = createAvroFiles();
      // create offline table
      TableConfig tableConfig = createOfflineTableConfig();
      addTableConfig(tableConfig);

      // create & upload segments
      int segmentIndex = 0;
      for (File avroFile : avroFiles) {
        ClusterIntegrationTestUtils.buildSegmentFromAvro(avroFile, tableConfig, schema, segmentIndex++, _segmentDir,
            _tarDir);
        uploadSegments(getTableName(), _tarDir);
      }
    }

  @AfterClass
  public void tearDown()
      throws IOException {
    // Shutdown the Pinot cluster
    stopServer();
    stopBroker();
    stopController();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  @Test
  public void testFunnel()
      throws Exception {
    setUseMultiStageQueryEngine(false);
    String query = String.format(
        "SELECT " + "funnelMaxStep(timestampCol, '1000', 4, " + "url = '/product/search', " + "url = '/cart/add', "
            + "url = '/checkout/start', " + "url = '/checkout/confirmation' " + ") " + "FROM %s LIMIT %d",
        getTableName(), getCountStarResult());

    JsonNode response = postQuery(query);
    ThreadResourceUsageAccountant accountant = Tracing.getThreadAccountant();
    assertEquals(getBrokerConf(0).getProperty(
            CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "." + CommonConstants.Accounting.CONFIG_OF_FACTORY_NAME),
        AggregateByQueryIdAccountantFactoryForTest.class.getCanonicalName());
    assertEquals(getServerConf(0).getProperty(
            CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "." + CommonConstants.Accounting.CONFIG_OF_FACTORY_NAME),
        AggregateByQueryIdAccountantFactoryForTest.class.getCanonicalName());
    assertEquals(accountant.getClass().getCanonicalName(),
        AggregateByQueryIdAccountantFactoryForTest.AggregateByQueryIdAccountant.class.getCanonicalName());
    Map<String, ? extends QueryResourceTracker> queryMemUsage = accountant.getQueryResources();
    assertFalse(queryMemUsage.isEmpty());
    boolean foundRequestId = false;
    String queryIdKey = null;
    for (String key : queryMemUsage.keySet()) {
      if (key.contains(response.get("requestId").asText())) {
        foundRequestId = true;
        queryIdKey = key;
        break;
      }
    }
    assertTrue(foundRequestId);
    assertTrue(queryMemUsage.get(queryIdKey).getAllocatedBytes() > 0);
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(getTableName()).build();
  }

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addSingleValueDimension(URL_COLUMN, FieldSpec.DataType.STRING)
        .addSingleValueDimension(TIMESTAMP_COLUMN, FieldSpec.DataType.TIMESTAMP)
        .addSingleValueDimension(USER_ID_COLUMN, FieldSpec.DataType.STRING)
        .build();
  }

  public List<File> createAvroFiles()
      throws Exception {
    // create avro schema
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(ImmutableList.of(
        new org.apache.avro.Schema.Field(URL_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING),
            null, null),
        new org.apache.avro.Schema.Field(TIMESTAMP_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG),
            null, null),
        new org.apache.avro.Schema.Field(USER_ID_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING),
            null, null)
    ));

    long[][] userTimestampValues = new long[][]{
        new long[]{1000, 1010, 1020, 1025, 1030},
        new long[]{2010, 2010, 2000},
        new long[]{1000, 1010, 1015, 1020, 11030},
        new long[]{2020, 12010, 12050},
    };
    String[][] userUrlValues = new String[][]{
        new String[]{"/product/search", "/cart/add", "/checkout/start", "/cart/add", "/checkout/confirmation"},
        new String[]{"/checkout/start", "/cart/add", "/product/search"},
        new String[]{"/product/search", "/cart/add", "/cart/add", "/checkout/start", "/checkout/confirmation"},
        new String[]{"/checkout/start", "/cart/add", "/product/search"},
    };
    int repeats = 10;
    long totalRows = 0;
    for (String[] userUrlValue : userUrlValues) {
      totalRows += userUrlValue.length;
    }
    _countStarResult = totalRows * repeats;
    // create avro file
    List<File> avroFiles = new ArrayList<>();
    for (int repeat = 0; repeat < repeats; repeat++) {
      File avroFile = new File(_tempDir, "data" + repeat + ".avro");
      try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
        fileWriter.create(avroSchema, avroFile);
        for (int i = 0; i < userUrlValues.length; i++) {
          for (int j = 0; j < userUrlValues[i].length; j++) {
            GenericData.Record record = new GenericData.Record(avroSchema);
            record.put(TIMESTAMP_COLUMN, userTimestampValues[i][j]);
            record.put(URL_COLUMN, userUrlValues[i][j]);
            record.put(USER_ID_COLUMN, "user" + i + repeat);
            fileWriter.append(record);
          }
        }
      }
      avroFiles.add(avroFile);
    }
    return avroFiles;
  }
}
