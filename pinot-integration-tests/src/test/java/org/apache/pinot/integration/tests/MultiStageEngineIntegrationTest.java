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
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.common.datatable.DataTableFactory;
import org.apache.pinot.query.service.QueryConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class MultiStageEngineIntegrationTest extends BaseClusterIntegrationTest {
  private static final String SCHEMA_FILE_NAME =
      "On_Time_On_Time_Performance_2014_100k_subset_nonulls_single_value_columns.schema";

  @Override
  protected String getSchemaFileName() {
    return SCHEMA_FILE_NAME;
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

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    // Create and upload segments
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(getTableName(), _tarDir);

    // Set up the H2 connection
    setUpH2Connection(avroFiles);

    // Initialize the query generator
    setUpQueryGenerator(avroFiles);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);

    // Setting data table version to 4
    DataTableFactory.setDataTableVersion(4);
  }

  @Override
  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    brokerConf.setProperty(CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_ENABLED, true);
    brokerConf.setProperty(QueryConfig.KEY_OF_QUERY_RUNNER_PORT, 8421);
  }

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    serverConf.setProperty(CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_ENABLED, true);
    serverConf.setProperty(QueryConfig.KEY_OF_QUERY_SERVER_PORT, 8842);
    serverConf.setProperty(QueryConfig.KEY_OF_QUERY_RUNNER_PORT, 8422);
  }

  @Test(dataProvider = "multiStageQueryEngineSqlTestSet")
  public void testMultiStageQuery(String sql, int expectedNumOfRows, int expectedNumOfColumns)
      throws IOException {
    JsonNode multiStageResponse = JsonUtils.stringToJsonNode(
        sendPostRequest(_brokerBaseApiUrl + "/query/sql",
            "{\"queryOptions\":\"useMultistageEngine=true\", \"sql\":\"" + sql + "\"}"));
    Assert.assertTrue(multiStageResponse.has("resultTable"));
    JsonNode jsonNode = multiStageResponse.get("resultTable");
    // TODO: assert actual result data payload.
    Assert.assertEquals(jsonNode.get("rows").size(), expectedNumOfRows);
    Assert.assertEquals(jsonNode.get("dataSchema").get("columnNames").size(), expectedNumOfColumns);
  }

  @DataProvider
  public Object[][] multiStageQueryEngineSqlTestSet() {
    return new Object[][] {
        new Object[]{"SELECT * FROM mytable_OFFLINE WHERE ArrDelay>10000", 0, 73},
        new Object[]{"SELECT COUNT(*) FROM mytable_OFFLINE WHERE Carrier='AA'", 1, 1},
        new Object[]{"SELECT * FROM mytable_OFFLINE WHERE ArrDelay>1000", 2, 73},
        new Object[]{"SELECT CarrierDelay, ArrDelay FROM mytable_OFFLINE"
            + " WHERE CarrierDelay=15 AND ArrDelay>20", 172, 2},
        new Object[]{"SELECT * FROM mytable_OFFLINE AS a JOIN mytable_OFFLINE AS b ON a.Origin = b.Origin "
            + " WHERE a.Carrier='AA' AND a.ArrDelay>1000 AND b.ArrDelay>1000", 2, 146}
    };
  }


  @AfterClass
  public void tearDown()
      throws Exception {
    // Setting data table version to 4
    DataTableFactory.setDataTableVersion(3);

    dropOfflineTable(DEFAULT_TABLE_NAME);

    stopServer();
    stopBroker();
    stopController();
    stopZk();

    FileUtils.deleteDirectory(_tempDir);
  }
}
