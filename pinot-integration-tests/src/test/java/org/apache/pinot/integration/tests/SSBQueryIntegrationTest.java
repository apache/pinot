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

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.client.ResultSetGroup;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.tools.utils.JarUtils;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.yaml.snakeyaml.Yaml;


@Test(suiteName = "integration-suite-2", groups = {"integration-suite-2"})
public class SSBQueryIntegrationTest extends BaseClusterIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(SSBQueryIntegrationTest.class);
  private static final Map<String, String> SSB_QUICKSTART_TABLE_RESOURCES = ImmutableMap.of(
      "customer", "examples/batch/ssb/customer",
      "dates", "examples/batch/ssb/dates",
      "lineorder", "examples/batch/ssb/lineorder",
      "part", "examples/batch/ssb/part",
      "supplier", "examples/batch/ssb/supplier");
  private static final String SSB_QUERY_SET_RESOURCE_NAME = "ssb/ssb_query_set.yaml";

  @BeforeClass
  public void setUp()
      throws Exception {
    System.out.println("this.getClass().getName() = " + this.getClass().getName());
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

    setUpH2Connection();
    for (Map.Entry<String, String> tableResource : SSB_QUICKSTART_TABLE_RESOURCES.entrySet()) {
      String tableName = tableResource.getKey();
      URL resourceUrl = getClass().getClassLoader().getResource(tableResource.getValue());
      Assert.assertNotNull(resourceUrl, "Unable to find resource from: " + tableResource.getValue());
      File resourceFile;
      if ("jar".equals(resourceUrl.getProtocol())) {
        String[] splits = resourceUrl.getFile().split("!");
        File tempUnpackDir = new File(_tempDir.getAbsolutePath() + File.separator + splits[1]);
        TestUtils.ensureDirectoriesExistAndEmpty(tempUnpackDir);
        JarUtils.copyResourcesToDirectory(splits[0], splits[1].substring(1), tempUnpackDir.getAbsolutePath());
        resourceFile = tempUnpackDir;
      } else {
        resourceFile = new File(resourceUrl.getFile());
      }
      File dataFile = new File(resourceFile.getAbsolutePath(), "rawdata" + File.separator + tableName + ".avro");
      Assert.assertTrue(dataFile.exists(), "Unable to load resource file from URL: " + dataFile);
      File schemaFile = new File(resourceFile.getPath(), tableName + "_schema.json");
      File tableFile = new File(resourceFile.getPath(), tableName + "_offline_table_config.json");
      // Pinot
      TestUtils.ensureDirectoriesExistAndEmpty(_segmentDir, _tarDir);
      Schema schema = createSchema(schemaFile);
      addSchema(schema);
      TableConfig tableConfig = createTableConfig(tableFile);
      addTableConfig(tableConfig);
      ClusterIntegrationTestUtils.buildSegmentsFromAvro(Collections.singletonList(dataFile), tableConfig, schema, 0,
          _segmentDir, _tarDir);
      uploadSegments(tableName, _tarDir);
      // H2
      ClusterIntegrationTestUtils.setUpH2TableWithAvro(Collections.singletonList(dataFile), tableName, _h2Connection);
    }
  }

  @Test(dataProvider = "QueryDataProvider")
  public void testSSBQueries(String query)
      throws Exception {
    testQueriesValidateAgainstH2(query);
  }

  protected void testQueriesValidateAgainstH2(String query)
      throws Exception {
    // connection response
    ResultSetGroup pinotResultSetGroup = getPinotConnection().execute(query);
    org.apache.pinot.client.ResultSet resultTableResultSet = pinotResultSetGroup.getResultSet(0);
    int numRows = resultTableResultSet.getRowCount();
    int numColumns = resultTableResultSet.getColumnCount();

    // h2 response
    Assert.assertNotNull(_h2Connection);
    Statement h2statement = _h2Connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    h2statement.execute(query);
    ResultSet h2ResultSet = h2statement.getResultSet();

    // compare results.
    Assert.assertEquals(numColumns, h2ResultSet.getMetaData().getColumnCount());
    if (h2ResultSet.first()) {
      for (int i = 0; i < numRows; i++) {
        for (int c = 0; c < numColumns; c++) {
          String h2Value = h2ResultSet.getString(c + 1);
          String pinotValue = resultTableResultSet.getString(i, c);
          boolean error = ClusterIntegrationTestUtils.fuzzyCompare(h2Value, pinotValue, pinotValue);
          if (error) {
            throw new RuntimeException("Value: " + c + " does not match at (" + i + ", " + c + "), "
                + "expected h2 value: " + h2Value + ", actual Pinot value: " + pinotValue);
          }
        }
        if (!h2ResultSet.next() && i != numRows - 1) {
          throw new RuntimeException("H2 result set is smaller than Pinot result set after: " + i + " rows!");
        }
      }
    }
    Assert.assertFalse(h2ResultSet.next(), "Pinot result set is smaller than H2 result set after: "
        + numRows + " rows!");
  }

  @Override
  protected long getCurrentCountStarResult() {
    return getPinotConnection().execute("SELECT COUNT(*) FROM lineorder").getResultSet(0).getLong(0);
  }

  @Override
  protected long getCountStarResult() {
    return 9999L;
  }

  @Override
  protected boolean useMultiStageQueryEngine() {
    return true;
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    // unload all SSB tables.
    for (String table : SSB_QUICKSTART_TABLE_RESOURCES.keySet()) {
      dropOfflineTable(table);
    }

    // stop components and clean up
    stopServer();
    stopBroker();
    stopController();
    stopZk();

    FileUtils.deleteDirectory(_tempDir);
  }

  @DataProvider(name = "QueryDataProvider")
  public static Object[][] queryDataProvider() {
    Yaml yaml = new Yaml();
    InputStream inputStream = SSBQueryIntegrationTest.class.getClassLoader()
        .getResourceAsStream(SSB_QUERY_SET_RESOURCE_NAME);
    Map<String, List<String>> ssbQuerySet = yaml.load(inputStream);
    List<String> ssbQueryList = ssbQuerySet.get("sqls");
    return ssbQueryList.stream().map(s -> new Object[]{s}).toArray(Object[][]::new);
  }
}
