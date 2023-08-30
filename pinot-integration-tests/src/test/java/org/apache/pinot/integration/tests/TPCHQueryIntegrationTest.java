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

import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.pinot.client.ResultSetGroup;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.tools.utils.JarUtils;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TPCHQueryIntegrationTest extends BaseClusterIntegrationTest {
  private static final Map<String, String> TPCH_QUICKSTART_TABLE_RESOURCES;
  private static final int NUM_TPCH_QUERIES = 24;
  private static final Set<Integer> EXEMPT_QUERIES;

  static {
    TPCH_QUICKSTART_TABLE_RESOURCES = new HashMap<>();
    TPCH_QUICKSTART_TABLE_RESOURCES.put("orders", "examples/batch/tpch/orders");
    TPCH_QUICKSTART_TABLE_RESOURCES.put("lineitem", "examples/batch/tpch/lineitem");
    TPCH_QUICKSTART_TABLE_RESOURCES.put("region", "examples/batch/tpch/region");
    TPCH_QUICKSTART_TABLE_RESOURCES.put("partsupp", "examples/batch/tpch/partsupp");
    TPCH_QUICKSTART_TABLE_RESOURCES.put("customer", "examples/batch/tpch/customer");
    TPCH_QUICKSTART_TABLE_RESOURCES.put("nation", "examples/batch/tpch/nation");
    TPCH_QUICKSTART_TABLE_RESOURCES.put("part", "examples/batch/tpch/part");
    TPCH_QUICKSTART_TABLE_RESOURCES.put("supplier", "examples/batch/tpch/supplier");
    EXEMPT_QUERIES = new HashSet<>();
    // The following queries fail due to lack of support for views.
    EXEMPT_QUERIES.addAll(ImmutableList.of(15, 16, 17));
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

    setUpH2Connection();
    for (Map.Entry<String, String> tableResource : TPCH_QUICKSTART_TABLE_RESOURCES.entrySet()) {
      File tableSegmentDir = new File(_segmentDir, tableResource.getKey());
      File tarDir = new File(_tarDir, tableResource.getKey());
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
      TestUtils.ensureDirectoriesExistAndEmpty(tableSegmentDir, tarDir);
      Schema schema = createSchema(schemaFile);
      addSchema(schema);
      TableConfig tableConfig = createTableConfig(tableFile);
      addTableConfig(tableConfig);
      ClusterIntegrationTestUtils.buildSegmentsFromAvro(Collections.singletonList(dataFile), tableConfig, schema, 0,
          tableSegmentDir, tarDir);
      uploadSegments(tableName, tarDir);
      // H2
      ClusterIntegrationTestUtils.setUpH2TableWithAvro(Collections.singletonList(dataFile), tableName, _h2Connection);
    }
  }

  @Test(dataProvider = "QueryDataProvider")
  public void testTPCHQueries(String[] pinotAndH2Queries) throws Exception {
    testQueriesSucceed(pinotAndH2Queries[0], pinotAndH2Queries[1]);
  }

  protected void testQueriesSucceed(String pinotQuery, String h2Query) throws Exception {
    ResultSetGroup pinotResultSetGroup = getPinotConnection().execute(pinotQuery);
    org.apache.pinot.client.ResultSet resultTableResultSet = pinotResultSetGroup.getResultSet(0);
    if (CollectionUtils.isNotEmpty(pinotResultSetGroup.getExceptions())) {
      Assert.fail(String.format(
          "TPC-H query raised exception: %s. query: %s", pinotResultSetGroup.getExceptions().get(0), pinotQuery));
    }

    int numRows = resultTableResultSet.getRowCount();
    int numColumns = resultTableResultSet.getColumnCount();

    // h2 response
    Assert.assertNotNull(_h2Connection);
    Statement h2statement = _h2Connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    h2statement.execute(h2Query);
    ResultSet h2ResultSet = h2statement.getResultSet();
    // TODO: fix the query parsing issue in Pinot
    //  many queries fail due to "ERROR [Connection] [main] Cannot parse table name from query"
    //  java.lang.ClassCastException:
    //  class org.apache.calcite.sql.SqlSelect cannot be cast to class org.apache.calcite.sql.SqlBasicCall
    //  (org.apache.calcite.sql.SqlSelect and org.apache.calcite.sql.SqlBasicCall are in unnamed module of loader 'app')
  }

  @Override
  protected long getCurrentCountStarResult() {
    return getPinotConnection().execute("SELECT COUNT(*) FROM orders").getResultSet(0).getLong(0);
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
    // unload all TPCH tables.
    for (String table : TPCH_QUICKSTART_TABLE_RESOURCES.keySet()) {
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
  public static Object[][] queryDataProvider()
      throws IOException {
    Object[][] queries = new Object[NUM_TPCH_QUERIES - EXEMPT_QUERIES.size()][];
    for (int query = 1, iter = 0; query <= NUM_TPCH_QUERIES; query++) {
      if (EXEMPT_QUERIES.contains(query)) {
        continue;
      }
      String pinotQueryFilePath = String.format("tpch/%s.sql", query);
      try (InputStream pinotQueryIs = TPCHQueryIntegrationTest.class.getClassLoader()
          .getResourceAsStream(pinotQueryFilePath)) {
        queries[iter] = new Object[2];
        queries[iter][0] = IOUtils.toString(Objects.requireNonNull(pinotQueryIs), Charset.defaultCharset());
        String h2QueryFilePath = String.format("tpch/%s-h2.sql", query);
        try (InputStream h2QueryIs = TPCHQueryIntegrationTest.class.getClassLoader()
            .getResourceAsStream(h2QueryFilePath)) {
          queries[iter][1] = IOUtils.toString(Objects.requireNonNull(h2QueryIs), Charset.defaultCharset());
        } catch (Exception e) {
          queries[iter][1] = queries[iter][0];
        }
        iter++;
      }
    }
    return queries;
  }
}
