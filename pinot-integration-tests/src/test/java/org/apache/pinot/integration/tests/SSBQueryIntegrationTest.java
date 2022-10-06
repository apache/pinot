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
import java.net.URL;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.client.Connection;
import org.apache.pinot.client.ConnectionFactory;
import org.apache.pinot.client.ResultSetGroup;
import org.apache.pinot.common.datatable.DataTableFactory;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.query.service.QueryConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class SSBQueryIntegrationTest extends BaseClusterIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(SSBQueryIntegrationTest.class);
  private static final Map<String, String> SSB_QUICKSTART_TABLE_RESOURCES = ImmutableMap.of(
      "customer", "examples/batch/ssb/customer",
      "dates", "examples/batch/ssb/dates",
      "lineorder", "examples/batch/ssb/lineorder",
      "part", "examples/batch/ssb/part",
      "supplier", "examples/batch/ssb/supplier");

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
    for (Map.Entry<String, String> tableResource : SSB_QUICKSTART_TABLE_RESOURCES.entrySet()) {
      String tableName = tableResource.getKey();
      URL resourceUrl = getClass().getClassLoader().getResource(tableResource.getValue());
      Assert.assertNotNull(resourceUrl, "Unable to find resource from: " + tableResource.getValue());
      File resourceFile = new File(resourceUrl.getFile());
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
      ClusterIntegrationTestUtils.buildSegmentsFromAvro(Collections.singletonList(dataFile), tableConfig, schema,
          0, _segmentDir, _tarDir);
      uploadSegments(tableName, _tarDir);
      // H2
      ClusterIntegrationTestUtils.setUpH2TableWithAvro(Collections.singletonList(dataFile), tableName, _h2Connection);
    }

    // Setting data table version to 4
    DataTableBuilderFactory.setDataTableVersion(DataTableFactory.VERSION_4);
  }

  @Test(dataProvider = "ssbQueryDataProvider")
  public void testSSBQueries(String query)
      throws Exception {
    testSSBQuery(query);
  }

  private void testSSBQuery(String query)
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
  protected Connection getPinotConnection() {
    Properties properties = new Properties();
    properties.put("queryOptions", "useMultistageEngine=true");
    if (_pinotConnection == null) {
      _pinotConnection = ConnectionFactory.fromZookeeper(properties, getZkUrl() + "/" + getHelixClusterName());
    }
    return _pinotConnection;
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

  @DataProvider(name = "ssbQueryDataProvider")
  public static Object[][] ssbQueryDataProvider() {
    // TODO: refactor these out to .sql files
    return new Object[][]{
        new Object[]{"select sum(CAST(LO_EXTENDEDPRICE AS DOUBLE) * LO_DISCOUNT) as revenue "
            + " from lineorder, dates where LO_ORDERDATE = D_DATEKEY and D_YEAR = 1993 "
            + " and LO_DISCOUNT between 1 and 3 and LO_QUANTITY < 25; \n"},
        new Object[]{"select sum(CAST(LO_EXTENDEDPRICE AS DOUBLE) * LO_DISCOUNT) as revenue "
            + " from lineorder, dates where LO_ORDERDATE = D_DATEKEY and D_YEARMONTHNUM = 199401 "
            + " and LO_DISCOUNT between 4 and 6 and LO_QUANTITY between 26 and 35; \n"},
        new Object[]{"select sum(CAST(LO_EXTENDEDPRICE AS DOUBLE) * LO_DISCOUNT) as revenue "
            + " from lineorder, dates where LO_ORDERDATE = D_DATEKEY and D_WEEKNUMINYEAR = 6 and D_YEAR = 1994 "
            + " and LO_DISCOUNT between 5 and 7 and LO_QUANTITY between 26 and 35; \n"},
        new Object[]{"select sum(CAST(LO_REVENUE AS DOUBLE)), D_YEAR, P_BRAND1 "
            + " from lineorder, dates, part, supplier where LO_ORDERDATE = D_DATEKEY and LO_PARTKEY = P_PARTKEY "
            + " and LO_SUPPKEY = S_SUPPKEY and P_CATEGORY = 'MFGR#12' and S_REGION = 'AMERICA' "
            + " group by D_YEAR, P_BRAND1 order by D_YEAR, P_BRAND1; \n"},
        new Object[]{"select sum(CAST(LO_REVENUE AS DOUBLE)), D_YEAR, P_BRAND1 "
            + " from lineorder, dates, part, supplier where LO_ORDERDATE = D_DATEKEY and LO_PARTKEY = P_PARTKEY "
            + " and LO_SUPPKEY = S_SUPPKEY and P_BRAND1 between 'MFGR#2221' and 'MFGR#2228' and S_REGION = 'ASIA' "
            + " group by D_YEAR, P_BRAND1 order by D_YEAR, P_BRAND1; \n"},
        new Object[]{"select sum(CAST(LO_REVENUE AS DOUBLE)), D_YEAR, P_BRAND1 "
            + " from lineorder, dates, part, supplier where LO_ORDERDATE = D_DATEKEY and LO_PARTKEY = P_PARTKEY "
            + " and LO_SUPPKEY = S_SUPPKEY and P_BRAND1 = 'MFGR#2221' and S_REGION = 'EUROPE' "
            + " group by D_YEAR, P_BRAND1 order by D_YEAR, P_BRAND1\n"},
        new Object[]{"select C_NATION, S_NATION, D_YEAR, sum(LO_REVENUE) as revenue "
            + " from customer, lineorder, supplier, dates where LO_CUSTKEY = C_CUSTKEY and LO_SUPPKEY = S_SUPPKEY "
            + " and LO_ORDERDATE = D_DATEKEY and C_REGION = 'ASIA' and S_REGION = 'ASIA' and D_YEAR >= 1992 "
            + " and D_YEAR <= 1997 "
            + " group by C_NATION, S_NATION, D_YEAR order by D_YEAR asc, revenue desc; \n"},
        new Object[]{"select C_CITY, S_CITY, D_YEAR, sum(LO_REVENUE) as revenue "
            + " from customer, lineorder, supplier, dates where LO_CUSTKEY = C_CUSTKEY and LO_SUPPKEY = S_SUPPKEY "
            + " and LO_ORDERDATE = D_DATEKEY and C_NATION = 'UNITED STATES' and S_NATION = 'UNITED STATES' "
            + " and D_YEAR >= 1992 and D_YEAR <= 1997 "
            + " group by C_CITY, S_CITY, D_YEAR order by D_YEAR asc, revenue desc; \n"},
        new Object[]{"select C_CITY, S_CITY, D_YEAR, sum(LO_REVENUE) as revenue "
            + " from customer, lineorder, supplier, dates where LO_CUSTKEY = C_CUSTKEY and LO_SUPPKEY = S_SUPPKEY "
            + " and LO_ORDERDATE = D_DATEKEY and (C_CITY='UNITED KI1' or C_CITY='UNITED KI5') "
            + " and (S_CITY='UNITED KI1' or S_CITY='UNITED KI5') and D_YEAR >= 1992 and D_YEAR <= 1997 "
            + " group by C_CITY, S_CITY, D_YEAR order by D_YEAR asc, revenue desc;\n"},
        new Object[]{"select C_CITY, S_CITY, D_YEAR, sum(LO_REVENUE) as revenue "
            + " from customer, lineorder, supplier, dates where LO_CUSTKEY = C_CUSTKEY and LO_SUPPKEY = S_SUPPKEY "
            + " and LO_ORDERDATE = D_DATEKEY and (C_CITY='UNITED KI1' or C_CITY='UNITED KI5') "
            + " and (S_CITY='UNITED KI1' or S_CITY='UNITED KI5') and D_YEARMONTH = 'Jul1995' "
            + " group by C_CITY, S_CITY, D_YEAR order by D_YEAR asc, revenue desc; \n"},
        new Object[]{"select D_YEAR, C_NATION, sum(LO_REVENUE - LO_SUPPLYCOST) as profit "
            + " from  lineorder, customer, supplier, part,  dates where LO_CUSTKEY = C_CUSTKEY "
            + " and LO_SUPPKEY = S_SUPPKEY and LO_PARTKEY = P_PARTKEY and LO_ORDERDATE = D_DATEKEY "
            + " and C_REGION = 'AMERICA' and S_REGION = 'AMERICA' and (P_MFGR = 'MFGR#1' or P_MFGR = 'MFGR#2') "
            + " group by D_YEAR, C_NATION order by D_YEAR, C_NATION\n"},
        new Object[]{"select D_YEAR, S_NATION, P_CATEGORY, sum(LO_REVENUE - LO_SUPPLYCOST) as profit "
            + " from lineorder, dates, customer, supplier, part where LO_CUSTKEY = C_CUSTKEY "
            + " and LO_SUPPKEY = S_SUPPKEY and LO_PARTKEY = P_PARTKEY and LO_ORDERDATE = D_DATEKEY "
            + " and C_REGION = 'AMERICA' and S_REGION = 'AMERICA' and (D_YEAR = 1997 or D_YEAR = 1998) "
            + " and (P_MFGR = 'MFGR#1' or P_MFGR = 'MFGR#2') "
            + " group by D_YEAR, S_NATION, P_CATEGORY order by D_YEAR, S_NATION, P_CATEGORY\n"},
        new Object[]{"select D_YEAR, S_CITY, P_BRAND1, sum(LO_REVENUE - LO_SUPPLYCOST) as profit "
            + " from lineorder, dates, customer, supplier, part where LO_CUSTKEY = C_CUSTKEY "
            + " and LO_SUPPKEY = S_SUPPKEY and LO_PARTKEY = P_PARTKEY and LO_ORDERDATE = D_DATEKEY "
            + " and C_REGION = 'AMERICA' and S_NATION = 'UNITED STATES' and (D_YEAR = 1997 or D_YEAR = 1998) "
            + " and P_CATEGORY = 'MFGR#14' "
            + " group by D_YEAR, S_CITY, P_BRAND1 order by D_YEAR, S_CITY, P_BRAND1 \n"},
    };
  }
}
