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
package org.apache.pinot.query.runtime.queries;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.pinot.common.datatable.DataTableFactory;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.QueryServerEnclosure;
import org.apache.pinot.query.mailbox.GrpcMailboxService;
import org.apache.pinot.query.routing.WorkerInstance;
import org.apache.pinot.query.runtime.QueryRunnerTestBase;
import org.apache.pinot.query.service.QueryConfig;
import org.apache.pinot.query.testutils.MockInstanceDataManagerFactory;
import org.apache.pinot.query.testutils.QueryTestUtils;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class ResourceBasedQueriesTest extends QueryRunnerTestBase {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Pattern TABLE_NAME_REPLACE_PATTERN = Pattern.compile("\\{([\\w\\d]+)\\}");
  private static final String QUERY_TEST_RESOURCE_FOLDER = "queries";

  @BeforeClass
  public void setUp()
      throws Exception {
    DataTableBuilderFactory.setDataTableVersion(DataTableFactory.VERSION_4);
    // Setting up mock server factories.
    MockInstanceDataManagerFactory factory1 = new MockInstanceDataManagerFactory("server1");
    MockInstanceDataManagerFactory factory2 = new MockInstanceDataManagerFactory("server2");
    // Setting up H2 for validation
    setH2Connection();

    // TODO: all test data are loaded upfront b/c the mock server and brokers needs to be in sync.
    // doing it dynamically should be our next step.

    // Scan through all the test cases.
    for (Map.Entry<String, QueryTestCase> testCaseEntry : getTestCases().entrySet()) {
      String testCaseName = testCaseEntry.getKey();
      QueryTestCase testCase = testCaseEntry.getValue();
      if (testCase._ignored) {
        continue;
      }

      // table will be registered on both servers.
      Map<String, Schema> schemaMap = new HashMap<>();
      for (Map.Entry<String, QueryTestCase.Table> tableEntry : testCase._tables.entrySet()) {
        String tableName = testCaseName + "_" + tableEntry.getKey();
        // TODO: able to choose table type, now default to OFFLINE
        String tableNameWithType = TableNameBuilder.forType(TableType.OFFLINE).tableNameWithType(tableName);
        org.apache.pinot.spi.data.Schema pinotSchema = constructSchema(tableName, tableEntry.getValue()._schema);
        schemaMap.put(tableName, pinotSchema);
        factory1.registerTable(pinotSchema, tableNameWithType);
        factory2.registerTable(pinotSchema, tableNameWithType);
        List<QueryTestCase.ColumnAndType> columnAndTypes = tableEntry.getValue()._schema;
        // TODO: able to select add rows to server1 or server2 (now default server1)
        // TODO: able to select add rows to existing segment or create new one (now default create one segment)
        factory1.addSegment(tableNameWithType, toRow(columnAndTypes, tableEntry.getValue()._inputs));
      }

      boolean anyHaveOutput = testCase._queries.stream().anyMatch(q -> q._outputs != null && !q._outputs.isEmpty());

      if (anyHaveOutput) {
        boolean allHaveOutput = testCase._queries.stream().allMatch(q -> q._outputs != null && !q._outputs.isEmpty());
        if (!allHaveOutput) {
          throw new IllegalArgumentException("Cannot support one test where some queries require H2 and others don't");
        }
      }

      if (!anyHaveOutput) {
        // Add all test cases without explicit output to the tables on H2
        for (Map.Entry<String, Schema> e : schemaMap.entrySet()) {
          String tableName = e.getKey();
          Schema schema = e.getValue();
          addTableToH2(tableName, schema);
          addDataToH2(tableName, schema, factory1.buildTableRowsMap().get(tableName));
          addDataToH2(tableName, schema, factory2.buildTableRowsMap().get(tableName));
        }
      }
    }
    QueryServerEnclosure server1 = new QueryServerEnclosure(factory1);
    QueryServerEnclosure server2 = new QueryServerEnclosure(factory2);

    _reducerGrpcPort = QueryTestUtils.getAvailablePort();
    _reducerHostname = String.format("Broker_%s", QueryConfig.DEFAULT_QUERY_RUNNER_HOSTNAME);
    Map<String, Object> reducerConfig = new HashMap<>();
    reducerConfig.put(QueryConfig.KEY_OF_QUERY_RUNNER_PORT, _reducerGrpcPort);
    reducerConfig.put(QueryConfig.KEY_OF_QUERY_RUNNER_HOSTNAME, _reducerHostname);
    _mailboxService = new GrpcMailboxService(_reducerHostname, _reducerGrpcPort, new PinotConfiguration(reducerConfig),
        ignored -> { });
    _mailboxService.start();

    _queryEnvironment = QueryEnvironmentTestBase.getQueryEnvironment(_reducerGrpcPort, server1.getPort(),
        server2.getPort(), factory1.buildSchemaMap(), factory1.buildTableSegmentNameMap(),
        factory2.buildTableSegmentNameMap());
    server1.start();
    server2.start();
    // this doesn't test the QueryServer functionality so the server port can be the same as the mailbox port.
    // this is only use for test identifier purpose.
    int port1 = server1.getPort();
    int port2 = server2.getPort();
    _servers.put(new WorkerInstance("localhost", port1, port1, port1, port1), server1);
    _servers.put(new WorkerInstance("localhost", port2, port2, port2, port2), server2);
  }

  @AfterClass
  public void tearDown() {
    DataTableBuilderFactory.setDataTableVersion(DataTableBuilderFactory.DEFAULT_VERSION);
    for (QueryServerEnclosure server : _servers.values()) {
      server.shutDown();
    }
    _mailboxService.shutdown();
  }

  // TODO: name the test using testCaseName for testng reports
  @Test(dataProvider = "testResourceQueryTestCaseProviderInputOnly")
  public void testQueryTestCasesWithH2(String testCaseName, String sql, String expect)
      throws Exception {
    // query pinot
    runQuery(sql, expect).ifPresent(rows -> {
      try {
        compareRowEquals(rows, queryH2(sql));
      } catch (Exception e) {
        Assert.fail(e.getMessage(), e);
      }
    });
  }

  @Test(dataProvider = "testResourceQueryTestCaseProviderBoth")
  public void testQueryTestCasesWithOutput(String testCaseName, String sql, List<Object[]> expectedRows, String expect)
      throws Exception {
    runQuery(sql, expect).ifPresent(rows -> compareRowEquals(rows, expectedRows));
  }

  private Optional<List<Object[]>> runQuery(String sql, final String except) {
    try {
      // query pinot
      List<Object[]> resultRows = queryRunner(sql);

      Assert.assertNull(except,
        "Expected error with message '" + except + "'. But instead rows were returned: "
            + resultRows.stream().map(Arrays::toString).collect(Collectors.joining(",\n")));

      return Optional.of(resultRows);
    } catch (Exception e) {
      if (except == null) {
        throw e;
      } else {
        Pattern pattern = Pattern.compile(except);
        Assert.assertTrue(pattern.matcher(e.getMessage()).matches(),
            String.format("Caught exception '%s', but it did not match the expected pattern '%s'.",
                e.getMessage(), except));
      }
    }

    return Optional.empty();
  }

  @DataProvider
  private static Object[][] testResourceQueryTestCaseProviderBoth()
      throws Exception {
    Map<String, QueryTestCase> testCaseMap = getTestCases();
    List<Object[]> providerContent = new ArrayList<>();
    for (Map.Entry<String, QueryTestCase> testCaseEntry : testCaseMap.entrySet()) {
      String testCaseName = testCaseEntry.getKey();
      if (testCaseEntry.getValue()._ignored) {
        continue;
      }

      List<QueryTestCase.Query> queryCases = testCaseEntry.getValue()._queries;
      for (QueryTestCase.Query queryCase : queryCases) {
        if (queryCase._ignored) {
          continue;
        }

        if (queryCase._outputs != null && !queryCase._outputs.isEmpty()) {
          String sql = replaceTableName(testCaseName, queryCase._sql);
          List<List<Object>> orgRows = queryCase._outputs;
          List<Object[]> expectedRows = new ArrayList<>(orgRows.size());
          for (List<Object> objs : orgRows) {
            expectedRows.add(objs.toArray());
          }
          Object[] testEntry = new Object[]{testCaseName, sql, expectedRows, queryCase._expectedException};
          providerContent.add(testEntry);
        }
      }
    }
    return providerContent.toArray(new Object[][]{});
  }

  @DataProvider
  private static Object[][] testResourceQueryTestCaseProviderInputOnly()
      throws Exception {
    Map<String, QueryTestCase> testCaseMap = getTestCases();
    List<Object[]> providerContent = new ArrayList<>();
    for (Map.Entry<String, QueryTestCase> testCaseEntry : testCaseMap.entrySet()) {
      if (testCaseEntry.getValue()._ignored) {
        continue;
      }

      String testCaseName = testCaseEntry.getKey();
      List<QueryTestCase.Query> queryCases = testCaseEntry.getValue()._queries;
      for (QueryTestCase.Query queryCase : queryCases) {
        if (queryCase._ignored) {
          continue;
        }
        if (queryCase._outputs == null || queryCase._outputs.isEmpty()) {
          String sql = replaceTableName(testCaseName, queryCase._sql);
          Object[] testEntry = new Object[]{testCaseName, sql, queryCase._expectedException};
          providerContent.add(testEntry);
        }
      }
    }
    return providerContent.toArray(new Object[][]{});
  }

  private static String replaceTableName(String testCaseName, String sql) {
    Matcher matcher = TABLE_NAME_REPLACE_PATTERN.matcher(sql);
    return matcher.replaceAll(testCaseName + "_$1");
  }

  // TODO: cache this test case generator
  private static Map<String, QueryTestCase> getTestCases()
      throws Exception {
    Map<String, QueryTestCase> testCaseMap = new HashMap<>();
    ClassLoader classLoader = ResourceBasedQueriesTest.class.getClassLoader();
    // Get all test files.
    List<String> testFilenames = new ArrayList<>();
    try (InputStream in = classLoader.getResourceAsStream(QUERY_TEST_RESOURCE_FOLDER);
        BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
      String resource;
      while ((resource = br.readLine()) != null) {
        testFilenames.add(resource);
      }
    }
    // Load each test file.
    for (String testCaseName : testFilenames) {
      String testCaseFile = QUERY_TEST_RESOURCE_FOLDER + File.separator + testCaseName;
      URL testFileUrl = classLoader.getResource(testCaseFile);
      // This test only supports local resource loading (e.g. must be a file), not support JAR test loading.
      if (testFileUrl != null && new File(testFileUrl.getFile()).exists()) {
        Map<String, QueryTestCase> testCases = MAPPER.readValue(new File(testFileUrl.getFile()),
            new TypeReference<Map<String, QueryTestCase>>() { });
        {
          HashSet<String> hashSet = new HashSet<>(testCaseMap.keySet());
          hashSet.retainAll(testCases.keySet());
          if (!hashSet.isEmpty()) {
            throw new IllegalArgumentException("testCase already exist for the following names: " + hashSet);
          }
        }
        testCaseMap.putAll(testCases);
      }
    }
    return testCaseMap;
  }
}
