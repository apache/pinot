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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.response.broker.BrokerResponseNativeV2;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.QueryServerEnclosure;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.runtime.MultiStageStatsTreeBuilder;
import org.apache.pinot.query.runtime.operator.LeafOperator;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.service.dispatch.QueryDispatcher;
import org.apache.pinot.query.testutils.MockInstanceDataManagerFactory;
import org.apache.pinot.query.testutils.QueryTestUtils;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class ResourceBasedQueriesTest extends QueryRunnerTestBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(ResourceBasedQueriesTest.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Pattern TABLE_NAME_REPLACE_PATTERN = Pattern.compile("\\{([\\w\\d]+)\\}");
  private static final String QUERY_TEST_RESOURCE_FOLDER = "queries";
  private static final String FILE_FILTER_PROPERTY = "pinot.fileFilter";
  private static final String IGNORE_FILTER_PROPERTY = "pinot.runIgnored";
  private static final int DEFAULT_NUM_PARTITIONS = 4;

  private final Map<String, Set<String>> _tableToSegmentMap = new HashMap<>();
  private boolean _isRunIgnored;
  private TimeZone _currentSystemTimeZone;

  @BeforeClass
  public void setUp()
      throws Exception {
    // Save the original default timezone
    _currentSystemTimeZone = TimeZone.getDefault();

    String runIgnoredProp = System.getProperty(IGNORE_FILTER_PROPERTY);
    _isRunIgnored = runIgnoredProp != null;

    // Change the default timezone
    TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));

    // Setting up mock server factories.
    // All test data are loaded upfront b/c the mock server and brokers needs to be in sync.
    MockInstanceDataManagerFactory factory1 = new MockInstanceDataManagerFactory("server1");
    MockInstanceDataManagerFactory factory2 = new MockInstanceDataManagerFactory("server2");
    // Setting up H2 for validation
    setH2Connection();

    // Scan through all the test cases.
    Map<String, Pair<String, List<List<String>>>> partitionedSegmentsMap = new HashMap<>();
    for (Map.Entry<String, QueryTestCase> testCaseEntry : getTestCases().entrySet()) {
      String testCaseName = testCaseEntry.getKey();
      QueryTestCase testCase = testCaseEntry.getValue();
      if (testCase._ignored) {
        continue;
      }

      // table will be registered on both servers.
      Map<String, Schema> schemaMap = new HashMap<>();
      for (Map.Entry<String, QueryTestCase.Table> entry : testCase._tables.entrySet()) {
        boolean allowEmptySegment = !testCase._extraProps.isNoEmptySegment();
        String tableName = testCaseName + "_" + entry.getKey();
        // Testing only OFFLINE table b/c Hybrid table test is a special case to test separately.
        String offlineTableName = TableNameBuilder.forType(TableType.OFFLINE).tableNameWithType(tableName);
        QueryTestCase.Table table = entry.getValue();
        Schema schema = constructSchema(tableName, table._schema);
        schema.setEnableColumnBasedNullHandling(testCase._extraProps.isEnableColumnBasedNullHandling());
        schemaMap.put(tableName, schema);
        factory1.registerTable(schema, offlineTableName);
        factory2.registerTable(schema, offlineTableName);
        List<QueryTestCase.ColumnAndType> columnAndTypes = table._schema;
        List<GenericRow> genericRows = toRow(columnAndTypes, table._inputs);
        if (table._replicated) {
          addSegmentReplicated(factory1, factory2, offlineTableName, genericRows);
          continue;
        }
        // generate segments and dump into server1 and server2
        List<String> partitionColumns = table._partitionColumns;
        int numPartitions = table._partitionCount == null ? DEFAULT_NUM_PARTITIONS : table._partitionCount;
        String partitionColumn = null;
        List<List<String>> partitionIdToSegmentsMap = null;
        if (partitionColumns != null && partitionColumns.size() == 1) {
          partitionColumn = partitionColumns.get(0);
          partitionIdToSegmentsMap = new ArrayList<>();
          for (int i = 0; i < numPartitions; i++) {
            partitionIdToSegmentsMap.add(new ArrayList<>());
          }
        }

        List<List<GenericRow>> partitionIdToRowsMap = new ArrayList<>();
        for (int i = 0; i < numPartitions; i++) {
          partitionIdToRowsMap.add(new ArrayList<>());
        }

        int numRows = genericRows.size();
        for (int i = 0; i < numRows; i++) {
          GenericRow row = genericRows.get(i);
          if (row == SEGMENT_BREAKER_ROW) {
            addSegments(factory1, factory2, offlineTableName, allowEmptySegment, partitionIdToRowsMap,
                partitionIdToSegmentsMap, numPartitions);
          } else {
            int partitionId;
            if (partitionColumns == null) {
              // Round-robin when there is no partition column
              partitionId = i % numPartitions;
            } else {
              int hashCode = 0;
              for (String field : partitionColumns) {
                hashCode += row.getValue(field).hashCode();
              }
              partitionId = (hashCode & Integer.MAX_VALUE) % numPartitions;
            }
            partitionIdToRowsMap.get(partitionId).add(row);
          }
        }
        addSegments(factory1, factory2, offlineTableName, allowEmptySegment, partitionIdToRowsMap,
            partitionIdToSegmentsMap, numPartitions);

        if (partitionColumn != null) {
          partitionedSegmentsMap.put(offlineTableName, Pair.of(partitionColumn, partitionIdToSegmentsMap));
        }
      }

      boolean anyHaveOutput = testCase._queries.stream().anyMatch(q -> q._outputs != null);

      if (anyHaveOutput) {
        boolean allHaveOutput = testCase._queries.stream().allMatch(q -> q._outputs != null);
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

    Map<String, List<String>> tableToSegmentMap1 = factory1.buildTableSegmentNameMap();
    Map<String, List<String>> tableToSegmentMap2 = factory2.buildTableSegmentNameMap();
    for (Map.Entry<String, List<String>> entry : tableToSegmentMap1.entrySet()) {
      _tableToSegmentMap.put(entry.getKey(), new HashSet<>(entry.getValue()));
    }
    for (Map.Entry<String, List<String>> entry : tableToSegmentMap2.entrySet()) {
      if (_tableToSegmentMap.containsKey(entry.getKey())) {
        _tableToSegmentMap.get(entry.getKey()).addAll(entry.getValue());
      } else {
        _tableToSegmentMap.put(entry.getKey(), new HashSet<>(entry.getValue()));
      }
    }

    _reducerHostname = "localhost";
    _reducerPort = QueryTestUtils.getAvailablePort();
    Map<String, Object> reducerConfig = new HashMap<>();
    reducerConfig.put(CommonConstants.MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_HOSTNAME, _reducerHostname);
    reducerConfig.put(CommonConstants.MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_PORT, _reducerPort);
    _mailboxService = new MailboxService(_reducerHostname, _reducerPort, new PinotConfiguration(reducerConfig));
    _mailboxService.start();

    QueryServerEnclosure server1 = new QueryServerEnclosure(factory1);
    QueryServerEnclosure server2 = new QueryServerEnclosure(factory2);
    server1.start();
    server2.start();
    // this doesn't test the QueryServer functionality so the server port can be the same as the mailbox port.
    // this is only use for test identifier purpose.
    int port1 = server1.getPort();
    int port2 = server2.getPort();
    _servers.put(new QueryServerInstance("Server_localhost_" + port1, "localhost", port1, port1), server1);
    _servers.put(new QueryServerInstance("Server_localhost_" + port2, "localhost", port2, port2), server2);

    _queryEnvironment = QueryEnvironmentTestBase.getQueryEnvironment(_reducerPort, server1.getPort(), server2.getPort(),
        factory1.getRegisteredSchemaMap(), factory1.buildTableSegmentNameMap(), factory2.buildTableSegmentNameMap(),
        partitionedSegmentsMap);
  }

  private void addSegments(MockInstanceDataManagerFactory factory1, MockInstanceDataManagerFactory factory2,
      String offlineTableName, boolean allowEmptySegment, List<List<GenericRow>> partitionIdToRowsMap,
      @Nullable List<List<String>> partitionIdToSegmentsMap, int numPartitions) {
    for (int i = 0; i < numPartitions; i++) {
      MockInstanceDataManagerFactory factory = i < (numPartitions / 2) ? factory1 : factory2;
      List<GenericRow> rows = partitionIdToRowsMap.get(i);
      if (allowEmptySegment || !rows.isEmpty()) {
        ImmutableSegment segment = factory.addSegment(offlineTableName, rows);
        if (partitionIdToSegmentsMap != null) {
          partitionIdToSegmentsMap.get(i).add(segment.getSegmentName());
        }
        rows.clear();
      }
    }
  }

  private void addSegmentReplicated(MockInstanceDataManagerFactory factory1, MockInstanceDataManagerFactory factory2,
      String offlineTableName, List<GenericRow> rows) {
    ImmutableSegment segment = factory1.addSegment(offlineTableName, rows);
    factory2.addSegment(offlineTableName, segment);
  }

  @AfterClass
  public void tearDown() {
    // Restore the original default timezone
    TimeZone.setDefault(_currentSystemTimeZone);
    for (QueryServerEnclosure server : _servers.values()) {
      server.shutDown();
    }
    _mailboxService.shutdown();
  }

  // TODO: name the test using testCaseName for testng reports
  @Test(dataProvider = "testResourceQueryTestCaseProviderInputOnly")
  public void testQueryTestCasesWithH2(String testCaseName, boolean isIgnored, String sql, String h2Sql, String expect,
      boolean keepOutputRowOrder, boolean ignoreV2Optimizer)
      throws Exception {
    // query pinot
    runQuery(sql, expect, false).ifPresent(queryResult -> {
      try {
        compareRowEquals(queryResult.getResultTable(), queryH2(h2Sql), keepOutputRowOrder);
      } catch (Exception e) {
        Assert.fail(e.getMessage(), e);
      }
    });
  }

  // TODO: name the test using testCaseName for testng reports
  @Test(dataProvider = "testResourceQueryTestCaseProviderInputOnly")
  public void testQueryTestCasesWithH2WithNewOptimizer(String testCaseName, boolean isIgnored, String sql, String h2Sql,
      String expect, boolean keepOutputRowOrder, boolean ignoreV2Optimizer)
      throws Exception {
    // query pinot
    if (ignoreV2Optimizer) {
      LOGGER.warn("Ignoring query for test-case ({}): with v2 optimizer: {}", testCaseName, sql);
      return;
    }
    sql = String.format("SET usePhysicalOptimizer=true; %s", sql);
    runQuery(sql, expect, false).ifPresent(queryResult -> {
      try {
        compareRowEquals(queryResult.getResultTable(), queryH2(h2Sql), keepOutputRowOrder);
      } catch (Exception e) {
        Assert.fail(e.getMessage(), e);
      }
    });
  }

  @Test(dataProvider = "testResourceQueryTestCaseProviderBoth")
  public void testQueryTestCasesWithOutput(String testCaseName, boolean isIgnored, String sql, String h2Sql,
      List<Object[]> expectedRows, String expect, boolean keepOutputRowOrder)
      throws Exception {
    runQuery(sql, expect, false).ifPresent(
        queryResult -> compareRowEquals(queryResult.getResultTable(), expectedRows, keepOutputRowOrder));
  }

  @Test(dataProvider = "testResourceQueryTestCaseProviderBoth")
  public void testQueryTestCasesWithNewOptimizerWithOutput(String testCaseName, boolean isIgnored, String sql,
      String h2Sql, List<Object[]> expectedRows, String expect, boolean keepOutputRowOrder)
      throws Exception {
    final String finalSql = String.format("SET usePhysicalOptimizer=true; %s", sql);
    runQuery(finalSql, expect, false).ifPresent(
        queryResult -> compareRowEquals(queryResult.getResultTable(), expectedRows, keepOutputRowOrder));
  }

  @Test(dataProvider = "testResourceQueryTestCaseProviderBoth")
  public void testQueryTestCasesWithLiteModeWithOutput(String testCaseName, boolean isIgnored, String sql,
      String h2Sql, List<Object[]> expectedRows, String expect, boolean keepOutputRowOrder)
      throws Exception {
    final String finalSql = String.format("SET usePhysicalOptimizer=true; SET useLiteMode=true; %s", sql);
    runQuery(finalSql, expect, false).ifPresent(
        queryResult -> compareRowEquals(queryResult.getResultTable(), expectedRows, keepOutputRowOrder));
  }

  private Map<String, JsonNode> tableToStats(String sql, QueryDispatcher.QueryResult queryResult) {

    Map<Integer, DispatchablePlanFragment> planNodes = planQuery(sql).getQueryPlan().getQueryStageMap();

    MultiStageStatsTreeBuilder multiStageStatsTreeBuilder =
        new MultiStageStatsTreeBuilder(planNodes, queryResult.getQueryStats());
    ObjectNode jsonNodes = multiStageStatsTreeBuilder.jsonStatsByStage(1);

    Map<String, JsonNode> map = new HashMap<>();
    tableToStatsRec(map, jsonNodes);
    return map;
  }

  private void tableToStatsRec(Map<String, JsonNode> map, ObjectNode node) {
    JsonNode type = node.get("type");
    if (type == null || !type.equals("LEAF")) {
      return;
    }
    String tableName = node.get("table").asText();
    JsonNode old = map.put(tableName, node);
    if (old != null) {
      throw new RuntimeException("Found at least two leaf stages for table " + tableName);
    }
    JsonNode children = node.get("children");
    if (children != null) {
      for (JsonNode child : children) {
        tableToStatsRec(map, (ObjectNode) child);
      }
    }
  }

  @Test(dataProvider = "testResourceQueryTestCaseProviderWithMetadata")
  public void testQueryTestCasesWithMetadata(String testCaseName, boolean isIgnored, String sql, String h2Sql,
      String expect, int numSegments)
      throws Exception {
    runQuery(sql, expect, true).ifPresent(queryResult -> {
      BrokerResponseNativeV2 brokerResponseNative = new BrokerResponseNativeV2();
      for (MultiStageQueryStats.StageStats.Closed stageStats : queryResult.getQueryStats()) {
        stageStats.forEach((type, stats) -> type.mergeInto(brokerResponseNative, stats));
      }

      Assert.assertEquals(brokerResponseNative.getNumSegmentsQueried(), numSegments);

      Map<String, JsonNode> tableToStats = tableToStats(sql, queryResult);
      for (Map.Entry<String, JsonNode> entry : tableToStats.entrySet()) {
        String tableName = entry.getKey();
        TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
        if (tableType == null) {
          tableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
        }

        Assert.assertNotNull(_tableToSegmentMap.get(tableName));
        String statName = LeafOperator.StatKey.NUM_SEGMENTS_QUERIED.getStatName();
        int numSegmentsQueried = entry.getValue().get(statName).asInt();
        Assert.assertEquals(numSegmentsQueried, _tableToSegmentMap.get(tableName).size());
      }
    });
  }

  private Optional<QueryDispatcher.QueryResult> runQuery(String sql, final String except, boolean trace)
      throws Exception {
    try {
      // query pinot
      QueryDispatcher.QueryResult queryResult = queryRunner(sql, trace);
      Assert.assertNull(except, "Expected error with message '" + except + "'. But instead rows were returned: "
          + JsonUtils.objectToPrettyString(queryResult.getResultTable()));
      return Optional.of(queryResult);
    } catch (Exception e) {
      if (except == null) {
        throw e;
      } else {
        Pattern pattern = Pattern.compile(except, Pattern.DOTALL);
        Assert.assertTrue(pattern.matcher(e.getMessage()).matches(),
            String.format("Caught exception '%s', but it did not match the expected pattern '%s'.", e.getMessage(),
                except));
      }
    }

    return Optional.empty();
  }

  @DataProvider
  private Object[][] testResourceQueryTestCaseProviderBoth()
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
        if (queryCase._ignored && !_isRunIgnored) {
          continue;
        }

        if (queryCase._outputs != null) {
          String sql = replaceTableName(testCaseName, queryCase._sql);
          String h2Sql = queryCase._h2Sql != null ? replaceTableName(testCaseName, queryCase._h2Sql)
              : replaceTableName(testCaseName, queryCase._sql);
          List<List<Object>> orgRows = queryCase._outputs;
          List<Object[]> expectedRows = new ArrayList<>(orgRows.size());
          for (List<Object> objs : orgRows) {
            expectedRows.add(objs.toArray());
          }
          Object[] testEntry = new Object[]{
              testCaseName, queryCase._ignored, sql, h2Sql, expectedRows, queryCase._expectedException,
              queryCase._keepOutputRowOrder
          };
          providerContent.add(testEntry);
        }
      }
    }
    return providerContent.toArray(new Object[][]{});
  }

  @DataProvider
  private Object[][] testResourceQueryTestCaseProviderWithMetadata()
      throws Exception {
    Map<String, QueryTestCase> testCaseMap = getTestCases();
    List<Object[]> providerContent = new ArrayList<>();
    Set<String> validTestCases = new HashSet<>();
    validTestCases.add("metadata_test");

    for (Map.Entry<String, QueryTestCase> testCaseEntry : testCaseMap.entrySet()) {
      String testCaseName = testCaseEntry.getKey();
      if (!validTestCases.contains(testCaseName)) {
        continue;
      }

      if (testCaseEntry.getValue()._ignored) {
        continue;
      }

      List<QueryTestCase.Query> queryCases = testCaseEntry.getValue()._queries;
      for (QueryTestCase.Query queryCase : queryCases) {

        if (queryCase._ignored && !_isRunIgnored) {
          continue;
        }

        if (queryCase._outputs == null) {
          String sql = replaceTableName(testCaseName, queryCase._sql);
          String h2Sql = queryCase._h2Sql != null ? replaceTableName(testCaseName, queryCase._h2Sql)
              : replaceTableName(testCaseName, queryCase._sql);

          int segmentCount = 0;
          if (queryCase._expectedNumSegments != null) {
            segmentCount = queryCase._expectedNumSegments;
          } else {
            throw new RuntimeException("Unable to test metadata without expected num segments configuration!");
          }

          Object[] testEntry = new Object[]{
              testCaseName, queryCase._ignored, sql, h2Sql, queryCase._expectedException, segmentCount
          };
          providerContent.add(testEntry);
        }
      }
    }
    return providerContent.toArray(new Object[][]{});
  }

  @DataProvider
  private Object[][] testResourceQueryTestCaseProviderInputOnly()
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
        if (queryCase._ignored && !_isRunIgnored) {
          continue;
        }
        if (queryCase._outputs == null) {
          String sql = replaceTableName(testCaseName, queryCase._sql);
          String h2Sql = queryCase._h2Sql != null ? replaceTableName(testCaseName, queryCase._h2Sql)
              : replaceTableName(testCaseName, queryCase._sql);
          Object[] testEntry = new Object[]{
              testCaseName, queryCase._ignored, sql, h2Sql, queryCase._expectedException, queryCase._keepOutputRowOrder,
              queryCase._ignoreV2Optimizer
          };
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
  private Map<String, QueryTestCase> getTestCases()
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

    // get filter if set
    String fileFilterProp = System.getProperty(FILE_FILTER_PROPERTY);

    // Load each test file.
    for (String testCaseName : testFilenames) {
      if (fileFilterProp != null && !testCaseName.toLowerCase().contains(fileFilterProp.toLowerCase())) {
        continue;
      }

      String testCaseFile = QUERY_TEST_RESOURCE_FOLDER + File.separator + testCaseName;
      URL testFileUrl = classLoader.getResource(testCaseFile);
      // This test only supports local resource loading (e.g. must be a file), not support JAR test loading.
      if (testFileUrl != null && new File(testFileUrl.getFile()).exists()) {
        Map<String, QueryTestCase> testCases =
            MAPPER.readValue(new File(testFileUrl.getFile()), new TypeReference<Map<String, QueryTestCase>>() {
            });
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

  private static Object extractExtraProps(Map<String, Object> extraProps, String propKey) {
    if (extraProps == null) {
      return null;
    }
    return extraProps.getOrDefault(propKey, null);
  }
}
