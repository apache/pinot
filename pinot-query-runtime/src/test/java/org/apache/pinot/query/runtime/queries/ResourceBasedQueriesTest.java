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
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.response.broker.BrokerResponseNativeV2;
import org.apache.pinot.common.response.broker.BrokerResponseStats;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.core.query.reduce.ExecutionStatsAggregator;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.QueryServerEnclosure;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.runtime.QueryRunnerTestBase;
import org.apache.pinot.query.runtime.executor.OpChainSchedulerService;
import org.apache.pinot.query.service.QueryConfig;
import org.apache.pinot.query.testutils.MockInstanceDataManagerFactory;
import org.apache.pinot.query.testutils.QueryTestUtils;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.BooleanUtils;
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
  private static final Random RANDOM = new Random(42);
  private static final String FILE_FILTER_PROPERTY = "pinot.fileFilter";
  private static final int NUM_PARTITIONS = 4;

  private final Map<String, Set<String>> _tableToSegmentMap = new HashMap<>();
  private TimeZone _currentSystemTimeZone;

  @BeforeClass
  public void setUp()
      throws Exception {
    // Save the original default timezone
    _currentSystemTimeZone = TimeZone.getDefault();

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
      for (Map.Entry<String, QueryTestCase.Table> tableEntry : testCase._tables.entrySet()) {
        boolean allowEmptySegment = !BooleanUtils.toBoolean(extractExtraProps(testCase._extraProps, "noEmptySegment"));
        String tableName = testCaseName + "_" + tableEntry.getKey();
        // Testing only OFFLINE table b/c Hybrid table test is a special case to test separately.
        String offlineTableName = TableNameBuilder.forType(TableType.OFFLINE).tableNameWithType(tableName);
        Schema pinotSchema = constructSchema(tableName, tableEntry.getValue()._schema);
        schemaMap.put(tableName, pinotSchema);
        factory1.registerTable(pinotSchema, offlineTableName);
        factory2.registerTable(pinotSchema, offlineTableName);
        List<QueryTestCase.ColumnAndType> columnAndTypes = tableEntry.getValue()._schema;
        List<GenericRow> genericRows = toRow(columnAndTypes, tableEntry.getValue()._inputs);

        // generate segments and dump into server1 and server2
        List<String> partitionColumns = tableEntry.getValue()._partitionColumns;
        String partitionColumn = null;
        List<List<String>> partitionIdToSegmentsMap = null;
        if (partitionColumns != null && partitionColumns.size() == 1) {
          partitionColumn = partitionColumns.get(0);
          partitionIdToSegmentsMap = new ArrayList<>();
          for (int i = 0; i < NUM_PARTITIONS; i++) {
            partitionIdToSegmentsMap.add(new ArrayList<>());
          }
        }

        List<List<GenericRow>> partitionIdToRowsMap = new ArrayList<>();
        for (int i = 0; i < NUM_PARTITIONS; i++) {
          partitionIdToRowsMap.add(new ArrayList<>());
        }

        for (GenericRow row : genericRows) {
          if (row == SEGMENT_BREAKER_ROW) {
            addSegments(factory1, factory2, offlineTableName, allowEmptySegment, partitionIdToRowsMap,
                partitionIdToSegmentsMap);
          } else {
            int partitionId;
            if (partitionColumns == null) {
              partitionId = RANDOM.nextInt(NUM_PARTITIONS);
            } else {
              int hashCode = 0;
              for (String field : partitionColumns) {
                hashCode += row.getValue(field).hashCode();
              }
              partitionId = (hashCode & Integer.MAX_VALUE) % NUM_PARTITIONS;
            }
            partitionIdToRowsMap.get(partitionId).add(row);
          }
        }
        addSegments(factory1, factory2, offlineTableName, allowEmptySegment, partitionIdToRowsMap,
            partitionIdToSegmentsMap);

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
    QueryServerEnclosure server1 = new QueryServerEnclosure(factory1);
    QueryServerEnclosure server2 = new QueryServerEnclosure(factory2);

    _reducerGrpcPort = QueryTestUtils.getAvailablePort();
    _reducerHostname = String.format("Broker_%s", QueryConfig.DEFAULT_QUERY_RUNNER_HOSTNAME);
    Map<String, Object> reducerConfig = new HashMap<>();
    reducerConfig.put(QueryConfig.KEY_OF_QUERY_RUNNER_PORT, _reducerGrpcPort);
    reducerConfig.put(QueryConfig.KEY_OF_QUERY_RUNNER_HOSTNAME, _reducerHostname);
    _reducerScheduler = new OpChainSchedulerService(EXECUTOR);
    _mailboxService = new MailboxService(QueryConfig.DEFAULT_QUERY_RUNNER_HOSTNAME, _reducerGrpcPort,
        new PinotConfiguration(reducerConfig), ignoreMe -> {
    });
    _mailboxService.start();

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

    _queryEnvironment =
        QueryEnvironmentTestBase.getQueryEnvironment(_reducerGrpcPort, server1.getPort(), server2.getPort(),
            factory1.getRegisteredSchemaMap(), factory1.buildTableSegmentNameMap(), factory2.buildTableSegmentNameMap(),
            partitionedSegmentsMap);
    server1.start();
    server2.start();
    // this doesn't test the QueryServer functionality so the server port can be the same as the mailbox port.
    // this is only use for test identifier purpose.
    int port1 = server1.getPort();
    int port2 = server2.getPort();
    _servers.put(new QueryServerInstance("localhost", port1, port1), server1);
    _servers.put(new QueryServerInstance("localhost", port2, port2), server2);
  }

  private void addSegments(MockInstanceDataManagerFactory factory1, MockInstanceDataManagerFactory factory2,
      String offlineTableName, boolean allowEmptySegment, List<List<GenericRow>> partitionIdToRowsMap,
      @Nullable List<List<String>> partitionIdToSegmentsMap) {
    for (int i = 0; i < NUM_PARTITIONS; i++) {
      MockInstanceDataManagerFactory factory = i < (NUM_PARTITIONS / 2) ? factory1 : factory2;
      List<GenericRow> rows = partitionIdToRowsMap.get(i);
      if (allowEmptySegment || !rows.isEmpty()) {
        String segmentName = factory.addSegment(offlineTableName, rows);
        if (partitionIdToSegmentsMap != null) {
          partitionIdToSegmentsMap.get(i).add(segmentName);
        }
        rows.clear();
      }
    }
  }

  @AfterClass
  public void tearDown() {
    // Restore the original default timezone
    TimeZone.setDefault(_currentSystemTimeZone);
    DataTableBuilderFactory.setDataTableVersion(DataTableBuilderFactory.DEFAULT_VERSION);
    for (QueryServerEnclosure server : _servers.values()) {
      server.shutDown();
    }
    _mailboxService.shutdown();
  }

  // TODO: name the test using testCaseName for testng reports
  @Test(dataProvider = "testResourceQueryTestCaseProviderInputOnly")
  public void testQueryTestCasesWithH2(String testCaseName, String sql, String h2Sql, String expect,
      boolean keepOutputRowOrder)
      throws Exception {
    // query pinot
    runQuery(sql, expect, null).ifPresent(rows -> {
      try {
        compareRowEquals(rows, queryH2(h2Sql), keepOutputRowOrder);
      } catch (Exception e) {
        Assert.fail(e.getMessage(), e);
      }
    });
  }

  @Test(dataProvider = "testResourceQueryTestCaseProviderBoth")
  public void testQueryTestCasesWithOutput(String testCaseName, String sql, String h2Sql, List<Object[]> expectedRows,
      String expect, boolean keepOutputRowOrder)
      throws Exception {
    runQuery(sql, expect, null).ifPresent(rows -> compareRowEquals(rows, expectedRows, keepOutputRowOrder));
  }

  @Test(dataProvider = "testResourceQueryTestCaseProviderWithMetadata")
  public void testQueryTestCasesWithMetadata(String testCaseName, String sql, String h2Sql, String expect,
      int numSegments)
      throws Exception {
    Map<Integer, ExecutionStatsAggregator> executionStatsAggregatorMap = new HashMap<>();
    runQuery(sql, expect, executionStatsAggregatorMap).ifPresent(rows -> {
      BrokerResponseNativeV2 brokerResponseNative = new BrokerResponseNativeV2();
      executionStatsAggregatorMap.get(0).setStats(brokerResponseNative);
      Assert.assertFalse(executionStatsAggregatorMap.isEmpty());
      for (Integer stageId : executionStatsAggregatorMap.keySet()) {
        if (stageId > 0) {
          BrokerResponseStats brokerResponseStats = new BrokerResponseStats();
          executionStatsAggregatorMap.get(stageId).setStageLevelStats(null, brokerResponseStats, null);
          brokerResponseNative.addStageStat(stageId, brokerResponseStats);
        }
      }

      Assert.assertEquals(brokerResponseNative.getNumSegmentsQueried(), numSegments);

      Map<Integer, BrokerResponseStats> stageIdStats = brokerResponseNative.getStageIdStats();
      int numTables = 0;
      for (Integer stageId : stageIdStats.keySet()) {
        // check stats only for leaf stage
        BrokerResponseStats brokerResponseStats = stageIdStats.get(stageId);

        if (brokerResponseStats.getTableNames().isEmpty()) {
          continue;
        }

        String tableName = brokerResponseStats.getTableNames().get(0);
        Assert.assertEquals(brokerResponseStats.getTableNames().size(), 1);
        numTables++;
        TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
        if (tableType == null) {
          tableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
        }

        Assert.assertNotNull(_tableToSegmentMap.get(tableName));
        Assert.assertEquals(brokerResponseStats.getNumSegmentsQueried(), _tableToSegmentMap.get(tableName).size());

        Assert.assertFalse(brokerResponseStats.getOperatorStats().isEmpty());
        Map<String, Map<String, String>> operatorStats = brokerResponseStats.getOperatorStats();
        for (Map.Entry<String, Map<String, String>> entry : operatorStats.entrySet()) {
          if (entry.getKey().contains("LEAF_STAGE")) {
            Assert.assertNotNull(entry.getValue().get(DataTable.MetadataKey.NUM_SEGMENTS_QUERIED.getName()));
          } else {
            Assert.assertNotNull(entry.getValue().get(DataTable.MetadataKey.NUM_BLOCKS.getName()));
          }
        }
      }

      Assert.assertTrue(numTables > 0);
    });
  }

  private Optional<List<Object[]>> runQuery(String sql, final String except,
      Map<Integer, ExecutionStatsAggregator> executionStatsAggregatorMap) {
    try {
      // query pinot
      List<Object[]> resultRows = queryRunner(sql, executionStatsAggregatorMap);

      Assert.assertNull(except,
          "Expected error with message '" + except + "'. But instead rows were returned: " + resultRows.stream()
              .map(Arrays::toString).collect(Collectors.joining(",\n")));

      return Optional.of(resultRows);
    } catch (Exception e) {
      if (except == null) {
        throw e;
      } else {
        Pattern pattern = Pattern.compile(except);
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
        if (queryCase._ignored) {
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
              testCaseName, sql, h2Sql, expectedRows, queryCase._expectedException, queryCase._keepOutputRowOrder
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

        if (queryCase._ignored) {
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

          Object[] testEntry = new Object[]{testCaseName, sql, h2Sql, queryCase._expectedException, segmentCount};
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
        if (queryCase._ignored) {
          continue;
        }
        if (queryCase._outputs == null) {
          String sql = replaceTableName(testCaseName, queryCase._sql);
          String h2Sql = queryCase._h2Sql != null ? replaceTableName(testCaseName, queryCase._h2Sql)
              : replaceTableName(testCaseName, queryCase._sql);
          Object[] testEntry =
              new Object[]{testCaseName, sql, h2Sql, queryCase._expectedException, queryCase._keepOutputRowOrder};
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
    String property = System.getProperty(FILE_FILTER_PROPERTY);

    // Load each test file.
    for (String testCaseName : testFilenames) {
      if (property != null && !testCaseName.toLowerCase().contains(property.toLowerCase())) {
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
