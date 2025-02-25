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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.query.QueryEnvironmentTestBase;
import org.apache.pinot.query.QueryServerEnclosure;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.testutils.MockInstanceDataManagerFactory;
import org.apache.pinot.query.testutils.QueryTestUtils;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey.*;


/**
 * all special tests that doesn't fit into {@link org.apache.pinot.query.runtime.queries.ResourceBasedQueriesTest}
 * pattern goes here.
 */
public class QueryRunnerTest extends QueryRunnerTestBase {
  //@formatter:off
  public static final Object[][] ROWS = new Object[][]{
      new Object[]{"foo", "foo", 1},
      new Object[]{"bar", "bar", 42},
      new Object[]{"alice", "alice", 1},
      new Object[]{"bob", "foo", 42},
      new Object[]{"charlie", "bar", 1}
  };
  //@formatter:on
  public static final Schema.SchemaBuilder SCHEMA_BUILDER;

  static {
    SCHEMA_BUILDER = new Schema.SchemaBuilder().addSingleValueDimension("col1", FieldSpec.DataType.STRING, "")
        .addSingleValueDimension("col2", FieldSpec.DataType.STRING, "")
        .addDateTime("ts", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:HOURS")
        .addMetric("col3", FieldSpec.DataType.INT, 0)
        .setSchemaName("defaultSchemaName")
        .setEnableColumnBasedNullHandling(true);
  }

  public static List<GenericRow> buildRows(String tableName) {
    List<GenericRow> rows = new ArrayList<>(ROWS.length);
    for (int i = 0; i < ROWS.length; i++) {
      GenericRow row = new GenericRow();
      row.putValue("col1", ROWS[i][0]);
      row.putValue("col2", ROWS[i][1]);
      row.putValue("col3", ROWS[i][2]);
      row.putValue("ts",
          TableType.OFFLINE.equals(TableNameBuilder.getTableTypeFromTableName(tableName)) ? System.currentTimeMillis()
              - TimeUnit.DAYS.toMillis(2) : System.currentTimeMillis());
      rows.add(row);
    }
    return rows;
  }

  protected Map<String, Object> getConfiguration() {
    return Collections.emptyMap();
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    MockInstanceDataManagerFactory factory1 = new MockInstanceDataManagerFactory("server1");
    factory1.registerTable(SCHEMA_BUILDER.setSchemaName("a").build(), "a_REALTIME");
    factory1.registerTable(SCHEMA_BUILDER.setSchemaName("b").build(), "b_REALTIME");
    factory1.registerTable(SCHEMA_BUILDER.setSchemaName("c").build(), "c_OFFLINE");
    factory1.registerTable(SCHEMA_BUILDER.setSchemaName("d").build(), "d");
    factory1.registerTable(SCHEMA_BUILDER.setSchemaName("tbl-escape-naming").build(), "tbl-escape-naming_OFFLINE");
    factory1.addSegment("a_REALTIME", buildRows("a_REALTIME"));
    factory1.addSegment("a_REALTIME", buildRows("a_REALTIME"));
    factory1.addSegment("b_REALTIME", buildRows("b_REALTIME"));
    factory1.addSegment("c_OFFLINE", buildRows("c_OFFLINE"));
    factory1.addSegment("d_OFFLINE", buildRows("d_OFFLINE"));
    factory1.addSegment("tbl-escape-naming_OFFLINE", buildRows("tbl-escape-naming_OFFLINE"));

    MockInstanceDataManagerFactory factory2 = new MockInstanceDataManagerFactory("server2");
    factory2.registerTable(SCHEMA_BUILDER.setSchemaName("a").build(), "a_REALTIME");
    factory2.registerTable(SCHEMA_BUILDER.setSchemaName("c").build(), "c_OFFLINE");
    factory2.registerTable(SCHEMA_BUILDER.setSchemaName("d").build(), "d");
    factory2.addSegment("a_REALTIME", buildRows("a_REALTIME"));
    factory2.addSegment("c_OFFLINE", buildRows("c_OFFLINE"));
    factory2.addSegment("c_OFFLINE", buildRows("c_OFFLINE"));
    factory2.addSegment("d_OFFLINE", buildRows("d_OFFLINE"));
    factory2.addSegment("d_REALTIME", buildRows("d_REALTIME"));

    // Setting up H2 for validation
    setH2Connection();
    Schema schema = SCHEMA_BUILDER.build();
    for (String tableName : Arrays.asList("a", "b", "c", "d")) {
      addTableToH2(tableName, schema);
      addDataToH2(tableName, schema, factory1.buildTableRowsMap().get(tableName));
      addDataToH2(tableName, schema, factory2.buildTableRowsMap().get(tableName));
    }

    _reducerHostname = "localhost";
    _reducerPort = QueryTestUtils.getAvailablePort();
    Map<String, Object> reducerConfig = new HashMap<>();
    reducerConfig.put(CommonConstants.MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_HOSTNAME, _reducerHostname);
    reducerConfig.put(CommonConstants.MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_PORT, _reducerPort);
    _mailboxService = new MailboxService(_reducerHostname, _reducerPort, new PinotConfiguration(reducerConfig));
    _mailboxService.start();

    QueryServerEnclosure server1 = new QueryServerEnclosure(factory1, getConfiguration());
    server1.start();
    // Start server1 to ensure the next server will have a different port.
    QueryServerEnclosure server2 = new QueryServerEnclosure(factory2, getConfiguration());
    server2.start();
    // this doesn't test the QueryServer functionality so the server port can be the same as the mailbox port.
    // this is only use for test identifier purpose.
    int port1 = server1.getPort();
    int port2 = server2.getPort();
    _servers.put(new QueryServerInstance("Server_localhost_" + port1, "localhost", port1, port1), server1);
    _servers.put(new QueryServerInstance("Server_localhost_" + port2, "localhost", port2, port2), server2);

    _queryEnvironment = QueryEnvironmentTestBase.getQueryEnvironment(_reducerPort, server1.getPort(), server2.getPort(),
        factory1.getRegisteredSchemaMap(), factory1.buildTableSegmentNameMap(), factory2.buildTableSegmentNameMap(),
        null);
  }

  @AfterClass
  public void tearDown() {
    for (QueryServerEnclosure server : _servers.values()) {
      server.shutDown();
    }
    _mailboxService.shutdown();
  }

  /**
   * Test compares with expected row count only.
   */
  @Test(dataProvider = "testDataWithSqlToFinalRowCount")
  public void testSqlWithFinalRowCountChecker(String sql, int expectedRows) {
    ResultTable resultTable = queryRunner(sql, false).getResultTable();
    Assert.assertEquals(resultTable.getRows().size(), expectedRows);
  }

  /**
   * Test automatically compares against H2.
   *
   * @deprecated do not add to this test set. this class will be broken down and clean up.
   *   add your test to the appropriate files in {@link org.apache.pinot.query.runtime.queries} instead.
   */
  @Test(dataProvider = "testSql")
  public void testSqlWithH2Checker(String sql)
      throws Exception {
    ResultTable resultTable = queryRunner(sql, false).getResultTable();
    // query H2 for data
    List<Object[]> expectedRows = queryH2(sql);
    compareRowEquals(resultTable, expectedRows);
  }

  /**
   * Test compares against its desired exceptions.
   */
  @Test(dataProvider = "testDataWithSqlExecutionExceptions")
  public void testSqlWithExceptionMsgChecker(String sql, String expectedError) {
    try {
      // query pinot
      ResultTable resultTable = queryRunner(sql, false).getResultTable();
      Assert.fail("Expected error with message '" + expectedError + "'. But instead rows were returned: "
          + JsonUtils.objectToPrettyString(resultTable));
    } catch (Exception e) {
      // NOTE: The actual message is (usually) something like:
      //   Received error query execution result block: {200=QueryExecutionError:
      //   Query execution error on: Server_localhost_12345
      //     java.lang.IllegalArgumentException: Illegal Json Path: $['path'] does not match document
      //   In some cases there is no prefix.
      String exceptionMessage = e.getMessage();
      boolean isFromQueryDispatcher = Pattern.compile("^Received \\d+ errors? from servers?.*", Pattern.MULTILINE)
          .matcher(exceptionMessage)
          .find();
      Assert.assertTrue(
          exceptionMessage.startsWith("Error occurred during stage submission")
              || exceptionMessage.equals(expectedError)
              || isFromQueryDispatcher,
          "Exception message didn't start with proper heading: " + exceptionMessage);
      Assert.assertTrue(exceptionMessage.contains(expectedError),
          "Exception should contain: " + expectedError + ", but found: " + exceptionMessage);
    }
  }

  @DataProvider(name = "testDataWithSqlToFinalRowCount")
  protected Object[][] provideTestSqlAndRowCount() {
    //@formatter:off
    return new Object[][]{
        // special hint test, the table is not actually partitioned by col1, thus this hint gives wrong result. but
        // b/c in order to test whether this hint produces the proper optimized plan, we are making this assumption
        new Object[]{
            "SELECT /*+ aggOptions(is_partitioned_by_group_by_keys='true') */ col1, COUNT(*) FROM a GROUP BY 1 "
                + "ORDER BY 2",
            10
        },

        // special hint test, we want to try if dynamic broadcast works for just any random table */
        new Object[]{
            "SELECT /*+ joinOptions(join_strategy='dynamic_broadcast') */ col1 FROM a WHERE a.col1 IN "
                + "(SELECT b.col2 FROM b WHERE b.col3 < 10) AND a.col3 > 0",
            9
        },

        // using join clause
        new Object[]{"SELECT * FROM a JOIN b USING (col1)", 15},

        // cannot compare with H2 w/o an ORDER BY because ordering is indeterminate
        new Object[]{"SELECT * FROM a LIMIT 2", 2},

        // test dateTrunc
        //   - on leaf stage
        new Object[]{"SELECT dateTrunc('DAY', ts) FROM a LIMIT 10", 10},
        new Object[]{"SELECT dateTrunc('DAY', CAST(col3 AS BIGINT)) FROM a LIMIT 10", 10},
        //   - on intermediate stage
        new Object[]{"SELECT dateTrunc('DAY', a.ts + b.ts) FROM a JOIN b ON a.col1 = b.col1 AND a.col2 = b.col2", 15},
        new Object[]{"SELECT dateTrunc('DAY', CAST(MAX(a.col3) AS BIGINT)) FROM a", 1},

        // ScalarFunction
        // test function can be used in predicate/leaf/intermediate stage (using regexpLike)
        new Object[]{"SELECT a.col1, b.col1 FROM a JOIN b ON a.col3 = b.col3 WHERE regexpLikeVar(a.col2, b.col1)", 9},
        new Object[]{"SELECT a.col1, b.col1 FROM a JOIN b ON a.col3 = b.col3 WHERE regexp_like_var(a.col2, b.col1)", 9},
        new Object[]{"SELECT regexpLikeVar(a.col1, b.col1) FROM a JOIN b ON a.col3 = b.col3", 39},
        new Object[]{"SELECT regexp_like_var(a.col1, b.col1) FROM a JOIN b ON a.col3 = b.col3", 39},

        // test function with @ScalarFunction annotation and alias works (using round_decimal)
        new Object[]{"SELECT roundDecimal(col3) FROM a", 15},
        new Object[]{"SELECT round_decimal(col3) FROM a", 15},
        new Object[]{"SELECT col1, roundDecimal(COUNT(*)) FROM a GROUP BY col1", 5},
        new Object[]{"SELECT col1, round_decimal(COUNT(*)) FROM a GROUP BY col1", 5},

        // test queries with special query options attached
        //   - when leaf limit is set, each server returns multiStageLeafLimit number of rows only.
        new Object[]{"SET multiStageLeafLimit = 1; SELECT * FROM a", 2},

        // test groups limit in both leaf and intermediate stage
        new Object[]{"SET numGroupsLimit = 1; SELECT col1, COUNT(*) FROM a GROUP BY col1", 1},
        new Object[]{"SET numGroupsLimit = 2; SELECT col1, COUNT(*) FROM a GROUP BY col1", 2},
        new Object[]{
            "SET numGroupsLimit = 1; "
                + "SELECT a.col2, b.col2, COUNT(*) FROM a JOIN b USING (col1) GROUP BY a.col2, b.col2",
            1
        },
        new Object[]{
            "SET numGroupsLimit = 2; "
                + "SELECT a.col2, b.col2, COUNT(*) FROM a JOIN b USING (col1) GROUP BY a.col2, b.col2",
            2
        },
        // TODO: Consider pushing down hint to the leaf stage
        new Object[]{
            "SET numGroupsLimit = 2; "
                + "SELECT /*+ aggOptions(num_groups_limit='1') */ col1, COUNT(*) FROM a GROUP BY col1",
            2
        },
        new Object[]{
            "SET numGroupsLimit = 2; "
                + "SELECT /*+ aggOptions(num_groups_limit='1') */ a.col2, b.col2, COUNT(*) FROM a JOIN b USING (col1) "
                + "GROUP BY a.col2, b.col2",
            1
        },
        new Object[]{"SELECT * FROM default.\"tbl-escape-naming\"", 5},
        new Object[]{"SELECT * FROM \"default\".\"tbl-escape-naming\"", 5}
    };
    //@formatter:on
  }

  @DataProvider(name = "testDataWithSqlExecutionExceptions")
  protected Iterator<Object[]> provideTestSqlWithExecutionException() {
    List<Object[]> testCases = new ArrayList<>();
    // Missing index
    testCases.add(new Object[]{"SELECT col1 FROM a WHERE textMatch(col1, 'f') LIMIT 10", "without text index"});
    testCases.add(new Object[]{"SELECT col1, textMatch(col1, 'f') FROM a LIMIT 10", "without text index"});

    // Query hint with dynamic broadcast pipeline breaker should return error upstream
    testCases.add(new Object[]{
        "SELECT /*+ joinOptions(join_strategy='dynamic_broadcast') */ col1 FROM a WHERE a.col1 IN "
            + "(SELECT b.col2 FROM b WHERE textMatch(col1, 'f')) AND a.col3 > 0",
        "without text index"
    });

    // Timeout exception should occur with this option:
    testCases.add(new Object[]{
        "SET timeoutMs = 1; SELECT * FROM a JOIN b ON a.col1 = b.col1 JOIN c ON a.col1 = c.col1",
        "Timeout"
    });

    // Function with incorrect argument signature should throw runtime exception when casting string to numeric
    testCases.add(new Object[]{"SELECT least(a.col2, b.col3) FROM a JOIN b ON a.col1 = b.col1", "For input string:"});

    // Scalar function that doesn't have a valid use should throw an exception on the leaf stage
    //   - predicate only functions:
    testCases.add(new Object[]{"SELECT * FROM a WHERE textMatch(col1, 'f')", "without text index"});
    testCases.add(new Object[]{"SELECT * FROM a WHERE text_match(col1, 'f')", "without text index"});
    testCases.add(new Object[]{"SELECT * FROM a WHERE textContains(col1, 'f')", "supported only on native text index"});
    testCases.add(new Object[]{
        "SELECT * FROM a WHERE text_contains(col1, 'f')",
        "supported only on native text index"}
    );

    //  - transform only functions
    testCases.add(new Object[]{"SELECT jsonExtractKey(col1, 'path') FROM a", "was expecting (JSON String"});
    testCases.add(new Object[]{"SELECT json_extract_key(col1, 'path') FROM a", "was expecting (JSON String"});

    //  - PlaceholderScalarFunction registered will throw on intermediate stage, but works on leaf stage.
    //    - checked "Illegal Json Path" as col1 is not actually a json string, but the call is correctly triggered.
    testCases.add(
        new Object[]{"SELECT CAST(jsonExtractScalar(col1, 'path', 'INT') AS INT) FROM a", "Cannot resolve JSON path"});
    //    - checked function cannot be found b/c there's no intermediate stage impl for json_extract_scalar
    testCases.add(new Object[]{
        "SELECT CAST(json_extract_scalar(a.col1, b.col2, 'INT') AS INT) FROM a JOIN b ON a.col1 = b.col1",
        "Unsupported function: JSONEXTRACTSCALAR"
    });

    // Positive int keys (only included ones that will be parsed for this query)
    for (String key : new String[]{
        MAX_EXECUTION_THREADS, NUM_GROUPS_LIMIT, MAX_INITIAL_RESULT_HOLDER_CAPACITY, MAX_STREAMING_PENDING_BLOCKS,
        MAX_ROWS_IN_JOIN
    }) {
      for (String value : new String[]{"-10000000000", "-2147483648", "-1", "0", "2147483648", "10000000000"}) {
        testCases.add(new Object[]{
            "set " + key + " = " + value + "; SELECT col1, count(*) FROM a GROUP BY col1",
            key + " must be a number between 1 and 2^31-1, got: " + value
        });
      }
    }

    return testCases.iterator();
  }
}
