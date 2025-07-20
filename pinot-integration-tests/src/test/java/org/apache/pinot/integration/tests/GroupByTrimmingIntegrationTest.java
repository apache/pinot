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
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.client.Connection;
import org.apache.pinot.client.ResultSetGroup;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.integration.tests.GroupByOptionsIntegrationTest.toExplainStr;
import static org.apache.pinot.integration.tests.GroupByOptionsIntegrationTest.toResultStr;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


// Tests that 'groupsTrimmed' flag is set when results trimming occurs at:
// SSQE - segment, inter-segment/server and broker levels
// MSQE - segment, inter-segment and intermediate levels
// Note: MSQE doesn't push collations depending on group by result into aggregation nodes
// so e.g. ORDER BY i*j doesn't trigger trimming even when hints are set
public class GroupByTrimmingIntegrationTest extends BaseClusterIntegrationTestSet {

  static final int FILES_NO = 4;
  static final int RECORDS_NO = 1000;
  static final String I_COL = "i";
  static final String J_COL = "j";
  static final int SERVERS_NO = 2;

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    startZk();
    startController();
    startServers(SERVERS_NO);
    startBroker();

    Schema schema = new Schema.SchemaBuilder().setSchemaName(DEFAULT_SCHEMA_NAME)
        .addSingleValueDimension(I_COL, FieldSpec.DataType.INT)
        .addSingleValueDimension(J_COL, FieldSpec.DataType.LONG)
        .build();
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    List<File> avroFiles = createAvroFile(_tempDir);
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(DEFAULT_TABLE_NAME, _tarDir);

    // Wait for all documents loaded
    TestUtils.waitForCondition(() -> getCurrentCountStarResult(DEFAULT_TABLE_NAME) == FILES_NO * RECORDS_NO, 100L,
        60_000,
        "Failed to load  documents", true, Duration.ofMillis(60_000 / 10));

    setUseMultiStageQueryEngine(true);

    Map<String, List<String>> map = getTableServersToSegmentsMap(getTableName(), TableType.OFFLINE);

    // make sure segments are split between multiple servers
    assertEquals(map.size(), SERVERS_NO);
  }

  protected TableConfig createOfflineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(getTableName())
        .setNumReplicas(getNumReplicas())
        .setBrokerTenant(getBrokerTenant())
        .build();
  }

  public static List<File> createAvroFile(File tempDir)
      throws IOException {

    // create avro schema
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(ImmutableList.of(
        new org.apache.avro.Schema.Field(I_COL,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT), null, null),
        new org.apache.avro.Schema.Field(J_COL,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG), null, null)));

    List<File> files = new ArrayList<>();
    for (int file = 0; file < FILES_NO; file++) {
      File avroFile = new File(tempDir, "data_" + file + ".avro");
      try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
        fileWriter.create(avroSchema, avroFile);

        for (int docId = 0; docId < RECORDS_NO; docId++) {
          GenericData.Record record = new GenericData.Record(avroSchema);
          record.put(I_COL, docId % 100);
          record.put(J_COL, docId);
          fileWriter.append(record);
        }
        files.add(avroFile);
      }
    }
    return files;
  }

  // MSQE - multi stage query engine
  @Test
  public void testMSQEOrderByOnDependingOnAggregateResultIsNotPushedDown()
      throws Exception {
    setUseMultiStageQueryEngine(true);

    Connection conn = getPinotConnection();
    assertTrimFlagNotSet(conn.execute("SELECT i, j, COUNT(*) FROM mytable GROUP BY i, j ORDER BY i*j DESC LIMIT 5"));

    String options = "SET minSegmentGroupTrimSize=5; ";
    String query = "SELECT /*+ aggOptions(is_enable_group_trim='true') */ i, j, COUNT(*) "
        + "FROM mytable GROUP BY i, j ORDER BY i*j DESC LIMIT 5 ";

    ResultSetGroup result = conn.execute(options + query);
    assertTrimFlagNotSet(result);

    assertEquals(toExplainStr(postQuery(options + " SET explainAskingServers=true; EXPLAIN PLAN FOR " + query), true),
        "Execution Plan\n"
            + "LogicalSort(sort0=[$3], dir0=[DESC], offset=[0], fetch=[5])\n"
            + "  PinotLogicalSortExchange(distribution=[hash], collation=[[3 DESC]], isSortOnSender=[false], "
            + "isSortOnReceiver=[true])\n"
            + "    LogicalSort(sort0=[$3], dir0=[DESC], fetch=[5])\n" // <-- actual sort & limit
            + "      LogicalProject(i=[$0], j=[$1], EXPR$2=[$2], EXPR$3=[*($0, $1)])\n"
            // <-- order by value is computed here, so trimming in upstream stages is not possible
            + "        PinotLogicalAggregate(group=[{0, 1}], agg#0=[COUNT($2)], aggType=[FINAL])\n"
            + "          PinotLogicalExchange(distribution=[hash[0, 1]])\n"
            + "            LeafStageCombineOperator(table=[mytable])\n"
            + "              StreamingInstanceResponse\n"
            + "                CombineGroupBy\n"
            + "                  GroupBy(groupKeys=[[i, j]], aggregations=[[count(*)]])\n"
            + "                    Project(columns=[[i, j]])\n"
            + "                      DocIdSet(maxDocs=[40000])\n"
            + "                        FilterMatchEntireSegment(numDocs=[4000])\n");
  }

  @Test
  public void testMSQEGroupsTrimmedAtSegmentLevelWithOrderByOnSomeGroupByKeysIsNotSafe()
      throws Exception {
    setUseMultiStageQueryEngine(true);

    Connection conn = getPinotConnection();
    assertTrimFlagNotSet(conn.execute("SELECT i, j, COUNT(*) FROM mytable GROUP BY i, j ORDER BY j DESC LIMIT 5"));

    String options = "SET minSegmentGroupTrimSize=5; ";
    String query = "SELECT /*+ aggOptions(is_enable_group_trim='true') */ i, j, COUNT(*) "
        + "FROM mytable GROUP BY i, j ORDER BY j DESC LIMIT 5 ";

    ResultSetGroup result = conn.execute(options + query);
    assertTrimFlagSet(result);

    assertEquals(toResultStr(result),
        "\"i\"[\"INT\"],\t\"j\"[\"LONG\"],\t\"EXPR$2\"[\"LONG\"]\n"
            + "99,\t999,\t4\n"
            + "98,\t998,\t4\n"
            + "97,\t997,\t4\n"
            + "96,\t996,\t4\n"
            + "95,\t995,\t4");

    assertEquals(toExplainStr(postQuery(options + " SET explainAskingServers=true; EXPLAIN PLAN FOR " + query), true),
        "Execution Plan\n"
            + "LogicalSort(sort0=[$1], dir0=[DESC], offset=[0], fetch=[5])\n"
            + "  PinotLogicalSortExchange(distribution=[hash], collation=[[1 DESC]], isSortOnSender=[false], "
            + "isSortOnReceiver=[true])\n"
            + "    LogicalSort(sort0=[$1], dir0=[DESC], fetch=[5])\n"
            + "      PinotLogicalAggregate(group=[{0, 1}], agg#0=[COUNT($2)], aggType=[FINAL], collations=[[1 DESC]],"
            + " limit=[5])\n"
            + "        PinotLogicalExchange(distribution=[hash[0, 1]])\n"
            + "          LeafStageCombineOperator(table=[mytable])\n"
            + "            StreamingInstanceResponse\n"
            + "              CombineGroupBy\n"
            + "                GroupBy(groupKeys=[[i, j]], aggregations=[[count(*)]])\n" // <-- trimming happens here
            + "                  Project(columns=[[i, j]])\n"
            + "                    DocIdSet(maxDocs=[40000])\n"
            + "                      FilterMatchEntireSegment(numDocs=[4000])\n");
  }

  @Test
  public void testMSQEGroupsTrimmedAtSegmentLevelWithOrderByOnAggregateIsNotSafe()
      throws Exception {
    setUseMultiStageQueryEngine(true);

    Connection conn = getPinotConnection();
    assertTrimFlagNotSet(
        conn.execute("SELECT i, j, COUNT(*) FROM mytable GROUP BY i, j ORDER BY COUNT(*) DESC LIMIT 5"));

    String options = "SET minSegmentGroupTrimSize=5; ";
    String query = "SELECT /*+ aggOptions(is_enable_group_trim='true') */ i, j, COUNT(*) "
        + "FROM mytable GROUP BY i, j ORDER BY count(*) DESC LIMIT 5 ";

    ResultSetGroup result = conn.execute(options + query);
    assertTrimFlagSet(result);

    assertEquals(toResultStr(result),
        "\"i\"[\"INT\"],\t\"j\"[\"LONG\"],\t\"EXPR$2\"[\"LONG\"]\n"
            + "77,\t377,\t4\n"
            + "66,\t566,\t4\n"
            + "39,\t339,\t4\n"
            + "96,\t396,\t4\n"
            + "25,\t25,\t4");

    assertEquals(toExplainStr(postQuery(options + " SET explainAskingServers=true; EXPLAIN PLAN FOR " + query), true),
        "Execution Plan\n"
            + "LogicalSort(sort0=[$2], dir0=[DESC], offset=[0], fetch=[5])\n"
            + "  PinotLogicalSortExchange(distribution=[hash], collation=[[2 DESC]], isSortOnSender=[false], "
            + "isSortOnReceiver=[true])\n"
            + "    LogicalSort(sort0=[$2], dir0=[DESC], fetch=[5])\n"
            + "      PinotLogicalAggregate(group=[{0, 1}], agg#0=[COUNT($2)], aggType=[FINAL], collations=[[2 DESC]],"
            + " limit=[5])\n"
            + "        PinotLogicalExchange(distribution=[hash[0, 1]])\n"
            + "          LeafStageCombineOperator(table=[mytable])\n"
            + "            StreamingInstanceResponse\n"
            + "              CombineGroupBy\n"
            + "                GroupBy(groupKeys=[[i, j]], aggregations=[[count(*)]])\n" //<-- trimming happens here
            + "                  Project(columns=[[i, j]])\n"
            + "                    DocIdSet(maxDocs=[40000])\n"
            + "                      FilterMatchEntireSegment(numDocs=[4000])\n");
  }

  @Test
  public void testMSQEGroupsTrimmedAtInterSegmentLevelWithOrderByOnSomeGroupByKeysIsNotSafe()
      throws Exception {
    setUseMultiStageQueryEngine(true);

    Connection conn = getPinotConnection();
    assertTrimFlagNotSet(conn.execute("SELECT i, j, COUNT(*) FROM mytable GROUP BY i, j ORDER BY j DESC LIMIT 5"));

    String options = "SET minServerGroupTrimSize = 5; SET groupTrimThreshold = 100; ";
    String query = "SELECT /*+ aggOptions(is_enable_group_trim='true') */ i, j, COUNT(*) "
        + "FROM mytable "
        + "GROUP BY i, j "
        + "ORDER BY j DESC "
        + "LIMIT 5 ";
    ResultSetGroup result = conn.execute(options + query);
    assertTrimFlagSet(result);

    assertEquals(toResultStr(result),
        "\"i\"[\"INT\"],\t\"j\"[\"LONG\"],\t\"EXPR$2\"[\"LONG\"]\n"
            + "99,\t999,\t4\n"
            + "98,\t998,\t4\n"
            + "97,\t997,\t4\n"
            + "96,\t996,\t4\n"
            + "95,\t995,\t4");

    assertEquals(toExplainStr(postQuery(options + "SET explainAskingServers=true; EXPLAIN PLAN FOR " + query), true),
        "Execution Plan\n"
            + "LogicalSort(sort0=[$1], dir0=[DESC], offset=[0], fetch=[5])\n"
            + "  PinotLogicalSortExchange(distribution=[hash], collation=[[1 DESC]], isSortOnSender=[false], "
            + "isSortOnReceiver=[true])\n"
            + "    LogicalSort(sort0=[$1], dir0=[DESC], fetch=[5])\n"
            + "      PinotLogicalAggregate(group=[{0, 1}], agg#0=[COUNT($2)], aggType=[FINAL], collations=[[1 DESC]],"
            + " limit=[5])\n"
            + "        PinotLogicalExchange(distribution=[hash[0, 1]])\n"
            + "          LeafStageCombineOperator(table=[mytable])\n"
            + "            StreamingInstanceResponse\n"
            + "              CombineGroupBy\n" // <-- trimming happens here
            + "                GroupBy(groupKeys=[[i, j]], aggregations=[[count(*)]])\n"
            + "                  Project(columns=[[i, j]])\n"
            + "                    DocIdSet(maxDocs=[40000])\n"
            + "                      FilterMatchEntireSegment(numDocs=[4000])\n");
  }

  @Test
  public void testMSQEGroupsTrimmedAtIntermediateLevelWithOrderByOnSomeGroupByKeysIsNotSafe()
      throws Exception {
    setUseMultiStageQueryEngine(true);

    Connection conn = getPinotConnection();
    assertTrimFlagNotSet(conn.execute("SELECT i, j, COUNT(*) FROM mytable GROUP BY i, j ORDER BY j DESC LIMIT 5"));

    // This case is tricky because intermediate results are hash-split among servers so one gets 50 rows on average.
    // That's the reason both limit and trim size needs to be so small.
    String query = "SELECT /*+ aggOptions(is_enable_group_trim='true',mse_min_group_trim_size='5') */ i, j, COUNT(*) "
        + "FROM mytable "
        + "GROUP BY i, j "
        + "ORDER BY j DESC "
        + "LIMIT 5 ";
    ResultSetGroup result = conn.execute(query);
    assertTrimFlagSet(result);

    assertEquals(toResultStr(result),
        "\"i\"[\"INT\"],\t\"j\"[\"LONG\"],\t\"EXPR$2\"[\"LONG\"]\n"
            + "99,\t999,\t4\n"
            + "98,\t998,\t4\n"
            + "97,\t997,\t4\n"
            + "96,\t996,\t4\n"
            + "95,\t995,\t4");

    assertEquals(toExplainStr(postQuery(" set explainAskingServers=true; EXPLAIN PLAN FOR " + query), true),
        "Execution Plan\n"
            + "LogicalSort(sort0=[$1], dir0=[DESC], offset=[0], fetch=[5])\n"
            + "  PinotLogicalSortExchange(distribution=[hash], collation=[[1 DESC]], isSortOnSender=[false], "
            + "isSortOnReceiver=[true])\n"
            + "    LogicalSort(sort0=[$1], dir0=[DESC], fetch=[5])\n"
            + "      PinotLogicalAggregate(group=[{0, 1}], agg#0=[COUNT($2)], aggType=[FINAL], collations=[[1 DESC]],"
            + " limit=[5])\n" // receives 50-row-big blocks, trimming kicks in only if limit is lower
            + "        PinotLogicalExchange(distribution=[hash[0, 1]])\n" // splits blocks via hash distribution
            + "          LeafStageCombineOperator(table=[mytable])\n" // no trimming happens 'below'
            + "            StreamingInstanceResponse\n"
            + "              CombineGroupBy\n"
            + "                GroupBy(groupKeys=[[i, j]], aggregations=[[count(*)]])\n"
            + "                  Project(columns=[[i, j]])\n"
            + "                    DocIdSet(maxDocs=[40000])\n"
            + "                      FilterMatchEntireSegment(numDocs=[4000])\n");
  }

  // SSQE segment level
  @Test
  public void testSSQEFilteredGroupsTrimmedAtSegmentLevelWithOrderGroupByKeysDerivedFunctionIsNotSafe()
      throws Exception {
    setUseMultiStageQueryEngine(false);
    String query = "SELECT i, j, SUM(i) FILTER (WHERE i > 0) FROM mytable GROUP BY i, j ORDER BY i + j DESC LIMIT 5";

    Connection conn = getPinotConnection();
    assertTrimFlagNotSet(conn.execute(query));

    ResultSetGroup result = conn.execute("SET minSegmentGroupTrimSize=5; " + query);
    assertTrimFlagSet(result);

    assertEquals(toResultStr(result),
        "\"i\"[\"INT\"],\t\"j\"[\"LONG\"],\t\"sum(i) FILTER(WHERE i > '0')\"[\"DOUBLE\"]\n"
            + "99,\t999,\t396.0\n"
            + "98,\t998,\t392.0\n"
            + "97,\t997,\t388.0\n"
            + "96,\t996,\t384.0\n"
            + "95,\t995,\t380.0");

    assertEquals(toExplainStr(postQuery("EXPLAIN PLAN FOR " + query), false),
        "BROKER_REDUCE(sort:[plus(i,j) DESC],limit:5,postAggregations:filter(sum(i),greater_than(i,'0'))),\t1,\t0\n"
            + "COMBINE_GROUP_BY,\t2,\t1\n"
            + "PLAN_START(numSegmentsForThisPlan:4),\t-1,\t-1\n"
            + "GROUP_BY_FILTERED(groupKeys:i, j, aggregations:sum(i)),\t3,\t2\n" // <-- trimming happens here
            + "PROJECT(i, j),\t4,\t3\n"
            + "DOC_ID_SET,\t5,\t4\n"
            + "FILTER_FULL_SCAN(operator:RANGE,predicate:i > '0'),\t6,\t5\n"
            + "PROJECT(i, j),\t7,\t3\n"
            + "DOC_ID_SET,\t8,\t7\n"
            + "FILTER_MATCH_ENTIRE_SEGMENT(docs:1000),\t9,\t8\n");
  }

  @Test
  public void testSSQEGroupsTrimmedAtSegmentLevelWithOrderGroupByKeysDerivedFunctionIsNotSafe()
      throws Exception {
    setUseMultiStageQueryEngine(false);
    String query = "SELECT i, j, COUNT(*) FROM mytable GROUP BY i, j ORDER BY i + j DESC LIMIT 5";

    Connection conn = getPinotConnection();
    assertTrimFlagNotSet(conn.execute(query));

    ResultSetGroup result = conn.execute("SET minSegmentGroupTrimSize=5; " + query);
    assertTrimFlagSet(result);

    assertEquals(toResultStr(result),
        "\"i\"[\"INT\"],\t\"j\"[\"LONG\"],\t\"count(*)\"[\"LONG\"]\n"
            + "99,\t999,\t4\n"
            + "98,\t998,\t4\n"
            + "97,\t997,\t4\n"
            + "96,\t996,\t4\n"
            + "95,\t995,\t4");

    assertEquals(toExplainStr(postQuery("EXPLAIN PLAN FOR " + query), false),
        "BROKER_REDUCE(sort:[plus(i,j) DESC],limit:5),\t1,\t0\n"
            + "COMBINE_GROUP_BY,\t2,\t1\n"
            + "PLAN_START(numSegmentsForThisPlan:4),\t-1,\t-1\n"
            + "GROUP_BY(groupKeys:i, j, aggregations:count(*)),\t3,\t2\n" //<-- trimming happens here
            + "PROJECT(i, j),\t4,\t3\n"
            + "DOC_ID_SET,\t5,\t4\n"
            + "FILTER_MATCH_ENTIRE_SEGMENT(docs:1000),\t6,\t5\n");
  }

  @Test
  public void testSSQEGroupsTrimmedAtSegmentLevelWithOrderBySomeGroupByKeysIsNotSafe()
      throws Exception {
    setUseMultiStageQueryEngine(false);
    String query = "SELECT i, j, COUNT(*) FROM mytable GROUP BY i, j ORDER BY j DESC LIMIT 5";

    Connection conn = getPinotConnection();
    assertTrimFlagNotSet(conn.execute(query));

    ResultSetGroup result = conn.execute("SET minSegmentGroupTrimSize=5; " + query);
    assertTrimFlagSet(result);

    // With test data set result is stable, but in general, trimming data ordered by subset of
    // group by keys can produce incomplete group aggregates due to lack of stability.
    // That is because, for a given value of j, sorting treats all values of i the same,
    // and segment data is usually unordered.
    assertEquals(toResultStr(result),
        "\"i\"[\"INT\"],\t\"j\"[\"LONG\"],\t\"count(*)\"[\"LONG\"]\n"
            + "99,\t999,\t4\n"
            + "98,\t998,\t4\n"
            + "97,\t997,\t4\n"
            + "96,\t996,\t4\n"
            + "95,\t995,\t4");

    assertEquals(toExplainStr(postQuery("EXPLAIN PLAN FOR " + query), false),
        "BROKER_REDUCE(sort:[j DESC],limit:5),\t1,\t0\n"
            + "COMBINE_GROUP_BY,\t2,\t1\n"
            + "PLAN_START(numSegmentsForThisPlan:4),\t-1,\t-1\n"
            + "GROUP_BY(groupKeys:i, j, aggregations:count(*)),\t3,\t2\n" // <- trimming happens here
            + "PROJECT(i, j),\t4,\t3\n"
            + "DOC_ID_SET,\t5,\t4\n"
            + "FILTER_MATCH_ENTIRE_SEGMENT(docs:1000),\t6,\t5\n");
  }

  @Test
  public void testSSQEGroupsTrimmedAtSegmentLevelWithOrderByAllGroupByKeysAndHavingIsNotSafe()
      throws Exception {
    setUseMultiStageQueryEngine(false);

    // trimming is safe on rows ordered by all group by keys (regardless of key order, direction or duplications)
    // but not when HAVING clause is present
    String query = "SELECT i, j, COUNT(*) FROM mytable GROUP BY i, j HAVING i > 50  ORDER BY i ASC, j ASC";

    Connection conn = getPinotConnection();
    assertTrimFlagNotSet(conn.execute(query));

    ResultSetGroup result = conn.execute("SET minSegmentGroupTrimSize=5; " + query);
    assertTrimFlagSet(result);

    // Result is unexpectedly empty  because segment-level trim keeps first 50 records ordered by i ASC, j ASC
    // that are later filtered out at broker stage.
    assertEquals(toResultStr(result), "\"i\"[\"INT\"],\t\"j\"[\"LONG\"],\t\"count(*)\"[\"LONG\"]");

    assertEquals(toExplainStr(postQuery("EXPLAIN PLAN FOR " + query), false),
        "BROKER_REDUCE(havingFilter:i > '50',sort:[i ASC, j ASC],limit:10),\t1,\t0\n"
            + "COMBINE_GROUP_BY,\t2,\t1\n"
            + "PLAN_START(numSegmentsForThisPlan:4),\t-1,\t-1\n"
            + "GROUP_BY(groupKeys:i, j, aggregations:count(*)),\t3,\t2\n" // <- trimming happens here
            + "PROJECT(i, j),\t4,\t3\n"
            + "DOC_ID_SET,\t5,\t4\n"
            + "FILTER_MATCH_ENTIRE_SEGMENT(docs:1000),\t6,\t5\n");
  }

  @Test
  public void testSSQEGroupsTrimmedAtSegmentLevelWithOrderByAllGroupByKeysIsSafe()
      throws Exception {
    setUseMultiStageQueryEngine(false);

    // trimming is safe on rows ordered by all group by keys (regardless of key order, direction or duplications)
    String query = "SELECT i, j, COUNT(*) FROM mytable GROUP BY i, j ORDER BY j ASC, i DESC, j ASC LIMIT 5";

    Connection conn = getPinotConnection();
    assertTrimFlagNotSet(conn.execute(query));

    ResultSetGroup result = conn.execute("SET minSegmentGroupTrimSize=5; " + query);
    assertTrimFlagNotSet(result);

    assertEquals(toResultStr(result),
        "\"i\"[\"INT\"],\t\"j\"[\"LONG\"],\t\"count(*)\"[\"LONG\"]\n"
            + "0,\t0,\t4\n"
            + "1,\t1,\t4\n"
            + "2,\t2,\t4\n"
            + "3,\t3,\t4\n"
            + "4,\t4,\t4");

    assertEquals(toExplainStr(postQuery("EXPLAIN PLAN FOR " + query), false),
        "BROKER_REDUCE(sort:[j ASC, i DESC],limit:5),\t1,\t0\n"
            + "COMBINE_GROUP_BY,\t2,\t1\n"
            + "PLAN_START(numSegmentsForThisPlan:4),\t-1,\t-1\n"
            + "GROUP_BY(groupKeys:i, j, aggregations:count(*)),\t3,\t2\n"
            + "PROJECT(i, j),\t4,\t3\n"
            + "DOC_ID_SET,\t5,\t4\n"
            + "FILTER_MATCH_ENTIRE_SEGMENT(docs:1000),\t6,\t5\n");
  }

  @Test
  public void testSSQEGroupsTrimmedAtSegmentLevelWithOrderByAllGroupByKeysDuplicateKeyIsSafe()
      throws Exception {
    setUseMultiStageQueryEngine(false);

    // trimming is safe on rows ordered by all group by keys (regardless of key order, direction or duplications)
    String query = "SELECT i, j, COUNT(*) FROM mytable GROUP BY i, i, j ORDER BY j ASC, i DESC, j ASC LIMIT 5";

    Connection conn = getPinotConnection();
    assertTrimFlagNotSet(conn.execute(query));

    ResultSetGroup result = conn.execute("SET minSegmentGroupTrimSize=5; " + query);
    assertTrimFlagNotSet(result);

    assertEquals(toResultStr(result),
        "\"i\"[\"INT\"],\t\"j\"[\"LONG\"],\t\"count(*)\"[\"LONG\"]\n"
            + "0,\t0,\t4\n"
            + "1,\t1,\t4\n"
            + "2,\t2,\t4\n"
            + "3,\t3,\t4\n"
            + "4,\t4,\t4");

    assertEquals(toExplainStr(postQuery("EXPLAIN PLAN FOR " + query), false),
        "BROKER_REDUCE(sort:[j ASC, i DESC],limit:5),\t1,\t0\n"
            + "COMBINE_GROUP_BY,\t2,\t1\n"
            + "PLAN_START(numSegmentsForThisPlan:4),\t-1,\t-1\n"
            + "GROUP_BY(groupKeys:i, i, j, aggregations:count(*)),\t3,\t2\n"
            + "PROJECT(i, j),\t4,\t3\n"
            + "DOC_ID_SET,\t5,\t4\n"
            + "FILTER_MATCH_ENTIRE_SEGMENT(docs:1000),\t6,\t5\n");
  }


  @Test
  public void testSSQEGroupsTrimmedAtSegmentLevelWithOrderByAggregateIsNotSafe()
      throws Exception {
    setUseMultiStageQueryEngine(false);

    // trimming is safe on rows ordered by all group by keys (regardless of key order or direction)
    String query = "SELECT i, j, COUNT(*) FROM mytable GROUP BY i, j ORDER BY count(*)*j ASC LIMIT 5";

    Connection conn = getPinotConnection();
    assertTrimFlagNotSet(conn.execute(query));

    ResultSetGroup result = conn.execute("SET minSegmentGroupTrimSize=5; " + query);
    assertTrimFlagSet(result);

    assertEquals(toResultStr(result),
        "\"i\"[\"INT\"],\t\"j\"[\"LONG\"],\t\"count(*)\"[\"LONG\"]\n"
            + "0,\t0,\t4\n"
            + "1,\t1,\t4\n"
            + "2,\t2,\t4\n"
            + "3,\t3,\t4\n"
            + "4,\t4,\t4");

    assertEquals(toExplainStr(postQuery("EXPLAIN PLAN FOR " + query), false),
        "BROKER_REDUCE(sort:[times(count(*),j) ASC],limit:5,postAggregations:times(count(*),j)),\t1,\t0\n"
            + "COMBINE_GROUP_BY,\t2,\t1\n"
            + "PLAN_START(numSegmentsForThisPlan:4),\t-1,\t-1\n"
            + "GROUP_BY(groupKeys:i, j, aggregations:count(*)),\t3,\t2\n"
            + "PROJECT(i, j),\t4,\t3\n"
            + "DOC_ID_SET,\t5,\t4\n"
            + "FILTER_MATCH_ENTIRE_SEGMENT(docs:1000),\t6,\t5\n");
  }

  // SSQE inter-segment level

  @Test
  public void testSSQEGroupsTrimmedAtInterSegmentLevelWithOrderByOnSomeGroupByKeysIsNotSafe()
      throws Exception {
    setUseMultiStageQueryEngine(false);
    String query = "SELECT i, j, COUNT(*) FROM mytable GROUP BY i, j ORDER BY i DESC  LIMIT 5";

    Connection conn = getPinotConnection();
    assertTrimFlagNotSet(conn.execute(query));

    // on server level, trimming occurs only when threshold is reached
    ResultSetGroup result = conn.execute("SET minServerGroupTrimSize = 5; SET groupTrimThreshold = 50; " + query);
    assertTrimFlagSet(result);
    // result's order is not stable due to concurrent operations on indexed table

    assertEquals(toExplainStr(postQuery("EXPLAIN PLAN FOR " + query), false),
        "BROKER_REDUCE(sort:[i DESC],limit:5),\t1,\t0\n"
            + "COMBINE_GROUP_BY,\t2,\t1\n" // <-- trimming happens here
            + "PLAN_START(numSegmentsForThisPlan:4),\t-1,\t-1\n"
            + "GROUP_BY(groupKeys:i, j, aggregations:count(*)),\t3,\t2\n"
            + "PROJECT(i, j),\t4,\t3\n"
            + "DOC_ID_SET,\t5,\t4\n"
            + "FILTER_MATCH_ENTIRE_SEGMENT(docs:1000),\t6,\t5\n");
  }

  @Test
  public void testSSQEGroupsTrimmedAtInterSegmentLevelWithOrderByOnAllGroupByKeysIsSafe()
      throws Exception {
    // for SSQE server level == inter-segment level
    setUseMultiStageQueryEngine(false);
    String query = "SELECT i, j, COUNT(*) FROM mytable GROUP BY i, j ORDER BY i, j  LIMIT 5";

    Connection conn = getPinotConnection();
    assertTrimFlagNotSet(conn.execute(query));

    // on server level, trimming occurs only when threshold is reached
    ResultSetGroup result = conn.execute("SET minServerGroupTrimSize = 5; SET groupTrimThreshold = 100; " + query);
    assertTrimFlagNotSet(result);

    assertEquals(toResultStr(result),
        "\"i\"[\"INT\"],\t\"j\"[\"LONG\"],\t\"count(*)\"[\"LONG\"]\n"
            + "0,\t0,\t4\n"
            + "0,\t100,\t4\n"
            + "0,\t200,\t4\n"
            + "0,\t300,\t4\n"
            + "0,\t400,\t4");

    assertEquals(toExplainStr(postQuery("EXPLAIN PLAN FOR " + query), false),
        "BROKER_REDUCE(sort:[i ASC, j ASC],limit:5),\t1,\t0\n"
            + "COMBINE_GROUP_BY,\t2,\t1\n" //<-- trimming happens here
            + "PLAN_START(numSegmentsForThisPlan:4),\t-1,\t-1\n"
            + "GROUP_BY(groupKeys:i, j, aggregations:count(*)),\t3,\t2\n"
            + "PROJECT(i, j),\t4,\t3\n"
            + "DOC_ID_SET,\t5,\t4\n"
            + "FILTER_MATCH_ENTIRE_SEGMENT(docs:1000),\t6,\t5\n");
  }

  @Test
  public void testSSQEGroupsTrimmedAtInterSegmentLevelWithOrderByOnAggregateIsNotSafe()
      throws Exception {
    // for SSQE server level == inter-segment level
    setUseMultiStageQueryEngine(false);
    String query = "SELECT i, j, COUNT(*) FROM mytable GROUP BY i, j ORDER BY count(*)*j DESC LIMIT 5";

    Connection conn = getPinotConnection();
    assertTrimFlagNotSet(conn.execute(query));

    // on server level, trimming occurs only when threshold is reached
    ResultSetGroup result = conn.execute("SET minServerGroupTrimSize = 5; SET groupTrimThreshold = 100; " + query);
    assertTrimFlagSet(result);

    // Result, though unstable due to concurrent operations on IndexedTable, is similar to the following
    // (which is not correct):
    //i[INT],j[LONG],count(*)[LONG]
    //98,\t998,\t4
    //94,\t994,\t4
    //90,\t990,\t4
    //86,\t986,\t4
    //79,\t979,\t4

    assertEquals(toExplainStr(postQuery("EXPLAIN PLAN FOR " + query), false),
        "BROKER_REDUCE(sort:[times(count(*),j) DESC],limit:5,postAggregations:times(count(*),j)),\t1,\t0\n"
            + "COMBINE_GROUP_BY,\t2,\t1\n" //<-- trimming happens here
            + "PLAN_START(numSegmentsForThisPlan:4),\t-1,\t-1\n"
            + "GROUP_BY(groupKeys:i, j, aggregations:count(*)),\t3,\t2\n"
            + "PROJECT(i, j),\t4,\t3\n"
            + "DOC_ID_SET,\t5,\t4\n"
            + "FILTER_MATCH_ENTIRE_SEGMENT(docs:1000),\t6,\t5\n");
  }

  @Test
  public void testSSQEGroupsTrimmedAtInterSegmentLevelWithOrderByOnAllGroupByKeysAndHavingIsNotSafe()
      throws Exception {
    setUseMultiStageQueryEngine(false);
    String query = "SELECT i, j, COUNT(*) FROM mytable GROUP BY i, j HAVING i > 50  ORDER BY i ASC, j ASC LIMIT 5";

    Connection conn = getPinotConnection();
    assertTrimFlagNotSet(conn.execute(query));

    // on server level, trimming occurs only when threshold is reached
    ResultSetGroup result = conn.execute("SET minServerGroupTrimSize = 5; SET groupTrimThreshold = 50; " + query);
    assertTrimFlagSet(result);

    // Result is unexpectedly empty  because inter-segment-level trim keeps first 25 records ordered by i ASC, j ASC
    // that are later filtered out at broker stage.
    assertEquals(toResultStr(result), "\"i\"[\"INT\"],\t\"j\"[\"LONG\"],\t\"count(*)\"[\"LONG\"]");

    assertEquals(toExplainStr(postQuery("EXPLAIN PLAN FOR " + query), false),
        "BROKER_REDUCE(havingFilter:i > '50',sort:[i ASC, j ASC],limit:5),\t1,\t0\n"
            + "COMBINE_GROUP_BY,\t2,\t1\n" //<-- trimming happens here
            + "PLAN_START(numSegmentsForThisPlan:4),\t-1,\t-1\n"
            + "GROUP_BY(groupKeys:i, j, aggregations:count(*)),\t3,\t2\n"
            + "PROJECT(i, j),\t4,\t3\n"
            + "DOC_ID_SET,\t5,\t4\n"
            + "FILTER_MATCH_ENTIRE_SEGMENT(docs:1000),\t6,\t5\n");
  }

  // SSQE broker level

  @Test
  public void testSSQEGroupsTrimmedAtBrokerLevelOrderedByAllGroupByKeysIsSafe()
      throws Exception {
    setUseMultiStageQueryEngine(false);
    String query = "SELECT i, j, COUNT(*) FROM mytable GROUP BY i, j ORDER BY i, j LIMIT 5";

    Connection conn = getPinotConnection();
    assertTrimFlagNotSet(conn.execute(query));

    // on broker level, trimming occurs only when threshold is reached
    ResultSetGroup result = conn.execute("SET minBrokerGroupTrimSize = 5; SET groupTrimThreshold = 50; " + query);
    assertTrimFlagNotSet(result);

    assertEquals(toResultStr(result),
        "\"i\"[\"INT\"],\t\"j\"[\"LONG\"],\t\"count(*)\"[\"LONG\"]\n"
            + "0,\t0,\t4\n"
            + "0,\t100,\t4\n"
            + "0,\t200,\t4\n"
            + "0,\t300,\t4\n"
            + "0,\t400,\t4");

    assertEquals(toExplainStr(postQuery("EXPLAIN PLAN FOR " + query), false),
        "BROKER_REDUCE(sort:[i ASC, j ASC],limit:5),\t1,\t0\n" //<-- trimming happens here
            + "COMBINE_GROUP_BY,\t2,\t1\n"
            + "PLAN_START(numSegmentsForThisPlan:4),\t-1,\t-1\n"
            + "GROUP_BY(groupKeys:i, j, aggregations:count(*)),\t3,\t2\n"
            + "PROJECT(i, j),\t4,\t3\n"
            + "DOC_ID_SET,\t5,\t4\n"
            + "FILTER_MATCH_ENTIRE_SEGMENT(docs:1000),\t6,\t5\n");
  }

  @Test
  public void testSSQEGroupsTrimmedAtBrokerLevelOrderedBySomeGroupByKeysIsNotSafe()
      throws Exception {
    setUseMultiStageQueryEngine(false);
    String query = "SELECT i, j, COUNT(*) FROM mytable GROUP BY i, j ORDER BY j DESC LIMIT 5";

    Connection conn = getPinotConnection();
    ResultSetGroup result1 = conn.execute(query);
    assertTrimFlagNotSet(result1);

    // on broker level, trimming occurs only when threshold is reached
    ResultSetGroup result = conn.execute("SET minBrokerGroupTrimSize = 5; SET groupTrimThreshold = 50; " + query);
    assertTrimFlagSet(result);

    assertEquals(toResultStr(result),
        "\"i\"[\"INT\"],\t\"j\"[\"LONG\"],\t\"count(*)\"[\"LONG\"]\n"
            + "99,\t999,\t4\n"
            + "98,\t998,\t4\n"
            + "97,\t997,\t4\n"
            + "96,\t996,\t4\n"
            + "95,\t995,\t4");

    assertEquals(toExplainStr(postQuery("EXPLAIN PLAN FOR " + query), false),
        "BROKER_REDUCE(sort:[j DESC],limit:5),\t1,\t0\n" //<-- trimming happens here
            + "COMBINE_GROUP_BY,\t2,\t1\n"
            + "PLAN_START(numSegmentsForThisPlan:4),\t-1,\t-1\n"
            + "GROUP_BY(groupKeys:i, j, aggregations:count(*)),\t3,\t2\n"
            + "PROJECT(i, j),\t4,\t3\n"
            + "DOC_ID_SET,\t5,\t4\n"
            + "FILTER_MATCH_ENTIRE_SEGMENT(docs:1000),\t6,\t5\n");
  }

  @Test
  public void testSSQEGroupsTrimmedAtBrokerLevelOrderedByAllGroupByKeysAndHavingIsNotSafe()
      throws Exception {
    setUseMultiStageQueryEngine(false);
    String query = "SELECT i, j, COUNT(*) FROM mytable GROUP BY i, j HAVING i > 50  ORDER BY i ASC, j ASC LIMIT 5";

    Connection conn = getPinotConnection();
    ResultSetGroup result1 = conn.execute(query);
    assertTrimFlagNotSet(result1);

    // on broker level, trimming occurs only when threshold is reached
    ResultSetGroup result = conn.execute("SET minBrokerGroupTrimSize = 5; SET groupTrimThreshold = 50; " + query);
    assertTrimFlagSet(result);

    // Result is unexpectedly empty  because segment-level trim keeps first 50 records ordered by i ASC, j ASC
    // that are later filtered out at broker stage.
    assertEquals(toResultStr(result), "\"i\"[\"INT\"],\t\"j\"[\"LONG\"],\t\"count(*)\"[\"LONG\"]");

    assertEquals(toExplainStr(postQuery("EXPLAIN PLAN FOR " + query), false),
        "BROKER_REDUCE(havingFilter:i > '50',sort:[i ASC, j ASC],limit:5),\t1,\t0\n" //<-- trimming happens here
            + "COMBINE_GROUP_BY,\t2,\t1\n"
            + "PLAN_START(numSegmentsForThisPlan:4),\t-1,\t-1\n"
            + "GROUP_BY(groupKeys:i, j, aggregations:count(*)),\t3,\t2\n"
            + "PROJECT(i, j),\t4,\t3\n"
            + "DOC_ID_SET,\t5,\t4\n"
            + "FILTER_MATCH_ENTIRE_SEGMENT(docs:1000),\t6,\t5\n");
  }

  @Test
  public void testSSQEGroupsTrimmedAtBrokerLevelOrderedByAllGroupByAggregateIsNotSafe()
      throws Exception {
    setUseMultiStageQueryEngine(false);
    String query = "SELECT i, j, COUNT(*) FROM mytable GROUP BY i, j ORDER BY count(*)*j DESC LIMIT 5";

    Connection conn = getPinotConnection();
    assertTrimFlagNotSet(conn.execute(query));

    // on broker level, trimming occurs only when threshold is reached
    ResultSetGroup result = conn.execute("SET minBrokerGroupTrimSize = 5; SET groupTrimThreshold = 50; " + query);
    assertTrimFlagSet(result);

    // result is similar to the following, but unstable:
    //i[INT],j[LONG],count(*)[LONG]
    //99,999,4
    //98,998,4
    //97,997,4
    //96,996,4
    //82,982,4

    assertEquals(toExplainStr(postQuery("EXPLAIN PLAN FOR " + query), false),
        "BROKER_REDUCE(sort:[times(count(*),j) DESC],limit:5," //<-- trimming happens here
            + "postAggregations:times(count(*),j)),\t1,\t0\n"
            + "COMBINE_GROUP_BY,\t2,\t1\n"
            + "PLAN_START(numSegmentsForThisPlan:4),\t-1,\t-1\n"
            + "GROUP_BY(groupKeys:i, j, aggregations:count(*)),\t3,\t2\n"
            + "PROJECT(i, j),\t4,\t3\n"
            + "DOC_ID_SET,\t5,\t4\n"
            + "FILTER_MATCH_ENTIRE_SEGMENT(docs:1000),\t6,\t5\n");
  }

  private static void assertTrimFlagNotSet(ResultSetGroup result) {
    assertFalse(result.getBrokerResponse().getExecutionStats().isGroupsTrimmed());
  }

  private static void assertTrimFlagSet(ResultSetGroup result) {
    assertTrue(result.getBrokerResponse().getExecutionStats().isGroupsTrimmed());
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    dropOfflineTable(DEFAULT_TABLE_NAME);

    stopServer();
    stopBroker();
    stopController();
    stopZk();

    FileUtils.deleteDirectory(_tempDir);
  }
}
