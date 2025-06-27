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
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;


// Tests that 'groupsTrimmed' flag is set when results trimming occurs at:
// SSQE - segment, inter-segment/server and broker levels
// MSQE - segment, inter-segment and intermediate levels
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

  @Test
  public void testMSQEGroupsTrimmedAtSegmentLevel()
      throws Exception {
    setUseMultiStageQueryEngine(true);

    Connection conn = getPinotConnection();
    assertFalse(conn.execute("SELECT COUNT(*) FROM mytable GROUP BY i ORDER BY i")
        .getBrokerResponse()
        .getExecutionStats()
        .isGroupsTrimmed());

    String options = "SET minSegmentGroupTrimSize=5; ";
    String query = "SELECT /*+ aggOptions(is_enable_group_trim='true') */ COUNT(*) "
        + "FROM mytable "
        + "GROUP BY i "
        + "ORDER BY i "
        + "LIMIT 10 ";

    assertTrue(conn.execute(options + query).getBrokerResponse().getExecutionStats().isGroupsTrimmed());

    assertEquals("Execution Plan\n"
            + "LogicalSort(sort0=[$1], dir0=[ASC], offset=[0], fetch=[10])\n"
            + "  PinotLogicalSortExchange(distribution=[hash], collation=[[1]], isSortOnSender=[false], "
            + "isSortOnReceiver=[true])\n"
            + "    LogicalSort(sort0=[$1], dir0=[ASC], fetch=[10])\n"
            + "      LogicalProject(EXPR$0=[$1], i=[$0])\n"
            + "        PinotLogicalAggregate(group=[{0}], agg#0=[COUNT($1)], aggType=[FINAL], collations=[[0]], "
            + "limit=[10])\n"
            + "          PinotLogicalExchange(distribution=[hash[0]])\n"
            + "            LeafStageCombineOperator(table=[mytable])\n"
            + "              StreamingInstanceResponse\n"
            + "                CombineGroupBy\n"
            + "                  GroupBy(groupKeys=[[i]], aggregations=[[count(*)]])\n" //<-- trimming happens here
            + "                    Project(columns=[[i]])\n"
            + "                      DocIdSet(maxDocs=[40000])\n"
            + "                        FilterMatchEntireSegment(numDocs=[4000])\n",
        GroupByOptionsIntegrationTest.toExplainStr(postQuery(options
            + " SET explainAskingServers=true; EXPLAIN PLAN FOR " + query)));
  }

  @Test
  public void testMSQEGroupsTrimmedAtInterSegmentLevel()
      throws Exception {
    setUseMultiStageQueryEngine(true);

    Connection conn = getPinotConnection();
    assertFalse(conn.execute("SELECT COUNT(*) FROM mytable GROUP BY i ORDER BY i")
        .getBrokerResponse()
        .getExecutionStats()
        .isGroupsTrimmed());

    String options = "SET minServerGroupTrimSize = 5; SET groupTrimThreshold = 100; ";
    String query = "SELECT /*+ aggOptions(is_enable_group_trim='true') */ COUNT(*) "
        + "FROM mytable "
        + "GROUP BY i "
        + "ORDER BY i "
        + "LIMIT 10 ";
    assertTrue(conn.execute(options + query).getBrokerResponse().getExecutionStats().isGroupsTrimmed());

    assertEquals("Execution Plan\n"
            + "LogicalSort(sort0=[$1], dir0=[ASC], offset=[0], fetch=[10])\n"
            + "  PinotLogicalSortExchange(distribution=[hash], collation=[[1]], isSortOnSender=[false], "
            + "isSortOnReceiver=[true])\n"
            + "    LogicalSort(sort0=[$1], dir0=[ASC], fetch=[10])\n"
            + "      LogicalProject(EXPR$0=[$1], i=[$0])\n"
            + "        PinotLogicalAggregate(group=[{0}], agg#0=[COUNT($1)], aggType=[FINAL], collations=[[0]], "
            + "limit=[10])\n"
            + "          PinotLogicalExchange(distribution=[hash[0]])\n"
            + "            LeafStageCombineOperator(table=[mytable])\n"
            + "              StreamingInstanceResponse\n"
            + "                CombineGroupBy\n" // <-- trimming happens here
            + "                  GroupBy(groupKeys=[[i]], aggregations=[[count(*)]])\n"
            + "                    Project(columns=[[i]])\n"
            + "                      DocIdSet(maxDocs=[40000])\n"
            + "                        FilterMatchEntireSegment(numDocs=[4000])\n",
        GroupByOptionsIntegrationTest.toExplainStr(postQuery(options
            + "SET explainAskingServers=true; EXPLAIN PLAN FOR " + query)));
  }

  @Test
  public void testMSQEGroupsTrimmedAtIntermediateLevel()
      throws Exception {
    setUseMultiStageQueryEngine(true);

    Connection conn = getPinotConnection();
    assertFalse(conn.execute("SELECT COUNT(*) FROM mytable GROUP BY i ORDER BY i")
        .getBrokerResponse()
        .getExecutionStats()
        .isGroupsTrimmed());

    // This case is tricky because intermediate results are hash-split among servers so one gets 50 rows on average.
    // That's the reason both limit and trim size needs to be so small.
    String query = "SELECT /*+ aggOptions(is_enable_group_trim='true',mse_min_group_trim_size='5') */ COUNT(*) "
        + "FROM mytable "
        + "GROUP BY i "
        + "ORDER BY i "
        + "LIMIT 5 ";
    assertTrue(conn.execute(query).getBrokerResponse().getExecutionStats().isGroupsTrimmed());

    assertEquals("Execution Plan\n"
            + "LogicalSort(sort0=[$1], dir0=[ASC], offset=[0], fetch=[5])\n"
            + "  PinotLogicalSortExchange(distribution=[hash], collation=[[1]], isSortOnSender=[false], "
            + "isSortOnReceiver=[true])\n"
            + "    LogicalSort(sort0=[$1], dir0=[ASC], fetch=[5])\n"
            + "      LogicalProject(EXPR$0=[$1], i=[$0])\n"
            + "        PinotLogicalAggregate(group=[{0}], agg#0=[COUNT($1)], aggType=[FINAL], collations=[[0]], "
            + "limit=[5])\n" // receives 50-row-big blocks, trimming kicks in only if limit is lower
            + "          PinotLogicalExchange(distribution=[hash[0]])\n" // splits blocks via hash distribution
            + "            LeafStageCombineOperator(table=[mytable])\n" // no trimming happens 'below'
            + "              StreamingInstanceResponse\n"
            + "                CombineGroupBy\n"
            + "                  GroupBy(groupKeys=[[i]], aggregations=[[count(*)]])\n"
            + "                    Project(columns=[[i]])\n"
            + "                      DocIdSet(maxDocs=[40000])\n"
            + "                        FilterMatchEntireSegment(numDocs=[4000])\n",
        GroupByOptionsIntegrationTest.toExplainStr(postQuery(" set explainAskingServers=true; EXPLAIN PLAN FOR "
            + query)));
  }

  @Test
  public void testSSQEGroupsTrimmedAtSegmentLevel() {
    setUseMultiStageQueryEngine(false);
    String query = "SELECT COUNT(*) FROM mytable GROUP BY i ORDER BY i";

    Connection conn = getPinotConnection();
    assertFalse(conn.execute(query).getBrokerResponse().getExecutionStats().isGroupsTrimmed());

    assertTrue(conn.execute("SET minSegmentGroupTrimSize=5; " + query)
        .getBrokerResponse().getExecutionStats().isGroupsTrimmed());
  }

  @Test
  public void testSSQEGroupsTrimmedAtServerLevel() {
    setUseMultiStageQueryEngine(false);
    String query = "SELECT COUNT(*) FROM mytable GROUP BY i ORDER BY i";

    Connection conn = getPinotConnection();
    assertFalse(conn.execute(query).getBrokerResponse().getExecutionStats().isGroupsTrimmed());

    // on server level, trimming occurs only when threshold is reached
    assertTrue(conn.execute("SET minServerGroupTrimSize = 5; SET groupTrimThreshold = 100; " + query)
        .getBrokerResponse().getExecutionStats().isGroupsTrimmed());
  }

  @Test
  public void testSSQEGroupsTrimmedAtBrokerLevel() {
    setUseMultiStageQueryEngine(false);
    String query = "SELECT COUNT(*) FROM mytable GROUP BY i ORDER BY i";

    Connection conn = getPinotConnection();
    assertFalse(conn.execute(query).getBrokerResponse().getExecutionStats().isGroupsTrimmed());

    // on broker level, trimming occurs only when threshold is reached
    assertTrue(conn.execute("SET minBrokerGroupTrimSize = 5; SET groupTrimThreshold = 100; " + query)
        .getBrokerResponse().getExecutionStats().isGroupsTrimmed());
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
