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
package org.apache.pinot.integration.tests.custom;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.integration.tests.BaseClusterIntegrationTest;
import org.apache.pinot.integration.tests.ClusterIntegrationTestUtils;
import org.apache.pinot.integration.tests.ExplainIntegrationTestTrait;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TimestampConfig;
import org.apache.pinot.spi.config.table.TimestampIndexGranularity;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TimestampIndexMseTest extends BaseClusterIntegrationTest implements ExplainIntegrationTestTrait {
  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServers(2);

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

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }

  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    String property = CommonConstants.MultiStageQueryRunner.KEY_OF_MULTISTAGE_EXPLAIN_INCLUDE_SEGMENT_PLAN;
    brokerConf.setProperty(property, "true");
  }

  @Test
  public void timestampIndexSubstitutedInProjections() {
    setUseMultiStageQueryEngine(true);
    explain("SELECT datetrunc('SECOND',ArrTime) FROM mytable",
        "Execution Plan\n"
            + "PinotLogicalExchange(distribution=[broadcast])\n"
            + "  LeafStageCombineOperator(table=[mytable])\n"
            + "    StreamingInstanceResponse\n"
            + "      StreamingCombineSelect\n"
            + "        SelectStreaming(table=[mytable], totalDocs=[115545])\n"
            + "          Project(columns=[[$ArrTime$SECOND]])\n"
            + "            DocIdSet(maxDocs=[120000])\n"
            + "              FilterMatchEntireSegment(numDocs=[115545])\n");
  }

  @Test
  public void timestampIndexSubstitutedInAggregateFilter() {
    setUseMultiStageQueryEngine(true);
    explain("SELECT sum(case when datetrunc('SECOND',ArrTime) > 1 then 2 else 0 end) FROM mytable",
        "Execution Plan\n"
            + "LogicalProject(EXPR$0=[CASE(=($1, 0), null:BIGINT, $0)])\n"
            + "  PinotLogicalAggregate(group=[{}], agg#0=[$SUM0($0)], agg#1=[COUNT($1)], aggType=[FINAL])\n"
            + "    PinotLogicalExchange(distribution=[hash])\n"
            + "      LeafStageCombineOperator(table=[mytable])\n"
            + "        StreamingInstanceResponse\n"
            + "          CombineAggregate\n"
            + "            AggregateFiltered(aggregations=[[sum('2'), count(*)]])\n"
            + "              Transform(expressions=[['2']])\n"
            + "                Project(columns=[[]])\n"
            + "                  DocIdSet(maxDocs=[120000])\n"
            + "                    FilterRangeIndex(predicate=[$ArrTime$SECOND > '1'], indexLookUp=[range_index], "
            + "operator=[RANGE])\n"
            + "              Project(columns=[[]])\n"
            + "                DocIdSet(maxDocs=[120000])\n"
            + "                  FilterMatchEntireSegment(numDocs=[115545])\n");
  }

  @Test
  public void timestampIndexSubstitutedInGroupBy() {
    setUseMultiStageQueryEngine(true);
    explain("SELECT count(*) FROM mytable group by datetrunc('SECOND',ArrTime)",
        "Execution Plan\n"
            + "LogicalProject(EXPR$0=[$1])\n"
            + "  PinotLogicalAggregate(group=[{0}], agg#0=[COUNT($1)], aggType=[FINAL])\n"
            + "    PinotLogicalExchange(distribution=[hash[0]])\n"
            + "      LeafStageCombineOperator(table=[mytable])\n"
            + "        StreamingInstanceResponse\n"
            + "          CombineGroupBy\n"
            + "            GroupBy(groupKeys=[[$ArrTime$SECOND]], aggregations=[[count(*)]])\n"
            + "              Project(columns=[[$ArrTime$SECOND]])\n"
            + "                DocIdSet(maxDocs=[120000])\n"
            + "                  FilterMatchEntireSegment(numDocs=[115545])\n");
  }

  @Test
  public void timestampIndexSubstitutedInJoinMSE() {
    setUseMultiStageQueryEngine(true);
    explain("SELECT 1 "
            + "FROM mytable as a1 "
            + "join mytable as a2 "
            + "on datetrunc('SECOND',a1.ArrTime) = datetrunc('DAY',a2.ArrTime)",
        "Execution Plan\n"
            + "LogicalProject(EXPR$0=[1])\n"
            + "  LogicalJoin(condition=[=($0, $1)], joinType=[inner])\n"
            + "    PinotLogicalExchange(distribution=[hash[0]])\n"
            + "      LeafStageCombineOperator(table=[mytable])\n"
            + "        StreamingInstanceResponse\n"
            + "          StreamingCombineSelect\n"
            + "            SelectStreaming(table=[mytable], totalDocs=[115545])\n"
            + "              Project(columns=[[$ArrTime$SECOND]])\n" // substituted because we have SECOND granularity
            + "                DocIdSet(maxDocs=[120000])\n"
            + "                  FilterMatchEntireSegment(numDocs=[115545])\n"
            + "    PinotLogicalExchange(distribution=[hash[0]])\n"
            + "      LeafStageCombineOperator(table=[mytable])\n"
            + "        StreamingInstanceResponse\n"
            + "          StreamingCombineSelect\n"
            + "            SelectStreaming(table=[mytable], totalDocs=[115545])\n"
            + "              Transform(expressions=[[datetrunc('DAY',ArrTime)]])\n" // we don't set the DAY granularity
            + "                Project(columns=[[ArrTime]])\n"
            + "                  DocIdSet(maxDocs=[120000])\n"
            + "                    FilterMatchEntireSegment(numDocs=[115545])\n");
  }


  protected TableConfig createOfflineTableConfig() {
    String colName = "ArrTime";

    TableConfig tableConfig = super.createOfflineTableConfig();
    List<FieldConfig> fieldConfigList = tableConfig.getFieldConfigList();
    if (fieldConfigList == null) {
      fieldConfigList = new ArrayList<>();
      tableConfig.setFieldConfigList(fieldConfigList);
    } else {
      fieldConfigList.stream()
          .filter(fieldConfig -> fieldConfig.getName().equals(colName))
          .findFirst()
          .ifPresent(
              fieldConfig -> {
                throw new IllegalStateException("Time column already exists in the field config list");
              }
          );
    }
    FieldConfig newTimeFieldConfig = new FieldConfig.Builder(colName)
        .withTimestampConfig(
            new TimestampConfig(List.of(TimestampIndexGranularity.SECOND))
        )
        .build();
    fieldConfigList.add(newTimeFieldConfig);
    return tableConfig;
  }
}
