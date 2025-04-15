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
import java.util.regex.Pattern;
import org.apache.pinot.integration.tests.BaseClusterIntegrationTest;
import org.apache.pinot.integration.tests.ClusterIntegrationTestUtils;
import org.apache.pinot.integration.tests.ExplainIntegrationTestTrait;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TimestampConfig;
import org.apache.pinot.spi.config.table.TimestampIndexGranularity;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TimestampIndexSseTest extends BaseClusterIntegrationTest implements ExplainIntegrationTestTrait {
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

  @Test
  public void timestampIndexSubstitutedInProjections() {
    setUseMultiStageQueryEngine(false);
    explainSse("SELECT datetrunc('SECOND',ArrTime) FROM mytable",
    "[BROKER_REDUCE(limit:10), 1, 0]",
        "[COMBINE_SELECT, 2, 1]",
        "[PLAN_START(numSegmentsForThisPlan:1), -1, -1]",
        "[SELECT(selectList:$ArrTime$SECOND), 3, 2]",
        "[PROJECT($ArrTime$SECOND), 4, 3]",
        "[DOC_ID_SET, 5, 4]",
        Pattern.compile("\\[FILTER_MATCH_ENTIRE_SEGMENT\\(docs:[0-9]+\\), 6, 5]"));
  }

  @Test
  public void timestampIndexSubstitutedInAggregateFilter() {
    setUseMultiStageQueryEngine(false);
    explainSse("SELECT sum(case when datetrunc('SECOND',ArrTime) > 1 then 2 else 0 end) FROM mytable",
    "[BROKER_REDUCE(limit:10), 1, 0]",
        "[COMBINE_AGGREGATE, 2, 1]",
        "[PLAN_START(numSegmentsForThisPlan:1), -1, -1]",
        "[AGGREGATE(aggregations:sum(case(greater_than($ArrTime$SECOND,'1'),'2','0'))), 3, 2]",
        "[TRANSFORM(case(greater_than($ArrTime$SECOND,'1'),'2','0')), 4, 3]",
        "[PROJECT($ArrTime$SECOND), 5, 4]",
        "[DOC_ID_SET, 6, 5]",
        Pattern.compile("\\[FILTER_MATCH_ENTIRE_SEGMENT\\(docs:[0-9]+\\), 7, 6]"));
  }

  @Test
  public void timestampIndexSubstitutedInGroupBy() {
    setUseMultiStageQueryEngine(false);
    explainSse("SELECT count(*) FROM mytable group by datetrunc('SECOND',ArrTime)",
    "[BROKER_REDUCE(limit:10), 1, 0]",
        "[COMBINE_GROUP_BY, 2, 1]",
        "[PLAN_START(numSegmentsForThisPlan:1), -1, -1]",
        "[GROUP_BY(groupKeys:$ArrTime$SECOND, aggregations:count(*)), 3, 2]",
        "[PROJECT($ArrTime$SECOND), 4, 3]",
        "[DOC_ID_SET, 5, 4]",
        Pattern.compile("\\[FILTER_MATCH_ENTIRE_SEGMENT\\(docs:[0-9]+\\), 6, 5]"));
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
