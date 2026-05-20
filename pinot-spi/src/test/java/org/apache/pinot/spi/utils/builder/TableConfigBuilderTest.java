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

package org.apache.pinot.spi.utils.builder;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.config.table.DedupConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceReplicaGroupPartitionConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceTagPoolConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for the validations in {@link TableConfigBuilder}
 */
public class TableConfigBuilderTest {

  private static final String TABLE_NAME = "testTable";
  private static final String TIME_COLUMN = "timeColumn";

  @Test
  public void testValidateSkipSegmentPreprocessFlag() {

    TableConfig tableconfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName(TABLE_NAME).setTimeColumnName(TIME_COLUMN)
        .setSkipSegmentPreprocess(true).build();
    Assert.assertTrue(tableconfig.getIndexingConfig().isSkipSegmentPreprocess(),
        "skipSegmentPreprocess will be true");
  }

  @Test
  public void testBuildOmitsDefaultDeprecatedFieldsFromSerializedJson()
      throws Exception {
    TableConfig offlineTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setFieldConfigList(List.of(new FieldConfig("c1", FieldConfig.EncodingType.DICTIONARY,
            List.of(FieldConfig.IndexType.INVERTED), null, null)))
        .build();
    JsonNode offlineJson = JsonUtils.stringToJsonNode(offlineTableConfig.toJsonString());
    Assert.assertFalse(offlineJson.path("segmentsConfig").has("minimizeDataMovement"));
    Assert.assertFalse(offlineJson.path("tableIndexConfig").has("createInvertedIndexDuringSegmentGeneration"));
    Assert.assertFalse(offlineJson.path("fieldConfigList").get(0).has("indexType"));

    TableConfig upsertTableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setUpsertConfig(new UpsertConfig())
        .build();
    JsonNode upsertJson = JsonUtils.stringToJsonNode(upsertTableConfig.toJsonString());
    Assert.assertFalse(upsertJson.path("upsertConfig").has("enableSnapshot"));
    Assert.assertFalse(upsertJson.path("upsertConfig").has("enablePreload"));
    Assert.assertFalse(upsertJson.path("upsertConfig").has("allowPartialUpsertConsumptionDuringCommit"));

    InstanceAssignmentConfig instanceAssignmentConfig = new InstanceAssignmentConfig(
        new InstanceTagPoolConfig("testTenant", false, 0, null), null,
        new InstanceReplicaGroupPartitionConfig(false, 0, 0, 0, 0, 0, false, null), null, false);
    TableConfig dedupTableConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName(TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setDedupConfig(new DedupConfig())
        .setInstanceAssignmentConfigMap(Map.of("CONSUMING", instanceAssignmentConfig))
        .build();
    JsonNode dedupJson = JsonUtils.stringToJsonNode(dedupTableConfig.toJsonString());
    Assert.assertFalse(dedupJson.path("dedupConfig").has("enablePreload"));
    Assert.assertFalse(dedupJson.path("dedupConfig").has("allowDedupConsumptionDuringCommit"));
    Assert.assertFalse(dedupJson.path("instanceAssignmentConfigMap").path("CONSUMING")
        .path("replicaGroupPartitionConfig").has("minimizeDataMovement"));
  }
}
