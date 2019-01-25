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
package org.apache.pinot.broker.requesthandler;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.concurrent.TimeUnit;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.data.TimeFieldSpec;
import org.apache.pinot.common.utils.JsonUtils;
import org.apache.pinot.common.utils.SchemaUtils;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TableSchemaCacheTest {
  private static String TEST_TABLE = "testTableName";

  @Test
  public void testTableSchemaCache()
      throws Exception {
    ZNRecord znRecord = makeZNRecord();
    Schema expectedSchema = SchemaUtils.fromZNRecord(znRecord);
    ZkHelixPropertyStore<ZNRecord> fakePropertyStore = makePropertyStore(znRecord);
    TableSchemaCache tableSchemaCache = new TableSchemaCache(fakePropertyStore, 1);

    Schema schema = tableSchemaCache.getIfTableSchemaPresent(TEST_TABLE);
    Assert.assertNull(schema);

    long timeoutInMs = 500L;
    String errorMsg = String.format("TableSchemaCache can't be updated within %d", timeoutInMs);
    tableSchemaCache.refreshTableSchema(TEST_TABLE);
    TestUtils.waitForCondition(aVoid -> {
      Schema schema1 = tableSchemaCache.getIfTableSchemaPresent(TEST_TABLE);
      return schema1 != null;
    }, 100L, timeoutInMs, errorMsg);

    schema = tableSchemaCache.getIfTableSchemaPresent(TEST_TABLE);
    Assert.assertEquals(schema, expectedSchema);
  }

  private ZNRecord makeZNRecord() {
    ObjectNode jsonSchema = JsonUtils.newObjectNode();
    jsonSchema.put("schemaName", TEST_TABLE);

    // dimension fields spec
    ArrayNode dimensionJsonArray = JsonUtils.newArrayNode();
    ObjectNode dimensionObjectNode = JsonUtils.newObjectNode();
    dimensionObjectNode.put("name", "name");
    dimensionObjectNode.put("dataType", "STRING");
    dimensionObjectNode.put("isSingleValueField", true);
    dimensionObjectNode.put("defaultNullValue", "null");
    dimensionJsonArray.add(dimensionObjectNode);
    jsonSchema.set("dimensionFieldSpecs", dimensionJsonArray);

    // metric fields spec
    ArrayNode metricJsonArray = JsonUtils.newArrayNode();
    ObjectNode metricObjectNode = JsonUtils.newObjectNode();
    metricObjectNode.put("name", "count");
    metricObjectNode.put("dataType", "INT");
    metricObjectNode.put("isSingleValueField", true);
    metricObjectNode.put("defaultNullValue", 0);
    metricJsonArray.add(metricObjectNode);
    jsonSchema.set("metricFieldSpecs", metricJsonArray);

    // time field spec
    TimeFieldSpec timeFieldSpec = new TimeFieldSpec("incomingSec", FieldSpec.DataType.INT, TimeUnit.SECONDS);
    jsonSchema.set("timeFieldSpec", timeFieldSpec.toJsonObject());

    ZNRecord schemaZNRecord = new ZNRecord(TEST_TABLE);
    schemaZNRecord.setSimpleField("schemaJSON", jsonSchema.toString());
    return schemaZNRecord;
  }

  private ZkHelixPropertyStore<ZNRecord> makePropertyStore(ZNRecord znRecord) {
    ZkHelixPropertyStore<ZNRecord> store = mock(ZkHelixPropertyStore.class);
    when(store.get(anyString(), any(), anyInt())).thenReturn(znRecord);
    return store;
  }
}
