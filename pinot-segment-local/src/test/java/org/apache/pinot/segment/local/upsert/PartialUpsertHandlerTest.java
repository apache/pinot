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
package org.apache.pinot.segment.local.upsert;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class PartialUpsertHandlerTest {

  @Test
  public void testMerge() {
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension("pk", FieldSpec.DataType.STRING)
        .addSingleValueDimension("field1", FieldSpec.DataType.LONG)
        .addDateTime("hoursSinceEpoch", FieldSpec.DataType.LONG, "1:HOURS:EPOCH", "1:HOURS")
        .setPrimaryKeyColumns(Arrays.asList("pk")).build();
    Map<String, UpsertConfig.Strategy> partialUpsertStrategies = new HashMap<>();
    partialUpsertStrategies.put("field1", UpsertConfig.Strategy.INCREMENT);
    UpsertConfig upsertConfig = new UpsertConfig();
    upsertConfig.setDefaultPartialUpsertStrategy(UpsertConfig.Strategy.OVERWRITE);
    PartialUpsertHandler handler =
        new PartialUpsertHandler(schema, partialUpsertStrategies, upsertConfig,
            Collections.singletonList("hoursSinceEpoch"));

    // both records are null.
    GenericRow previousRecord = new GenericRow();
    GenericRow incomingRecord = new GenericRow();

    previousRecord.putDefaultNullValue("field1", 1);
    incomingRecord.putDefaultNullValue("field1", 2);
    GenericRow newRecord = handler.merge(previousRecord, incomingRecord);
    assertTrue(newRecord.isNullValue("field1"));
    assertEquals(newRecord.getValue("field1"), 2);

    // previousRecord is null default value, while newRecord is not.
    previousRecord.clear();
    incomingRecord.clear();
    previousRecord.putDefaultNullValue("field1", 1);
    incomingRecord.putValue("field1", 2);
    newRecord = handler.merge(previousRecord, incomingRecord);
    assertFalse(newRecord.isNullValue("field1"));
    assertEquals(newRecord.getValue("field1"), 2);

    // newRecord is default null value, while previousRecord is not.
    // field1 should not be incremented since the newRecord is null.
    // special case: field2 should be merged based on default partial upsert strategy.
    previousRecord.clear();
    incomingRecord.clear();
    previousRecord.putValue("field1", 1);
    previousRecord.putValue("field2", 2);
    incomingRecord.putDefaultNullValue("field1", 2);
    incomingRecord.putDefaultNullValue("field2", 0);
    newRecord = handler.merge(previousRecord, incomingRecord);
    assertFalse(newRecord.isNullValue("field1"));
    assertEquals(newRecord.getValue("field1"), 1);
    assertFalse(newRecord.isNullValue("field2"));
    assertEquals(newRecord.getValue("field2"), 2);

    // neither of records is null.
    previousRecord.clear();
    incomingRecord.clear();
    previousRecord.putValue("field1", 1);
    incomingRecord.putValue("field1", 2);
    newRecord = handler.merge(previousRecord, incomingRecord);
    assertFalse(newRecord.isNullValue("field1"));
    assertEquals(newRecord.getValue("field1"), 3);
  }

  @Test
  public void testMergeWithDefaultPartialUpsertStrategy() {
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension("pk", FieldSpec.DataType.STRING)
        .addSingleValueDimension("field1", FieldSpec.DataType.LONG).addMetric("field2", FieldSpec.DataType.LONG)
        .addDateTime("hoursSinceEpoch", FieldSpec.DataType.LONG, "1:HOURS:EPOCH", "1:HOURS")
        .setPrimaryKeyColumns(Arrays.asList("pk")).build();
    Map<String, UpsertConfig.Strategy> partialUpsertStrategies = new HashMap<>();
    partialUpsertStrategies.put("field1", UpsertConfig.Strategy.INCREMENT);
    UpsertConfig upsertConfig = new UpsertConfig();
    upsertConfig.setDefaultPartialUpsertStrategy(UpsertConfig.Strategy.OVERWRITE);
    PartialUpsertHandler handler =
        new PartialUpsertHandler(schema, partialUpsertStrategies, upsertConfig,
            Collections.singletonList("hoursSinceEpoch"));

    // previousRecord is null default value, while newRecord is not.
    GenericRow previousRecord = new GenericRow();
    GenericRow incomingRecord = new GenericRow();
    previousRecord.putDefaultNullValue("field1", 1);
    previousRecord.putDefaultNullValue("field2", 2);
    incomingRecord.putValue("field1", 2);
    incomingRecord.putValue("field2", 1);
    GenericRow newRecord = handler.merge(previousRecord, incomingRecord);
    assertFalse(newRecord.isNullValue("field1"));
    assertEquals(newRecord.getValue("field1"), 2);
    assertEquals(newRecord.getValue("field2"), 1);

    // newRecord is default null value, while previousRecord is not.
    // field1 should not be incremented since the newRecord is null.
    // field2 should not be overrided by null value since we have default partial upsert strategy.
    previousRecord.clear();
    incomingRecord.clear();
    previousRecord.putValue("field1", 8);
    previousRecord.putValue("field2", 8);
    incomingRecord.putDefaultNullValue("field1", 1);
    incomingRecord.putDefaultNullValue("field2", 0);
    newRecord = handler.merge(previousRecord, incomingRecord);
    assertEquals(newRecord.getValue("field1"), 8);
    assertEquals(newRecord.getValue("field2"), 8);

    // neither of records is null.
    previousRecord.clear();
    incomingRecord.clear();
    previousRecord.putValue("field1", 1);
    previousRecord.putValue("field2", 100);
    incomingRecord.putValue("field1", 2);
    incomingRecord.putValue("field2", 1000);
    newRecord = handler.merge(previousRecord, incomingRecord);
    assertEquals(newRecord.getValue("field1"), 3);
    assertEquals(newRecord.getValue("field2"), 1000);
  }
}
