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

import java.util.HashMap;
import java.util.Map;
import org.apache.helix.HelixManager;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class PartialUpsertHandlerTest {

  @Test
  public void testMerge() {
    HelixManager helixManager = Mockito.mock(HelixManager.class);
    String realtimeTableName = "testTable_REALTIME";
    Map<String, UpsertConfig.Strategy> partialUpsertStrategies = new HashMap<>();
    partialUpsertStrategies.put("field1", UpsertConfig.Strategy.INCREMENT);
    PartialUpsertHandler handler = new PartialUpsertHandler(helixManager, realtimeTableName, partialUpsertStrategies);

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
    previousRecord.clear();
    incomingRecord.clear();
    previousRecord.putValue("field1", 1);
    incomingRecord.putDefaultNullValue("field1", 2);
    newRecord = handler.merge(previousRecord, incomingRecord);
    assertFalse(newRecord.isNullValue("field1"));
    assertEquals(newRecord.getValue("field1"), 1);

    // neither of records is null.
    previousRecord.clear();
    incomingRecord.clear();
    previousRecord.putValue("field1", 1);
    incomingRecord.putValue("field1", 2);
    newRecord = handler.merge(previousRecord, incomingRecord);
    assertFalse(newRecord.isNullValue("field1"));
    assertEquals(newRecord.getValue("field1"), 3);
  }
}
