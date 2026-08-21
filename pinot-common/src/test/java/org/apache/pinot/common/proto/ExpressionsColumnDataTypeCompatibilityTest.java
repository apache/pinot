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
package org.apache.pinot.common.proto;

import java.util.Map;
import org.apache.pinot.common.proto.legacy.LegacyExpressions;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Locks the permanent protobuf numbers used by query-plan expressions.
///
/// These numbers are read across broker/server version boundaries. Renumbering an existing value, or assigning a
/// new type to an already allocated number, can silently reinterpret a query plan as a different logical type.
public class ExpressionsColumnDataTypeCompatibilityTest {
  @Test
  public void testStableWireNumbers() {
    Map<String, Integer> legacyNumbers = Map.ofEntries(
        Map.entry("INT", 0),
        Map.entry("LONG", 1),
        Map.entry("FLOAT", 2),
        Map.entry("DOUBLE", 3),
        Map.entry("BIG_DECIMAL", 4),
        Map.entry("BOOLEAN", 5),
        Map.entry("TIMESTAMP", 6),
        Map.entry("STRING", 7),
        Map.entry("JSON", 8),
        Map.entry("BYTES", 9),
        Map.entry("OBJECT", 10),
        Map.entry("INT_ARRAY", 11),
        Map.entry("LONG_ARRAY", 12),
        Map.entry("FLOAT_ARRAY", 13),
        Map.entry("DOUBLE_ARRAY", 14),
        Map.entry("BOOLEAN_ARRAY", 15),
        Map.entry("TIMESTAMP_ARRAY", 16),
        Map.entry("STRING_ARRAY", 17),
        Map.entry("BYTES_ARRAY", 18),
        Map.entry("UNKNOWN", 19),
        Map.entry("MAP", 20),
        Map.entry("BIG_DECIMAL_ARRAY", 21),
        Map.entry("UUID", 22),
        Map.entry("UUID_ARRAY", 23));

    for (Map.Entry<String, Integer> entry : legacyNumbers.entrySet()) {
      assertEquals(Expressions.ColumnDataType.valueOf(entry.getKey()).getNumber(), entry.getValue().intValue(),
          "Wire number changed for " + entry.getKey());
    }
    assertEquals(Expressions.ColumnDataType.VARIANT.getNumber(), 24);
  }

  @Test
  public void testPreVariantAndCurrentPeersHaveDeterministicWireBehavior()
      throws Exception {
    byte[] legacyPayload = LegacyExpressions.FunctionCall.newBuilder()
        .setDataType(LegacyExpressions.ColumnDataType.STRING)
        .build()
        .toByteArray();
    Expressions.FunctionCall parsedByCurrentPeer = Expressions.FunctionCall.parseFrom(legacyPayload);
    assertEquals(parsedByCurrentPeer.getDataType(), Expressions.ColumnDataType.STRING);
    assertEquals(parsedByCurrentPeer.getDataTypeValue(), 7);

    byte[] variantPayload = Expressions.FunctionCall.newBuilder()
        .setDataType(Expressions.ColumnDataType.VARIANT)
        .build()
        .toByteArray();
    LegacyExpressions.FunctionCall parsedByLegacyPeer = LegacyExpressions.FunctionCall.parseFrom(variantPayload);
    assertEquals(parsedByLegacyPeer.getDataType(), LegacyExpressions.ColumnDataType.UNRECOGNIZED);
    assertEquals(parsedByLegacyPeer.getDataTypeValue(), 24);
  }
}
