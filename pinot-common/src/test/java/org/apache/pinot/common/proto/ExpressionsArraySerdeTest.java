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

import java.util.List;
import org.apache.pinot.common.proto.Expressions.FloatArray;
import org.apache.pinot.common.proto.Expressions.StringArray;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Verifies indexed expression-array values across the protobuf wire format using independent local messages.
public class ExpressionsArraySerdeTest {
  @Test
  public void testFloatArrayWireRoundTripRetainsIndexedValues()
      throws Exception {
    List<Float> values = List.of(1.25f, -0.0f, Float.NaN, Float.POSITIVE_INFINITY);
    FloatArray original = FloatArray.newBuilder().addAllValues(values).build();
    FloatArray restored = FloatArray.parseFrom(original.toByteArray());

    assertEquals(restored.getValuesCount(), values.size());
    for (int i = 0; i < values.size(); i++) {
      assertEquals(Float.floatToIntBits(restored.getValues(i)), Float.floatToIntBits(values.get(i)),
          "Wire serialization must retain the indexed value, including signed zero and NaN");
    }
    assertEquals(restored, original);
  }

  @Test
  public void testStringArrayWireRoundTripRetainsIndexedUtf8Values()
      throws Exception {
    List<String> values = List.of("", "pinot", "\u03c0 \u4e2d \uD83D\uDE80");
    StringArray original = StringArray.newBuilder().addAllValues(values).build();
    StringArray restored = StringArray.parseFrom(original.toByteArray());

    assertEquals(restored.getValuesCount(), values.size());
    for (int i = 0; i < values.size(); i++) {
      assertEquals(restored.getValues(i), values.get(i));
      assertTrue(restored.getValuesBytes(i).isValidUtf8());
    }
    assertEquals(restored, original);
  }
}
