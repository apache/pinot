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
package org.apache.pinot.spi.data;

import java.util.Map;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.data.ComplexFieldSpec.KEY_FIELD;
import static org.apache.pinot.spi.data.ComplexFieldSpec.VALUE_FIELD;
import static org.apache.pinot.spi.data.FieldSpec.DataType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class OpenStructDataTypeTest {

  @Test
  public void openStructRequiresDefaultValueFieldSpec() {
    assertThrows(IllegalArgumentException.class, () -> new ComplexFieldSpec(
        "o", DataType.OPEN_STRUCT, true, null, null, null));
  }

  @Test
  public void openStructRejectsChildFieldSpecs() {
    FieldSpec dflt = new DimensionFieldSpec("default", DataType.STRING, true);
    assertThrows(IllegalArgumentException.class, () -> new ComplexFieldSpec(
        "o", DataType.OPEN_STRUCT, true,
        Map.of(KEY_FIELD, new DimensionFieldSpec(KEY_FIELD, DataType.STRING, true),
            VALUE_FIELD, new DimensionFieldSpec(VALUE_FIELD, DataType.STRING, true)),
        null, dflt));
  }

  @Test
  public void openStructAcceptsValueFieldSpecsAndDefault() {
    FieldSpec dflt = new DimensionFieldSpec("default", DataType.STRING, true);
    Map<String, FieldSpec> valueFieldSpecs = Map.of(
        "count", new DimensionFieldSpec("count", DataType.INT, true),
        "name", new DimensionFieldSpec("name", DataType.STRING, true));
    ComplexFieldSpec spec = new ComplexFieldSpec(
        "o", DataType.OPEN_STRUCT, true, null, valueFieldSpecs, dflt);
    assertEquals(spec.getValueFieldSpecs(), valueFieldSpecs);
    assertEquals(spec.getDefaultValueFieldSpec(), dflt);
  }

  @Test
  public void mapRejectsValueFieldSpecs() {
    Map<String, FieldSpec> valueFieldSpecs = Map.of(
        "k", new DimensionFieldSpec("k", DataType.INT, true));
    assertThrows(IllegalArgumentException.class, () -> new ComplexFieldSpec(
        "m", DataType.MAP, true,
        Map.of(KEY_FIELD, new DimensionFieldSpec(KEY_FIELD, DataType.STRING, true),
            VALUE_FIELD, new DimensionFieldSpec(VALUE_FIELD, DataType.INT, true)),
        valueFieldSpecs, null));
  }

  @Test
  public void mapRejectsDefaultValueFieldSpec() {
    FieldSpec dflt = new DimensionFieldSpec("default", DataType.STRING, true);
    assertThrows(IllegalArgumentException.class, () -> new ComplexFieldSpec(
        "m", DataType.MAP, true,
        Map.of(KEY_FIELD, new DimensionFieldSpec(KEY_FIELD, DataType.STRING, true),
            VALUE_FIELD, new DimensionFieldSpec(VALUE_FIELD, DataType.INT, true)),
        null, dflt));
  }

  @Test
  public void mapAcceptsKeyAndValueFieldSpecs() {
    ComplexFieldSpec spec = new ComplexFieldSpec(
        "m", DataType.MAP, true,
        Map.of(KEY_FIELD, new DimensionFieldSpec(KEY_FIELD, DataType.STRING, true),
            VALUE_FIELD, new DimensionFieldSpec(VALUE_FIELD, DataType.INT, true)),
        null, null);
    assertEquals(spec.getChildFieldSpec(KEY_FIELD).getDataType(), DataType.STRING);
    assertEquals(spec.getChildFieldSpec(VALUE_FIELD).getDataType(), DataType.INT);
  }

  @Test
  public void openStructJsonRoundtrip()
      throws Exception {
    FieldSpec dflt = new DimensionFieldSpec("default", DataType.STRING, true);
    Map<String, FieldSpec> valueFieldSpecs = Map.of(
        "count", new DimensionFieldSpec("count", DataType.INT, true));
    ComplexFieldSpec original = new ComplexFieldSpec(
        "o", DataType.OPEN_STRUCT, true, null, valueFieldSpecs, dflt);

    String json = JsonUtils.objectToString(original);
    ComplexFieldSpec roundtripped = JsonUtils.stringToObject(json, ComplexFieldSpec.class);

    assertEquals(roundtripped.getDataType(), DataType.OPEN_STRUCT);
    assertEquals(roundtripped.getDefaultValueFieldSpec(), dflt);
    assertEquals(roundtripped.getValueFieldSpecs(), valueFieldSpecs);
  }

  @Test
  public void openStructJsonDeserializeWithoutDefaultIsRejected() {
    String json = "{\"name\":\"o\",\"dataType\":\"OPEN_STRUCT\",\"singleValueField\":true}";
    try {
      JsonUtils.stringToObject(json, ComplexFieldSpec.class);
      org.testng.Assert.fail("Expected deserialization to fail without defaultValueFieldSpec");
    } catch (Exception e) {
      Throwable cause = e;
      while (cause.getCause() != null && cause.getCause() != cause) {
        cause = cause.getCause();
      }
      assertTrue(cause instanceof IllegalArgumentException,
          "Expected IllegalArgumentException root cause, got " + cause);
      assertTrue(cause.getMessage().contains("OPEN_STRUCT requires defaultValueFieldSpec"),
          "Unexpected message: " + cause.getMessage());
    }
  }

  @Test
  public void openStructEmitsSyntheticChildFieldSpecsForCompat()
      throws Exception {
    ComplexFieldSpec spec = new ComplexFieldSpec(
        "o", DataType.OPEN_STRUCT, true, null, null,
        new DimensionFieldSpec("default", DataType.STRING, true));
    String json = spec.toJsonObject().toString();
    assertTrue(json.contains("childFieldSpecs"));
    assertTrue(json.contains("\"key\""));
    assertTrue(json.contains("\"value\""));
  }
}
