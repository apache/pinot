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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class OpenStructDataTypeTest {

  @Test
  public void openStructRequiresDefaultValueFieldSpec() {
    assertThrows(IllegalArgumentException.class, () -> new ComplexFieldSpec(
        "o", DataType.OPEN_STRUCT, true, null, null));
  }

  @Test
  public void openStructAcceptsChildFieldSpecsAndDefault() {
    FieldSpec dflt = new DimensionFieldSpec("default", DataType.STRING, true);
    Map<String, FieldSpec> childFieldSpecs = Map.of(
        "count", new DimensionFieldSpec("count", DataType.INT, true),
        "name", new DimensionFieldSpec("name", DataType.STRING, true));
    ComplexFieldSpec spec = new ComplexFieldSpec(
        "o", DataType.OPEN_STRUCT, true, childFieldSpecs, dflt);
    assertEquals(spec.getChildFieldSpec("count").getDataType(), DataType.INT);
    assertEquals(spec.getChildFieldSpec("name").getDataType(), DataType.STRING);
    assertEquals(spec.getDefaultValueFieldSpec(), dflt);
  }

  @Test
  public void openStructAcceptsEmptyChildFieldSpecs() {
    FieldSpec dflt = new DimensionFieldSpec("default", DataType.STRING, true);
    ComplexFieldSpec spec = new ComplexFieldSpec(
        "o", DataType.OPEN_STRUCT, true, null, dflt);
    assertNotNull(spec.getChildFieldSpecs());
    assertTrue(spec.getChildFieldSpecs().isEmpty());
    assertEquals(spec.getDefaultValueFieldSpec(), dflt);
  }

  @Test
  public void mapRejectsDefaultValueFieldSpec() {
    FieldSpec dflt = new DimensionFieldSpec("default", DataType.STRING, true);
    assertThrows(IllegalArgumentException.class, () -> new ComplexFieldSpec(
        "m", DataType.MAP, true,
        Map.of(KEY_FIELD, new DimensionFieldSpec(KEY_FIELD, DataType.STRING, true),
            VALUE_FIELD, new DimensionFieldSpec(VALUE_FIELD, DataType.INT, true)),
        dflt));
  }

  @Test
  public void mapAcceptsKeyAndValueFieldSpecs() {
    ComplexFieldSpec spec = new ComplexFieldSpec(
        "m", DataType.MAP, true,
        Map.of(KEY_FIELD, new DimensionFieldSpec(KEY_FIELD, DataType.STRING, true),
            VALUE_FIELD, new DimensionFieldSpec(VALUE_FIELD, DataType.INT, true)));
    assertEquals(spec.getChildFieldSpec(KEY_FIELD).getDataType(), DataType.STRING);
    assertEquals(spec.getChildFieldSpec(VALUE_FIELD).getDataType(), DataType.INT);
  }

  @Test
  public void openStructJsonRoundtrip()
      throws Exception {
    FieldSpec dflt = new DimensionFieldSpec("default", DataType.STRING, true);
    Map<String, FieldSpec> childFieldSpecs = Map.of(
        "count", new DimensionFieldSpec("count", DataType.INT, true));
    ComplexFieldSpec original = new ComplexFieldSpec(
        "o", DataType.OPEN_STRUCT, true, childFieldSpecs, dflt);

    String json = JsonUtils.objectToString(original);
    ComplexFieldSpec roundtripped = JsonUtils.stringToObject(json, ComplexFieldSpec.class);

    assertEquals(roundtripped.getDataType(), DataType.OPEN_STRUCT);
    assertEquals(roundtripped.getDefaultValueFieldSpec().getDataType(), DataType.STRING);
    assertEquals(roundtripped.getChildFieldSpec("count").getDataType(), DataType.INT);
  }

  @Test
  public void openStructJsonDeserializeWithoutDefaultIsRejectedBySchemaValidate()
      throws Exception {
    String schemaJson = "{"
        + "\"schemaName\":\"test\","
        + "\"complexFieldSpecs\":["
        + "  {\"name\":\"attrs\",\"dataType\":\"OPEN_STRUCT\",\"singleValueField\":true}"
        + "]}";
    Schema schema = JsonUtils.stringToObject(schemaJson, Schema.class);
    try {
      schema.validate();
      org.testng.Assert.fail("Expected schema.validate() to reject OPEN_STRUCT without defaultValueFieldSpec");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("OPEN_STRUCT") && e.getMessage().contains("defaultValueFieldSpec"),
          "Unexpected message: " + e.getMessage());
    }
  }
}
