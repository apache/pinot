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
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Unit tests for MAP DataType and Schema integration using ComplexFieldSpec.
 */
public class ColumnarMapDataTypeTest {

  @Test
  public void testMapDataTypeProperties() {
    FieldSpec.DataType dt = FieldSpec.DataType.MAP;
    assertEquals(dt.getStoredType(), FieldSpec.DataType.MAP);
    assertFalse(dt.isFixedWidth());
    assertFalse(dt.isNumeric());
  }

  @Test
  public void testSchemaValidationAcceptsMapField() {
    Schema schema = new Schema();
    Map<String, FieldSpec> childFieldSpecs = Map.of(
        "key", new DimensionFieldSpec("key", FieldSpec.DataType.STRING, true),
        "value", new DimensionFieldSpec("value", FieldSpec.DataType.STRING, true)
    );
    ComplexFieldSpec spec = new ComplexFieldSpec("features", FieldSpec.DataType.MAP, true, childFieldSpecs);
    schema.addField(spec);
    schema.validate();
    assertNotNull(schema.getFieldSpecFor("features"));
    assertEquals(schema.getFieldSpecFor("features").getDataType(), FieldSpec.DataType.MAP);
    assertEquals(schema.getFieldSpecFor("features").getFieldType(), FieldSpec.FieldType.COMPLEX);
  }

  @Test
  public void testSchemaMultipleMapColumns() {
    Schema schema = new Schema();

    Map<String, FieldSpec> childFieldSpecs = Map.of(
        "key", new DimensionFieldSpec("key", FieldSpec.DataType.STRING, true),
        "value", new DimensionFieldSpec("value", FieldSpec.DataType.STRING, true)
    );
    ComplexFieldSpec features = new ComplexFieldSpec("features", FieldSpec.DataType.MAP, true, childFieldSpecs);
    schema.addField(features);

    ComplexFieldSpec labels = new ComplexFieldSpec("labels", FieldSpec.DataType.MAP, true, childFieldSpecs);
    schema.addField(labels);

    schema.addField(new DimensionFieldSpec("userId", FieldSpec.DataType.STRING, true));
    schema.validate();
    assertNotNull(schema.getFieldSpecFor("features"));
    assertNotNull(schema.getFieldSpecFor("labels"));
    assertEquals(schema.getFieldSpecFor("features").getDataType(), FieldSpec.DataType.MAP);
    assertEquals(schema.getFieldSpecFor("features").getFieldType(), FieldSpec.FieldType.COMPLEX);
  }

  @Test
  public void testComplexFieldSpecIsSingleValue() {
    Map<String, FieldSpec> childFieldSpecs = Map.of(
        "key", new DimensionFieldSpec("key", FieldSpec.DataType.STRING, true),
        "value", new DimensionFieldSpec("value", FieldSpec.DataType.STRING, true)
    );
    ComplexFieldSpec spec = new ComplexFieldSpec("col", FieldSpec.DataType.MAP, true, childFieldSpecs);
    assertTrue(spec.isSingleValueField());
  }

  @Test
  public void testSchemaRoundTripPreservesMapField()
      throws Exception {
    Schema original = new Schema();
    original.setSchemaName("roundTripSchema");
    Map<String, FieldSpec> childFieldSpecs = Map.of(
        "key", new DimensionFieldSpec("key", FieldSpec.DataType.STRING, true),
        "value", new DimensionFieldSpec("value", FieldSpec.DataType.STRING, true)
    );
    ComplexFieldSpec spec = new ComplexFieldSpec("metrics", FieldSpec.DataType.MAP, true, childFieldSpecs);
    original.addField(spec);

    String jsonStr = original.toJsonObject().toString();
    Schema reloaded = Schema.fromString(jsonStr);

    reloaded.validate();
    FieldSpec reloadedSpec = reloaded.getFieldSpecFor("metrics");
    assertNotNull(reloadedSpec);
    assertEquals(reloadedSpec.getDataType(), FieldSpec.DataType.MAP);
    assertEquals(reloadedSpec.getFieldType(), FieldSpec.FieldType.COMPLEX);
  }
}
