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

  // ---- Tests for ComplexFieldSpec.equals() including keyTypes and defaultValueType ----

  @Test
  public void testComplexFieldSpecEqualsDiffersOnKeyTypes() {
    Map<String, FieldSpec> children = Map.of(
        "key", new DimensionFieldSpec("key", FieldSpec.DataType.STRING, true),
        "value", new DimensionFieldSpec("value", FieldSpec.DataType.STRING, true)
    );
    ComplexFieldSpec spec1 = new ComplexFieldSpec("col", FieldSpec.DataType.MAP, true, children);
    ComplexFieldSpec spec2 = new ComplexFieldSpec("col", FieldSpec.DataType.MAP, true, children);

    // Without keyTypes, they should be equal
    assertEquals(spec1, spec2);

    // Set keyTypes on spec1 only
    spec1.setKeyTypes(Map.of("k1", FieldSpec.DataType.INT));
    assertNotEquals(spec1, spec2, "ComplexFieldSpecs with different keyTypes must not be equal");
  }

  @Test
  public void testComplexFieldSpecEqualsDiffersOnDefaultValueType() {
    Map<String, FieldSpec> children = Map.of(
        "key", new DimensionFieldSpec("key", FieldSpec.DataType.STRING, true),
        "value", new DimensionFieldSpec("value", FieldSpec.DataType.STRING, true)
    );
    ComplexFieldSpec spec1 = new ComplexFieldSpec("col", FieldSpec.DataType.MAP, true, children);
    ComplexFieldSpec spec2 = new ComplexFieldSpec("col", FieldSpec.DataType.MAP, true, children);

    spec1.setDefaultValueType(FieldSpec.DataType.STRING);
    assertNotEquals(spec1, spec2, "ComplexFieldSpecs with different defaultValueType must not be equal");
  }

  @Test
  public void testComplexFieldSpecEqualsMatchingKeyTypesAreEqual() {
    Map<String, FieldSpec> children = Map.of(
        "key", new DimensionFieldSpec("key", FieldSpec.DataType.STRING, true),
        "value", new DimensionFieldSpec("value", FieldSpec.DataType.STRING, true)
    );
    ComplexFieldSpec spec1 = new ComplexFieldSpec("col", FieldSpec.DataType.MAP, true, children);
    ComplexFieldSpec spec2 = new ComplexFieldSpec("col", FieldSpec.DataType.MAP, true, children);

    Map<String, FieldSpec.DataType> keyTypes = Map.of("k1", FieldSpec.DataType.INT, "k2", FieldSpec.DataType.STRING);
    spec1.setKeyTypes(keyTypes);
    spec2.setKeyTypes(keyTypes);
    spec1.setDefaultValueType(FieldSpec.DataType.STRING);
    spec2.setDefaultValueType(FieldSpec.DataType.STRING);

    assertEquals(spec1, spec2);
    assertEquals(spec1.hashCode(), spec2.hashCode());
  }

  @Test
  public void testSchemaEqualsDetectsKeyTypesChange() {
    Schema schema1 = new Schema();
    schema1.setSchemaName("test");
    ComplexFieldSpec spec1 = new ComplexFieldSpec("col", FieldSpec.DataType.MAP, true, Map.of());
    schema1.addField(spec1);

    Schema schema2 = new Schema();
    schema2.setSchemaName("test");
    ComplexFieldSpec spec2 = new ComplexFieldSpec("col", FieldSpec.DataType.MAP, true, Map.of());
    spec2.setKeyTypes(Map.of("tenant", FieldSpec.DataType.STRING));
    schema2.addField(spec2);

    assertNotEquals(schema1, schema2, "Schema.equals must detect keyTypes difference");
  }

  // ---- Test for fieldType: COMPLEX deserialization without explicit fieldType ----

  @Test
  public void testSchemaDeserializationWithoutFieldTypePreservesKeyTypes()
      throws Exception {
    // Simulate user-provided schema JSON without "fieldType": "COMPLEX"
    String schemaJson = "{"
        + "\"schemaName\": \"testSchema\","
        + "\"complexFieldSpecs\": [{"
        + "  \"name\": \"metadata\","
        + "  \"dataType\": \"MAP\","
        + "  \"keyTypes\": {\"tenant\": \"STRING\", \"count\": \"INT\"},"
        + "  \"defaultValueType\": \"STRING\""
        + "}]"
        + "}";

    Schema schema = Schema.fromString(schemaJson);
    FieldSpec fieldSpec = schema.getFieldSpecFor("metadata");
    assertNotNull(fieldSpec, "metadata field should be deserialized");
    assertTrue(fieldSpec instanceof ComplexFieldSpec, "Should be ComplexFieldSpec");

    ComplexFieldSpec complexSpec = (ComplexFieldSpec) fieldSpec;
    assertNotNull(complexSpec.getKeyTypes(), "keyTypes should be preserved");
    assertEquals(complexSpec.getKeyTypes().size(), 2);
    assertEquals(complexSpec.getKeyTypes().get("tenant"), FieldSpec.DataType.STRING);
    assertEquals(complexSpec.getKeyTypes().get("count"), FieldSpec.DataType.INT);
    assertEquals(complexSpec.getDefaultValueType(), FieldSpec.DataType.STRING);
  }

  @Test
  public void testSchemaDeserializationWithFieldTypePreservesKeyTypes()
      throws Exception {
    // Schema JSON with "fieldType": "COMPLEX" — the standard path
    String schemaJson = "{"
        + "\"schemaName\": \"testSchema\","
        + "\"complexFieldSpecs\": [{"
        + "  \"name\": \"metadata\","
        + "  \"dataType\": \"MAP\","
        + "  \"fieldType\": \"COMPLEX\","
        + "  \"keyTypes\": {\"tenant\": \"STRING\"},"
        + "  \"defaultValueType\": \"STRING\""
        + "}]"
        + "}";

    Schema schema = Schema.fromString(schemaJson);
    ComplexFieldSpec complexSpec = (ComplexFieldSpec) schema.getFieldSpecFor("metadata");
    assertNotNull(complexSpec.getKeyTypes());
    assertEquals(complexSpec.getKeyTypes().get("tenant"), FieldSpec.DataType.STRING);
    assertEquals(complexSpec.getDefaultValueType(), FieldSpec.DataType.STRING);
  }

  @Test
  public void testSchemaRoundTripPreservesKeyTypes()
      throws Exception {
    // Build schema programmatically with keyTypes
    Schema original = new Schema();
    original.setSchemaName("roundTripKeyTypes");
    ComplexFieldSpec spec = new ComplexFieldSpec("metrics", FieldSpec.DataType.MAP, true, Map.of());
    spec.setKeyTypes(Map.of("tenant", FieldSpec.DataType.STRING, "count", FieldSpec.DataType.INT));
    spec.setDefaultValueType(FieldSpec.DataType.STRING);
    original.addField(spec);

    // Serialize and deserialize
    String json = original.toPrettyJsonString();
    Schema reloaded = Schema.fromString(json);

    ComplexFieldSpec reloadedSpec = (ComplexFieldSpec) reloaded.getFieldSpecFor("metrics");
    assertNotNull(reloadedSpec);
    assertEquals(reloadedSpec.getKeyTypes(), spec.getKeyTypes());
    assertEquals(reloadedSpec.getDefaultValueType(), FieldSpec.DataType.STRING);

    // Schema.equals should confirm they're equal
    assertEquals(original, reloaded);
  }
}
