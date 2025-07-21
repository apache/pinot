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
package org.apache.pinot.plugin.inputformat.protobuf;

import com.google.protobuf.Descriptors;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.plugin.inputformat.protobuf.ProtoBufTestDataGenerator.*;
import static org.testng.Assert.assertEquals;


public class ProtoBufSchemaUtilsTest {

  @DataProvider(name = "scalarCases")
  public Object[][] scalarCases() {
    return new Object[][]{
        new Object[] {STRING_FIELD, FieldSpec.DataType.STRING, true},
        new Object[] {NULLABLE_STRING_FIELD, FieldSpec.DataType.STRING, true},
        new Object[] {REPEATED_STRINGS, FieldSpec.DataType.STRING, false},

        new Object[] {INT_FIELD, FieldSpec.DataType.INT, true},
        new Object[] {NULLABLE_INT_FIELD, FieldSpec.DataType.INT, true},
        new Object[] {REPEATED_INTS, FieldSpec.DataType.INT, false},

        new Object[] {LONG_FIELD, FieldSpec.DataType.LONG, true},
        new Object[] {NULLABLE_LONG_FIELD, FieldSpec.DataType.LONG, true},
        new Object[] {REPEATED_LONGS, FieldSpec.DataType.LONG, false},

        new Object[] {DOUBLE_FIELD, FieldSpec.DataType.DOUBLE, true},
        new Object[] {NULLABLE_DOUBLE_FIELD, FieldSpec.DataType.DOUBLE, true},
        new Object[] {REPEATED_DOUBLES, FieldSpec.DataType.DOUBLE, false},

        new Object[] {FLOAT_FIELD, FieldSpec.DataType.FLOAT, true},
        new Object[] {NULLABLE_FLOAT_FIELD, FieldSpec.DataType.FLOAT, true},
        new Object[] {REPEATED_FLOATS, FieldSpec.DataType.FLOAT, false},

        new Object[] {BYTES_FIELD, FieldSpec.DataType.BYTES, true},
        new Object[] {NULLABLE_BYTES_FIELD, FieldSpec.DataType.BYTES, true},
        new Object[] {REPEATED_BYTES, FieldSpec.DataType.BYTES, false},

        new Object[] {BOOL_FIELD, FieldSpec.DataType.BOOLEAN, true},
        new Object[] {NULLABLE_BOOL_FIELD, FieldSpec.DataType.BOOLEAN, true},
        new Object[] {REPEATED_BOOLS, FieldSpec.DataType.BOOLEAN, false},

        new Object[] {ENUM_FIELD, FieldSpec.DataType.STRING, true},
        new Object[] {REPEATED_ENUMS, FieldSpec.DataType.STRING, false},
    };
  }

  @Test(dataProvider = "scalarCases")
  public void testExtractSchemaWithComplexTypeHandling(
      String fieldName, FieldSpec.DataType type, boolean isSingleValue) {
    Descriptors.FieldDescriptor desc = ComplexTypes.TestMessage.getDescriptor().findFieldByName(fieldName);
    Schema schema = new Schema();
    ProtoBufSchemaUtils.extractSchemaWithComplexTypeHandling(
        desc,
        Collections.emptyList(),
        ".",
        desc.getName(),
        schema,
        new HashMap<>(),
        TimeUnit.SECONDS);
    Schema expectedSchema;
    if (isSingleValue) {
      expectedSchema = new Schema.SchemaBuilder()
          .addSingleValueDimension(fieldName, type)
          .build();
    } else {
      expectedSchema = new Schema.SchemaBuilder()
          .addMultiValueDimension(fieldName, type)
          .build();
    }
    assertEquals(schema, expectedSchema);
  }

  @Test
  public void testExtractSchemaWithComplexTypeHandlingNestedMessage() {
    Descriptors.FieldDescriptor desc = ComplexTypes.TestMessage.getDescriptor().findFieldByName(NESTED_MESSAGE);
    Schema schema = new Schema();
    ProtoBufSchemaUtils.extractSchemaWithComplexTypeHandling(
        desc,
        Collections.emptyList(),
        ".",
        desc.getName(),
        schema,
        new HashMap<>(),
        TimeUnit.SECONDS);
    Schema expectedSchema = new Schema.SchemaBuilder()
          .addSingleValueDimension("nested_message.nested_string_field", FieldSpec.DataType.STRING)
          .addSingleValueDimension("nested_message.nested_int_field", FieldSpec.DataType.INT)
          .build();
    assertEquals(schema, expectedSchema);

    schema = new Schema();
    ProtoBufSchemaUtils.extractSchemaWithComplexTypeHandling(
        desc,
        Collections.emptyList(),
        "__",
        desc.getName(),
        schema,
        new HashMap<>(),
        TimeUnit.SECONDS);
    expectedSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("nested_message__nested_string_field", FieldSpec.DataType.STRING)
        .addSingleValueDimension("nested_message__nested_int_field", FieldSpec.DataType.INT)
        .build();
    assertEquals(schema, expectedSchema);

    desc = ComplexTypes.TestMessage.getDescriptor().findFieldByName(REPEATED_NESTED_MESSAGES);
    schema = new Schema();
    ProtoBufSchemaUtils.extractSchemaWithComplexTypeHandling(
        desc,
        Collections.emptyList(),
        "__",
        desc.getName(),
        schema,
        new HashMap<>(),
        TimeUnit.SECONDS);
    expectedSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension(REPEATED_NESTED_MESSAGES, FieldSpec.DataType.STRING)
        .build();
    assertEquals(schema, expectedSchema);

    schema = new Schema();
    ProtoBufSchemaUtils.extractSchemaWithComplexTypeHandling(
        desc,
        Collections.singletonList(REPEATED_NESTED_MESSAGES),
        "__",
        desc.getName(),
        schema,
        new HashMap<>(),
        TimeUnit.SECONDS);
    expectedSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("repeated_nested_messages__nested_string_field", FieldSpec.DataType.STRING)
        .addSingleValueDimension("repeated_nested_messages__nested_int_field", FieldSpec.DataType.INT)
        .build();
    assertEquals(schema, expectedSchema);
  }

  @Test(dataProvider = "scalarCases")
  public void testExtractSchemaWithCompositeTypeHandling(
      String fieldName, FieldSpec.DataType type, boolean isSingleValue) {
    Descriptors.Descriptor desc = CompositeTypes.CompositeMessage.getDescriptor();
    FieldSpec schema = ProtoBufSchemaUtils.getPinotSchemaFromPinotSchemaWithComplexTypeHandling(
        desc,
        new HashMap<>(),
        TimeUnit.SECONDS,
        Collections.emptyList(),
        ".").getFieldSpecFor("test_message." + fieldName);
    FieldSpec expectedSchema = new DimensionFieldSpec("test_message." + fieldName, type, isSingleValue);
    assertEquals(schema, expectedSchema);
  }

  @Test
  public void testExtractSchemaWithCompositeTypeHandlingWithTimeColumnAndFieldSpec() {
    Descriptors.Descriptor desc = CompositeTypes.CompositeMessage.getDescriptor();
    Map<String, FieldSpec.FieldType> fieldTypeMap = new HashMap<>();
    fieldTypeMap.put("test_message.long_field", FieldSpec.FieldType.DATE_TIME);
    fieldTypeMap.put("test_message.int_field", FieldSpec.FieldType.METRIC);
    fieldTypeMap.put("test_message.double_field", FieldSpec.FieldType.METRIC);
    Schema schema = ProtoBufSchemaUtils.getPinotSchemaFromPinotSchemaWithComplexTypeHandling(
        desc,
        fieldTypeMap,
        TimeUnit.SECONDS,
        Collections.emptyList(),
        ".");
    FieldSpec fieldSpec = schema.getFieldSpecFor("test_message.long_field");
    FieldSpec expectedFieldSpec = new DateTimeFieldSpec("test_message.long_field", FieldSpec.DataType.LONG,
        "1:SECONDS:EPOCH", "1:SECONDS");
    assertEquals(fieldSpec, expectedFieldSpec);

    fieldSpec = schema.getFieldSpecFor("test_message.int_field");
    expectedFieldSpec = new MetricFieldSpec("test_message.int_field", FieldSpec.DataType.INT);
    assertEquals(fieldSpec, expectedFieldSpec);

    fieldSpec = schema.getFieldSpecFor("test_message.double_field");
    expectedFieldSpec = new MetricFieldSpec("test_message.double_field", FieldSpec.DataType.DOUBLE);
    assertEquals(fieldSpec, expectedFieldSpec);
  }

  @Test
  public void testGetPinotSchemaFromPinotSchemaWithComplexTypeHandling()
      throws URISyntaxException, IOException {
    Descriptors.Descriptor desc = CompositeTypes.CompositeMessage.getDescriptor();
    Map<String, FieldSpec.FieldType> fieldTypeMap = new HashMap<>();
    fieldTypeMap.put("test_message.long_field", FieldSpec.FieldType.DATE_TIME);
    Schema schema = ProtoBufSchemaUtils.getPinotSchemaFromPinotSchemaWithComplexTypeHandling(
        desc,
        fieldTypeMap,
        TimeUnit.MILLISECONDS,
        Collections.emptyList(),
        ".");
    URL resource = getClass().getClassLoader().getResource("complex_type_schema.json");
    Schema expectedSchema = Schema.fromString(new String(Files.readAllBytes(Paths.get(resource.toURI()))));
    assertEquals(schema, expectedSchema);
  }
}
