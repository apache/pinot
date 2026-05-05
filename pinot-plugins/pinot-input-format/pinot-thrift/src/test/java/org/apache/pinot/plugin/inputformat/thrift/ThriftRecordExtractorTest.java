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
package org.apache.pinot.plugin.inputformat.thrift;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


/// Tests [ThriftRecordExtractor] — see its class Javadoc for the thrift source type → Java output type
/// matrix. Out of scope: thrift `i8` / `binary` are not declared in `complex_types.thrift`; they follow the
/// same `convert` contract — mirror the pattern below if added.
public class ThriftRecordExtractorTest {

  // === Single value — order follows the type list in the class Javadoc ===

  @Test
  public void testBooleanPreserved() {
    ComplexTypes record = baseRecord();
    record.setBooleanField(true);
    Object result = extract(record, "booleanField");
    assertEquals(result, true);
  }

  @Test
  public void testShortWidenedToInteger() {
    // ComplexTypes has no top-level i16 field; ThriftSampleData uses i16 as list element type. The extractor
    // widens `Short` → `Integer` so all small-int thrift types unify behind `Integer`.
    ThriftSampleData record = new ThriftSampleData();
    record.setActive(false);
    record.setCreated_at(0L);
    record.setId(0);
    record.setGroups(List.of((short) 1, (short) 4));
    Object[] result = (Object[]) extract(record, "groups");
    assertEquals(result.length, 2);
    assertEquals(result, new Object[]{1, 4});
  }

  @Test
  public void testIntegerPreserved() {
    ComplexTypes record = baseRecord();
    record.setIntField(42);
    Object result = extract(record, "intField");
    assertEquals(result, 42);
  }

  @Test
  public void testLongPreserved() {
    ComplexTypes record = baseRecord();
    record.setLongField(1_588_469_340_000L);
    Object result = extract(record, "longField");
    assertEquals(result, 1_588_469_340_000L);
  }

  @Test
  public void testDoublePreserved() {
    ComplexTypes record = baseRecord();
    record.setDoubleField(1.5d);
    Object result = extract(record, "doubleField");
    assertEquals(result, 1.5d);
  }

  @Test
  public void testStringPreserved() {
    ComplexTypes record = baseRecord();
    record.setStringField("hello");
    Object result = extract(record, "stringField");
    assertEquals(result, "hello");
  }

  @Test
  public void testEnumExtractedAsString() {
    // BaseRecordExtractor.convertSingleValue falls back to toString() for unknown types; TestEnum.toString()
    // returns the enum constant name.
    ComplexTypes record = baseRecord();
    record.setEnumField(TestEnum.GAMMA);
    Object result = extract(record, "enumField");
    assertEquals(result, "GAMMA");
  }

  @Test
  public void testNullForUnsetOptionalField() {
    // optionalStringField is left unset; thrift returns null for unset optional fields.
    assertNull(extract(baseRecord(), "optionalStringField"));
  }

  // === Nested struct → Map ===

  @Test
  public void testNestedStructExtractedAsMap() {
    ComplexTypes record = baseRecord();
    record.setNestedStructField(nestedOf("hello", 42));
    Map<?, ?> result = (Map<?, ?>) extract(record, "nestedStructField");
    assertEquals(result.get("nestedStringField"), "hello");
    assertEquals(result.get("nestedIntField"), 42);
  }

  // === List / Set (thrift list / set) → Object[] ===

  @Test
  public void testListOfStrings() {
    ComplexTypes record = baseRecord();
    record.setSimpleListField(List.of("a", "b", "c"));
    Object[] result = (Object[]) extract(record, "simpleListField");
    assertEquals(result, new Object[]{"a", "b", "c"});
  }

  @Test
  public void testListOfStructs() {
    ComplexTypes record = baseRecord();
    record.setComplexListField(List.of(
        nestedOf("a", 1),
        nestedOf("b", 2)
    ));
    Object[] result = (Object[]) extract(record, "complexListField");
    assertEquals(result.length, 2);
    Map<?, ?> r0 = (Map<?, ?>) result[0];
    assertEquals(r0.get("nestedStringField"), "a");
    assertEquals(r0.get("nestedIntField"), 1);
    Map<?, ?> r1 = (Map<?, ?>) result[1];
    assertEquals(r1.get("nestedStringField"), "b");
    assertEquals(r1.get("nestedIntField"), 2);
  }

  @Test
  public void testSetOfStrings() {
    // ComplexTypes has no set<> field; ThriftSampleData has set<string>. Sets convert to Object[] like lists.
    ThriftSampleData record = new ThriftSampleData();
    record.setActive(false);
    record.setCreated_at(0L);
    record.setId(0);
    record.setSet_values(Set.of("alpha", "beta"));
    Object[] result = (Object[]) extract(record, "set_values");
    assertEquals(result.length, 2);
    // Set has no defined order — assert content via a set
    assertEquals(Set.of(result), Set.of("alpha", "beta"));
  }

  @Test
  public void testEmptyListExtractedAsEmptyArray() {
    ComplexTypes record = baseRecord();
    record.setSimpleListField(List.of());
    Object[] result = (Object[]) extract(record, "simpleListField");
    assertEquals(result, new Object[]{});
  }

  // === Map (thrift map) ===

  @Test
  public void testMapStringToInt() {
    ComplexTypes record = baseRecord();
    record.setSimpleMapField(Map.of(
        "a", 1,
        "b", 2
    ));
    Map<?, ?> result = (Map<?, ?>) extract(record, "simpleMapField");
    assertEquals(result.size(), 2);
    assertEquals(result.get("a"), 1);
    assertEquals(result.get("b"), 2);
  }

  @Test
  public void testMapStringToStruct() {
    ComplexTypes record = baseRecord();
    record.setComplexMapField(Map.of(
        "x", nestedOf("hello", 10),
        "y", nestedOf("world", 20)
    ));
    Map<?, ?> result = (Map<?, ?>) extract(record, "complexMapField");
    assertEquals(result.size(), 2);
    Map<?, ?> x = (Map<?, ?>) result.get("x");
    assertEquals(x.get("nestedStringField"), "hello");
    assertEquals(x.get("nestedIntField"), 10);
    Map<?, ?> y = (Map<?, ?>) result.get("y");
    assertEquals(y.get("nestedStringField"), "world");
    assertEquals(y.get("nestedIntField"), 20);
  }

  // === Helpers ===

  /// Run the extractor with `null` field-set (extract-all) and return the value of the named column. Builds
  /// the field-id map by introspecting the generated TBase class (same logic used by [ThriftRecordReader]).
  private static <T extends TBase<?, ?>> Object extract(T record, String column) {
    ThriftRecordExtractorConfig config = new ThriftRecordExtractorConfig();
    config.setFieldIds(buildFieldIdMap(record));
    ThriftRecordExtractor extractor = new ThriftRecordExtractor();
    extractor.init(null, config);
    GenericRow row = new GenericRow();
    extractor.extract(record, row);
    return row.getValue(column);
  }

  @SuppressWarnings("rawtypes")
  private static Map<String, Integer> buildFieldIdMap(TBase record) {
    Map<? extends TFieldIdEnum, FieldMetaData> metaDataMap =
        FieldMetaData.getStructMetaDataMap(record.getClass());
    Map<String, Integer> fieldIds = new HashMap<>();
    for (TFieldIdEnum field : metaDataMap.keySet()) {
      fieldIds.put(field.getFieldName(), Short.toUnsignedInt(field.getThriftFieldId()));
    }
    return fieldIds;
  }

  /// Builds a [ComplexTypes] with all required fields populated to defaults so the extract-all loop doesn't
  /// trip over unset required fields. Tests override only the field they care about.
  private static ComplexTypes baseRecord() {
    ComplexTypes record = new ComplexTypes();
    record.setIntField(0);
    record.setLongField(0L);
    record.setBooleanField(false);
    record.setDoubleField(0.0d);
    record.setStringField("");
    record.setEnumField(TestEnum.ALPHA);
    record.setNestedStructField(nestedOf("", 0));
    record.setSimpleListField(List.of());
    record.setComplexListField(List.of());
    return record;
  }

  private static NestedType nestedOf(String stringValue, int intValue) {
    NestedType n = new NestedType();
    n.setNestedStringField(stringValue);
    n.setNestedIntField(intValue);
    return n;
  }
}
