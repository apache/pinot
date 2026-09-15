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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.FieldSpec.FieldType;
import org.apache.pinot.spi.data.FieldSpec.MaxLengthExceedStrategy;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;


/// Protects the value and serialization contracts of field specs shared across segment loads.
public class FieldSpecFreezeTest {
  @Test
  public void testInheritedSettersCannotChangeFrozenKey() {
    FieldSpec spec = new DimensionFieldSpec("column", DataType.STRING, true, "missing");
    FieldSpec expected = new DimensionFieldSpec("column", DataType.STRING, true, "missing");
    int hash = spec.hashCode();
    assertSame(spec.freeze(), spec);
    assertSame(spec.freeze(), spec);
    List<Consumer<FieldSpec>> setters = List.of(
        value -> value.setName("changed"),
        value -> value.setDescription("changed"),
        value -> value.setTags(List.of("changed")),
        value -> value.setFieldId(7),
        value -> value.setAliases(List.of("changed")),
        value -> value.setMetadata(Map.of("key", "changed")),
        value -> value.setDataType(DataType.INT),
        value -> value.setSingleValueField(false),
        value -> value.setNullable(false),
        value -> value.setNotNull(true),
        value -> value.setMaxLength(100),
        value -> value.setMaxLengthExceedStrategy(MaxLengthExceedStrategy.ERROR),
        value -> value.setAllowTrailingZeros(true),
        value -> value.setDefaultNullValue((Object) "changed"),
        value -> value.setTransformFunction("changed"),
        value -> value.setVirtualColumnProvider("changed"));
    for (Consumer<FieldSpec> setter : setters) {
      assertThatThrownBy(() -> setter.accept(spec)).isInstanceOf(IllegalStateException.class);
      assertEquals(spec, expected);
      assertEquals(spec.hashCode(), hash);
    }
  }

  @Test
  public void testFreezeDetachesCollectionProperties() {
    List<String> tags = new ArrayList<>(List.of("tag"));
    List<String> aliases = new ArrayList<>(List.of("alias"));
    Map<String, String> metadata = new HashMap<>(Map.of("key", "value"));
    FieldSpec spec = new DimensionFieldSpec("column", DataType.STRING, true);
    spec.setTags(tags);
    spec.setAliases(aliases);
    spec.setMetadata(metadata);
    assertSame(spec.getTags(), tags);
    assertSame(spec.getAliases(), aliases);
    assertSame(spec.getMetadata(), metadata);
    spec.freeze();
    int hash = spec.hashCode();
    tags.clear();
    aliases.clear();
    metadata.clear();
    assertEquals(spec.getTags(), List.of("tag"));
    assertEquals(spec.getAliases(), List.of("alias"));
    assertEquals(spec.getMetadata(), Map.of("key", "value"));
    assertThatThrownBy(() -> spec.getTags().add("changed")).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> spec.getAliases().clear()).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> spec.getMetadata().put("key", "changed"))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> spec.getMetadata().entrySet().iterator().next().setValue("changed"))
        .isInstanceOf(UnsupportedOperationException.class);
    assertEquals(spec.hashCode(), hash);
  }

  @DataProvider
  public Object[][] byteDefaults() {
    return new Object[][]{
        {DataType.BYTES, "0102"},
        {DataType.UUID, "00112233-4455-6677-8899-aabbccddeeff"}
    };
  }

  @Test(dataProvider = "byteDefaults")
  public void testFrozenDefaultArraysAreDetached(DataType dataType, String literal) {
    FieldSpec spec = new DimensionFieldSpec("column", dataType, true, literal);
    byte[] beforeFreeze = (byte[]) spec.getDefaultNullValue();
    byte[] expected = beforeFreeze.clone();
    int hash = spec.hashCode();
    spec.freeze();
    beforeFreeze[0]++;
    byte[] afterFreeze = (byte[]) spec.getDefaultNullValue();
    assertEquals(afterFreeze, expected);
    afterFreeze[0]++;
    assertEquals((byte[]) spec.getDefaultNullValue(), expected);
    assertEquals(spec.hashCode(), hash);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testFrozenContainerDefaultsAreCopiedRecursively() {
    FieldSpec mapSpec = new DimensionFieldSpec("map", DataType.MAP, true, "{\"values\":[1,2]}");
    Map<String, Object> beforeFreeze = (Map<String, Object>) mapSpec.getDefaultNullValue();
    mapSpec.freeze();
    ((List<Integer>) beforeFreeze.get("values")).clear();
    Map<String, Object> afterFreeze = (Map<String, Object>) mapSpec.getDefaultNullValue();
    assertEquals(afterFreeze, Map.of("values", List.of(1, 2)));
    ((List<Integer>) afterFreeze.get("values")).clear();
    assertEquals(mapSpec.getDefaultNullValue(), Map.of("values", List.of(1, 2)));

    FieldSpec listSpec = new DimensionFieldSpec("list", DataType.LIST, true, "[{\"key\":1}]");
    List<Map<String, Integer>> originalList = (List<Map<String, Integer>>) listSpec.getDefaultNullValue();
    listSpec.freeze();
    originalList.get(0).clear();
    List<Map<String, Integer>> copiedList = (List<Map<String, Integer>>) listSpec.getDefaultNullValue();
    assertEquals(copiedList, List.of(Map.of("key", 1)));
    copiedList.get(0).clear();
    assertEquals(listSpec.getDefaultNullValue(), List.of(Map.of("key", 1)));
  }

  @Test
  public void testSubclassSettersCannotChangeFrozenKey() {
    MetricFieldSpec metric = new MetricFieldSpec("metric", DataType.LONG);
    metric.freeze();
    assertThatThrownBy(() -> metric.setSingleValueField(true)).isInstanceOf(IllegalStateException.class);

    DateTimeFieldSpec dateTime = new DateTimeFieldSpec("time", DataType.LONG, "1:MILLISECONDS:EPOCH",
        "1:MILLISECONDS");
    dateTime.freeze();
    List<Runnable> dateTimeSetters = List.of(
        () -> dateTime.setSingleValueField(true),
        () -> dateTime.setDataType(DataType.TIMESTAMP),
        () -> dateTime.setFormat("1:SECONDS:EPOCH"),
        () -> dateTime.setGranularity("1:SECONDS"),
        () -> dateTime.setSampleValue("1"));
    int hash = dateTime.hashCode();
    for (Runnable setter : dateTimeSetters) {
      assertThatThrownBy(setter::run).isInstanceOf(IllegalStateException.class);
      assertEquals(dateTime.hashCode(), hash);
    }
    // Derived caches remain readable and do not change the value used as an interner key.
    assertEquals(dateTime.getFormatSpec(), new DateTimeFormatSpec("1:MILLISECONDS:EPOCH"));
    assertEquals(dateTime.getGranularitySpec(), new DateTimeGranularitySpec("1:MILLISECONDS"));
    assertEquals(dateTime.hashCode(), hash);
  }

  @Test
  public void testTimeGranularityIsDetachedAndFrozen() {
    TimeGranularitySpec incoming = new TimeGranularitySpec(DataType.LONG, TimeUnit.MILLISECONDS, "incoming");
    TimeGranularitySpec outgoing = new TimeGranularitySpec(DataType.LONG, TimeUnit.SECONDS, "outgoing");
    TimeFieldSpec time = new TimeFieldSpec(incoming, outgoing);
    int hash = time.hashCode();
    time.freeze();
    assertNotSame(time.getIncomingGranularitySpec(), incoming);
    assertNotSame(time.getOutgoingGranularitySpec(), outgoing);
    incoming.setTimeType(TimeUnit.HOURS);
    outgoing.setName("changed");
    assertEquals(time.getIncomingGranularitySpec().getTimeType(), TimeUnit.MILLISECONDS);
    assertEquals(time.getOutgoingGranularitySpec().getName(), "outgoing");
    List<Runnable> setters = List.of(
        () -> time.setName("changed"),
        () -> time.setDataType(DataType.INT),
        () -> time.setSingleValueField(true),
        () -> time.setIncomingGranularitySpec(incoming),
        () -> time.setOutgoingGranularitySpec(outgoing));
    for (Runnable setter : setters) {
      assertThatThrownBy(setter::run).isInstanceOf(IllegalStateException.class);
    }
    for (TimeGranularitySpec nested : List.of(time.getIncomingGranularitySpec(), time.getOutgoingGranularitySpec())) {
      List<Runnable> nestedSetters = List.of(
          () -> nested.setName("changed"),
          () -> nested.setDataType(DataType.INT),
          () -> nested.setTimeType(TimeUnit.HOURS),
          () -> nested.setTimeUnitSize(2),
          () -> nested.setTimeunitSize(2),
          () -> nested.setTimeFormat("changed"));
      for (Runnable setter : nestedSetters) {
        assertThatThrownBy(setter::run).isInstanceOf(IllegalStateException.class);
      }
    }
    assertEquals(time.hashCode(), hash);
  }

  @Test
  public void testJsonCannotUpdateFrozenStateAndCopiesStayMutable() throws Exception {
    DimensionFieldSpec spec = new DimensionFieldSpec("column", DataType.BYTES, true, "0102");
    spec.setTags(new ArrayList<>(List.of("tag")));
    String beanJson = JsonUtils.objectToString(spec);
    Schema schema = new Schema.SchemaBuilder().addField(spec).build();
    String schemaJson = JsonUtils.objectToString(schema);
    spec.freeze();
    assertEquals(JsonUtils.objectToString(spec), beanJson);
    assertEquals(JsonUtils.objectToString(schema), schemaJson);
    assertFalse(JsonUtils.stringToJsonNode(beanJson).has("frozen"));
    for (String json : List.of("{\"name\":\"changed\"}", "{\"tags\":[\"changed\"]}",
        "{\"defaultNullValue\":\"0304\"}", "{\"frozen\":false,\"name\":\"changed\"}")) {
      assertThatThrownBy(() -> JsonUtils.DEFAULT_READER.forType(DimensionFieldSpec.class).withValueToUpdate(spec)
          .readValue(json)).hasRootCauseInstanceOf(IllegalStateException.class);
      assertEquals(JsonUtils.objectToString(spec), beanJson);
    }
    DimensionFieldSpec copy = JsonUtils.jsonNodeToObject(spec.toJsonObject(), DimensionFieldSpec.class);
    assertEquals(copy, spec);
    copy.setName("changed");
    copy.setDefaultNullValue((Object) "0304");
    copy.getTags().add("another");
    assertEquals(spec.getName(), "column");
    assertEquals(spec.getDefaultNullValueString(), "0102");
    assertEquals(spec.getTags(), List.of("tag"));
  }

  @Test
  public void testJavaSerializationCreatesMutableCopy() throws Exception {
    TimeFieldSpec spec = new TimeFieldSpec(new TimeGranularitySpec(DataType.LONG, TimeUnit.SECONDS, "time"));
    spec.freeze();
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
      output.writeObject(spec);
    }
    TimeFieldSpec copy;
    try (ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      copy = (TimeFieldSpec) input.readObject();
    }
    assertEquals(copy, spec);
    copy.setDescription("changed");
    copy.getIncomingGranularitySpec().setTimeType(TimeUnit.HOURS);
    assertEquals(spec.getIncomingGranularitySpec().getTimeType(), TimeUnit.SECONDS);
  }

  @Test
  public void testUnsupportedSpecsAndMutableSamplesAreRejected() {
    ComplexFieldSpec complex = new ComplexFieldSpec("map", DataType.MAP, true, Map.of());
    assertThatThrownBy(complex::freeze).isInstanceOf(IllegalStateException.class);
    FieldSpec custom = new FieldSpec("custom", DataType.STRING, true) {
      @Override
      public FieldType getFieldType() {
        return FieldType.DIMENSION;
      }
    };
    assertThatThrownBy(custom::freeze).isInstanceOf(IllegalStateException.class);
    TimeGranularitySpec customGranularity = new TimeGranularitySpec(DataType.LONG, TimeUnit.SECONDS, "time") {
    };
    assertThatThrownBy(customGranularity::freeze).isInstanceOf(IllegalStateException.class);
    DateTimeFieldSpec dateTime = new DateTimeFieldSpec("time", DataType.LONG, "1:MILLISECONDS:EPOCH",
        "1:MILLISECONDS", (Object) new byte[]{1});
    assertThatThrownBy(dateTime::freeze).isInstanceOf(IllegalStateException.class);
    dateTime.setSampleValue("1");
    assertSame(dateTime.freeze(), dateTime);
  }
}
