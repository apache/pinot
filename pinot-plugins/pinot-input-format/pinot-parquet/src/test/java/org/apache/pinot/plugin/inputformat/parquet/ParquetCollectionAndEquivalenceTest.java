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
package org.apache.pinot.plugin.inputformat.parquet;

import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.file.Files;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.MapKeyValueTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema.SchemaBuilder;
import org.apache.pinot.spi.data.readers.ColumnReader;
import org.apache.pinot.spi.data.readers.ColumnReaderFactory;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.MapUtils;
import org.apache.pinot.spi.utils.PinotDataType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


/// Tests Parquet collection-wrapper handling end-to-end:
/// - Specific value assertions for the standard 3-level LIST encoding, MAP encoding, nested structs, and all
///   four Parquet LogicalTypes spec backward-compat rules for LIST element resolution (via a hand-authored
///   `MessageType`).
/// - Specific value assertions for the checked-in `collection-reader-fixture.parquet` (primitives, DECIMAL /
///   DATE / TIMESTAMP, nested structs, struct lists, map of lists, real `element`-named struct field).
/// - Regression for nested `LIST<LIST<STRING>>` unwrapping through the Avro reader, including the two null
///   branches (null inner element, null inner wrapper).
/// - Regression for user-authored `array<record<UserTag, [element: string]>>` — the inner records must NOT
///   be flattened (the Avro shape matches the parquet-avro wrapper convention, but the file's Avro schema in
///   metadata identifies the records as user data).
/// - Cross-reader equivalence on every existing `.parquet` test resource — `ParquetAvroRecordReader` and
///   `ParquetNativeRecordReader` must produce structurally identical Pinot rows. Locks in that any encoding
///   subtlety in the fixtures is interpreted the same way by both code paths.
///
/// The hand-authored schema is asserted via the native reader only; the cross-reader equivalence test
/// ensures the Avro reader produces the same rows on the same file.
public class ParquetCollectionAndEquivalenceTest {
  private static final String SCHEMA = """
      message CollectionWrapperExample {
        optional group topLevelTags (LIST) {
          repeated group list {
            optional binary element (STRING);
          }
        }
        optional group topLevelProperties (MAP) {
          repeated group key_value {
            required binary key (STRING);
            optional binary value (STRING);
          }
        }
        optional group emptyProperties (MAP) {
          repeated group key_value {
            required binary key (STRING);
            optional binary value (STRING);
          }
        }
        optional group metadata {
          optional binary element (STRING);
          optional group tags (LIST) {
            repeated group list {
              optional binary element (STRING);
            }
          }
          optional group nullableTags (LIST) {
            repeated group list {
              optional binary element (STRING);
            }
          }
          optional group legacySingleFieldStructs (LIST) {
            repeated group list {
              optional binary item (STRING);
            }
          }
          optional group legacyMultiFieldStructs (LIST) {
            repeated group list {
              optional binary name (STRING);
              optional int32 score;
            }
          }
          optional group tagStructs (LIST) {
            repeated group list {
              optional group element {
                optional binary element (STRING);
              }
            }
          }
          optional group properties (MAP) {
            repeated group key_value {
              required binary key (STRING);
              optional binary value (STRING);
            }
          }
        }
      }
      """;

  private final File _tempDir = new File(FileUtils.getTempDirectory(), getClass().getSimpleName());

  @AfterClass
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(_tempDir);
  }

  @Test
  public void testHandAuthoredSchemaCollectionValues()
      throws IOException {
    File dataFile = writeHandAuthoredParquetFile();
    try (ParquetNativeRecordReader recordReader = new ParquetNativeRecordReader()) {
      recordReader.init(dataFile, null, null);
      assertHandAuthoredCollectionValues(recordReader);
    }
  }

  @Test
  public void testCollectionFixture()
      throws IOException {
    File dataFile = new File(getClass().getClassLoader().getResource("collection-reader-fixture.parquet").getFile());
    try (ParquetRecordReader recordReader = new ParquetRecordReader()) {
      recordReader.init(dataFile, null, null);
      assertFalse(recordReader.useAvroParquetRecordReader());
      assertFixtureValues(recordReader);
    }
  }

  @DataProvider
  public Object[][] directReaderModes() {
    return new Object[][]{{false, false}, {true, false}, {false, true}, {true, true}};
  }

  @Test(dataProvider = "directReaderModes")
  public void testDirectReaderCollectionsAndRowGroups(boolean dictionary, boolean rawTimeValues)
      throws IOException {
    File dataFile = writeDirectReaderFixture(dictionary);
    ParquetRecordReaderConfig groupConfig = new ParquetRecordReaderConfig();
    groupConfig.setExtractRawTimeValues(rawTimeValues);
    ParquetRecordReaderConfig directConfig = new ParquetRecordReaderConfig();
    directConfig.setExtractRawTimeValues(rawTimeValues);
    directConfig.setUseParquetNativeDirectReader(true);
    try (ParquetNativeRecordReader groupReader = new ParquetNativeRecordReader();
        ParquetNativeRecordReader directReader = new ParquetNativeRecordReader()) {
      List<GenericRow> expected = readAll(groupReader, dataFile, groupConfig);
      assertFalse(groupReader.isDirectRecordReader());
      List<GenericRow> actual = readAll(directReader, dataFile, directConfig);
      assertTrue(directReader.isDirectRecordReader());
      assertEquals(actual.size(), 1000);
      assertReadersAgree(dataFile.getName(), expected, actual);
      for (int i = 0; i < actual.size(); i++) {
        Object expectedMap = expected.get(i).getValue("attributes");
        Object actualMap = actual.get(i).getValue("attributes");
        if (actualMap != null) {
          // Pinot JSON conversion sorts keys; direct materialization must preserve its canonical output.
          assertEquals(MapUtils.toString((Map<String, Object>) actualMap),
              MapUtils.toString((Map<String, Object>) expectedMap));
        }
      }
      assertNull(actual.get(0).getValue("attributes"));
      assertEquals(actual.get(1).getValue("attributes"), Map.of());
      Map<String, Object> populatedMap = (Map<String, Object>) actual.get(2).getValue("attributes");
      assertEquals(populatedMap.get("z"), "last\\\"\n雪");
      assertTrue(populatedMap.containsKey("null"));
      assertNull(populatedMap.get("null"));
      assertNull(actual.get(0).getValue("tags"));
      assertEquals(actual.get(1).getValue("tags"), new Object[0]);
      assertEquals(actual.get(2).getValue("tags"), new Object[]{"tag", null});

      directReader.rewind();
      assertReadersAgree("rewind", actual, readRows(directReader));

      MessageType fileSchema;
      try (ParquetFileReader fileReader = ParquetFileReader.open(HadoopInputFile.fromPath(
          new Path(dataFile.getAbsolutePath()), ParquetUtils.getParquetHadoopConfiguration()))) {
        fileSchema = fileReader.getFooter().getFileMetaData().getSchema();
      }
      Set<String> selected = Set.of("timestampNanos", "flags");
      assertTrue(directReader.rewind(selected));
      for (int i = 0; i < actual.size(); i++) {
        assertTrue(directReader.hasNext());
        // The row group's physical pages, including nested MAP/LIST leaves, must exclude every omitted field.
        for (ColumnDescriptor column : fileSchema.getColumns()) {
          if (selected.contains(column.getPath()[0])) {
            assertNotNull(directReader.getPageReadStore().getPageReader(column));
          } else {
            assertThrows(IllegalArgumentException.class, () -> directReader.getPageReadStore().getPageReader(column));
          }
        }
        GenericRow projected = directReader.next();
        assertEquals(projected.getFieldToValueMap().keySet(), selected);
        for (String field : selected) {
          assertEquals(projected.getValue(field), actual.get(i).getValue(field));
        }
      }
      assertFalse(directReader.hasNext());
      directReader.rewind();
      assertReadersAgree("restore after physical projection", actual, readRows(directReader));
      assertTrue(directReader.rewind(Set.of()));
      assertReadersAgree("empty projection means original fields", actual, readRows(directReader));
    }
    try (ParquetNativeRecordReader projected = new ParquetNativeRecordReader()) {
      Set<String> initialFields = new HashSet<>(Set.of("timestampNanos", "attributes", "absent"));
      projected.init(dataFile, initialFields, directConfig);
      initialFields.add("body");
      assertTrue(projected.rewind(Set.of("timestampNanos", "body")));
      assertEquals(projected.next().getFieldToValueMap().keySet(), Set.of("timestampNanos"));
      assertFalse(projected.rewind(Set.of("body", "unknown")));
      assertEquals(projected.next().getValue("timestampNanos"), actualTimestamp(1, rawTimeValues));
      projected.rewind();
      GenericRow reuse = new GenericRow();
      for (int i = 0; i < 1000; i++) {
        reuse.clear();
        assertTrue(projected.hasNext());
        projected.next(reuse);
        assertEquals(reuse.getFieldToValueMap().keySet(), Set.of("timestampNanos", "attributes"));
      }
      assertFalse(projected.hasNext());
    }
    try (ParquetRecordReader wrapper = new ParquetRecordReader()) {
      directConfig.setUseParquetNativeRecordReader(true);
      wrapper.init(dataFile, null, directConfig);
      assertTrue(wrapper.rewind(Set.of("flags")));
      assertEquals(wrapper.next().getFieldToValueMap().keySet(), Set.of("flags"));
      wrapper.rewind();
      assertTrue(wrapper.next().getFieldToValueMap().containsKey("body"));
    }
    try (ParquetRecordReader avro = new ParquetRecordReader()) {
      ParquetRecordReaderConfig avroConfig = new ParquetRecordReaderConfig();
      avroConfig.setUseParquetAvroRecordReader(true);
      avroConfig.setUseParquetNativeDirectReader(true);
      avro.init(dataFile, null, avroConfig);
      assertFalse(avro.rewind(Set.of("flags")));
    }
    try (ParquetNativeRecordReader missingOnly = new ParquetNativeRecordReader()) {
      missingOnly.init(dataFile, Set.of("absent"), directConfig);
      assertTrue(missingOnly.next().getFieldToValueMap().isEmpty());
      assertFalse(missingOnly.rewind(Set.of("body")));
      missingOnly.rewind();
      assertEquals(readRows(missingOnly).size(), 1000);
      missingOnly.rewind();
      assertTrue(missingOnly.next().getFieldToValueMap().isEmpty());
    }
  }

  @DataProvider
  public Object[][] columnarReaderModes() {
    List<Object[]> modes = new ArrayList<>();
    for (String annotation : List.of("", "MAP_KEY_VALUE", "MAP")) {
      for (Object[] mode : directReaderModes()) {
        modes.add(new Object[]{mode[0], mode[1], annotation});
      }
    }
    return modes.toArray(new Object[0][]);
  }

  @Test(dataProvider = "columnarReaderModes")
  @SuppressWarnings("unchecked")
  public void testColumnarReadersPreserveRawValuesAndOwnTheirResources(boolean dictionary, boolean rawTimeValues,
      String entryAnnotation)
      throws IOException {
    File file = writeColumnarReaderFixture(dictionary, entryAnnotation);
    var schema = columnarSchema().build();
    ParquetRecordReaderConfig config = columnarConfig();
    config.setExtractRawTimeValues(rawTimeValues);
    List<GenericRow> expected;
    try (ParquetNativeRecordReader reader = new ParquetNativeRecordReader()) {
      expected = readAll(reader, file, config);
    }
    ColumnReaderFactory factory;
    try (ParquetRecordReader reader = new ParquetRecordReader()) {
      Set<String> sourceFields = new HashSet<>(schema.getPhysicalColumnNames());
      reader.init(file, sourceFields, config);
      // The capability snapshots initial options/restrictions, rather than retaining mutable caller state.
      sourceFields.clear();
      config.setUseColumnMajorSegmentBuilder(false);
      config.setExtractRawTimeValues(!rawTimeValues);
      factory = reader.tryCreateColumnReaderFactory(schema);
      assertNotNull(factory);
      assertThrows(IllegalStateException.class, factory::getAllColumnReaders);
      assertEquals(reader.next().getValue("eventId"), expected.get(0).getValue("eventId"),
          "Offering column readers must not consume or rewind the row reader");
    }
    // Closing the original row reader before factory initialization must leave the new readers usable.
    Map<String, ColumnReader> columns;
    try (factory) {
      factory.init(schema);
      columns = factory.getAllColumnReaders();
      assertEquals(columns.keySet(), schema.getPhysicalColumnNames());
      assertTrue(factory.getAvailableColumns().contains("unused"));
      for (Map.Entry<String, ColumnReader> entry : columns.entrySet()) {
        String name = entry.getKey();
        ColumnReader column = entry.getValue();
        assertEquals(column.getTotalDocs(), expected.size());
        boolean json = schema.getFieldSpecFor(name).getDataType() == DataType.JSON;
        assertEquals(column.isCanonicalJson(), json);
        assertEquals(column.getValueType(), json ? PinotDataType.STRING : null);
        for (int doc = 0; doc < expected.size(); doc++) {
          assertFalse(column.isNull(doc));
          Object actual = column.getValue(doc);
          assertSame(column.getValue(doc), actual, "Repeated access must not advance the reader");
          Object expectedValue = expected.get(doc).getValue(name);
          if (json) {
            expectedValue = MapUtils.toString((Map<String, Object>) expectedValue);
            assertSame(column.getString(doc), actual);
          }
          assertEquals(wrapArrayValue(actual), wrapArrayValue(expectedValue), name + ":" + doc);
        }
        assertThrows(IndexOutOfBoundsException.class, () -> column.getValue(expected.size()));
        column.rewind();
        Object first = expected.get(0).getValue(name);
        if (json) {
          first = MapUtils.toString((Map<String, Object>) first);
        }
        assertEquals(wrapArrayValue(column.getValue(0)), wrapArrayValue(first));
        column.getValue(42);
        assertThrows(UnsupportedOperationException.class, () -> column.getValue(41));
        assertEquals(wrapArrayValue(column.getValue(0)), wrapArrayValue(first),
            "A new sequential pass starts from zero");
      }
      ColumnReader maps = columns.get("properties");
      Object retained = maps.getValue(2);
      maps.getValue(3);
      assertEquals(retained, MapUtils.toString((Map<String, Object>) expected.get(2).getValue("properties")),
          "Later rows must not change an owned canonical JSON string");
    }
    factory.close();
    for (ColumnReader column : columns.values()) {
      column.close();
      assertThrows(IllegalStateException.class, () -> column.getValue(0));
    }
  }

  @Test
  public void testCanonicalJsonDictionaryEpochsPlainPagesAndFallback() {
    MessageType schema = MessageTypeParser.parseMessageType("""
        message JsonMap {
          required group properties (MAP) {
            repeated group entries {
              required binary key (STRING);
              required binary value (STRING);
            }
          }
        }
        """);
    ParquetNativeRecordMaterializer materializer =
        new ParquetNativeRecordMaterializer(schema, Set.of("properties"), false, Set.of("properties"));
    materializer.setRecord(new GenericRow());
    GroupConverter entries = materializer.getRootConverter().getConverter(0).asGroupConverter()
        .getConverter(0).asGroupConverter();
    PrimitiveConverter keys = entries.getConverter(0).asPrimitiveConverter();
    PrimitiveConverter values = entries.getConverter(1).asPrimitiveConverter();
    keys.setDictionary(stringDictionary("a", "z"));
    values.setDictionary(stringDictionary("before", "雪\"\\\n", "last"));
    String dictionary = materializeMap(materializer, 1, 0, 0, 1, 1, 2);
    assertEquals(dictionary, MapUtils.toString(Map.of("a", "雪\"\\\n", "z", "last")));
    assertSame(materializeMap(materializer, 1, 0, 0, 1, 1, 2), dictionary);
    String plain = materializeMap(materializer, "z", "before", "a", "雪\"\\\n", "z", "last");
    assertEquals(plain, dictionary);
    assertSame(materializeMap(materializer, "z", "before", "a", "雪\"\\\n", "z", "last"), plain,
        "PLAIN pages must retain canonical caching");
    String mixed = materializeMap(materializer, 0, "plain-value", "plain-key", 2);
    assertEquals(mixed, MapUtils.toString(Map.of("a", "plain-value", "plain-key", "last")));
    assertSame(materializeMap(materializer, 0, "plain-value", "plain-key", 2), mixed);
    String collision = materializeMap(materializer, "a", "Aa");
    assertEquals(materializeMap(materializer, "a", "BB"), "{\"a\":\"BB\"}");
    assertSame(materializeMap(materializer, "a", "Aa"), collision, "Hash collisions must compare all tokens");
    assertEquals(materializeMap(materializer, 0, 0), "{\"a\":\"before\"}");
    values.setDictionary(stringDictionary("changed"));
    assertEquals(materializeMap(materializer, 0, 0), "{\"a\":\"changed\"}");
    keys.setDictionary(stringDictionary("new-key"));
    assertEquals(materializeMap(materializer, 0, 0), "{\"new-key\":\"changed\"}");
    assertEquals(materializeMap(materializer), "{}");
    Object[] oversizedSequence = new Object[600];
    for (int i = 0; i < oversizedSequence.length; i += 2) {
      oversizedSequence[i] = "duplicate";
      oversizedSequence[i + 1] = i + 2 == oversizedSequence.length ? "last" : "before";
    }
    String fallback = materializeMap(materializer, oversizedSequence);
    assertEquals(fallback, "{\"duplicate\":\"last\"}");
    assertSame(materializeMap(materializer, oversizedSequence), fallback,
        "Bounded token overflow must retain the logical-map cache path");
    String oversizedValue = "large".repeat(60_000);
    assertEquals(materializeMap(materializer, "key", oversizedValue), MapUtils.toString(Map.of("key", oversizedValue)));
    materializer.resetCanonicalJsonCaches();
    assertEquals(materializeMap(materializer, "plain", "after-reset"), "{\"plain\":\"after-reset\"}");
    keys.setDictionary(stringDictionary("a"));
    values.setDictionary(stringDictionary("after-rewind"));
    assertEquals(materializeMap(materializer, 0, 0), "{\"a\":\"after-rewind\"}");
  }

  private static Dictionary stringDictionary(String... values) {
    return new Dictionary(Encoding.PLAIN) {
      @Override
      public int getMaxId() {
        return values.length - 1;
      }

      @Override
      public Binary decodeToBinary(int id) {
        return Binary.fromString(values[id]);
      }
    };
  }

  private static String materializeMap(ParquetNativeRecordMaterializer materializer, Object... tokens) {
    GroupConverter root = materializer.getRootConverter();
    GroupConverter map = root.getConverter(0).asGroupConverter();
    GroupConverter entries = map.getConverter(0).asGroupConverter();
    root.start();
    map.start();
    for (int i = 0; i < tokens.length; i += 2) {
      entries.start();
      for (int field = 0; field < 2; field++) {
        PrimitiveConverter converter = entries.getConverter(field).asPrimitiveConverter();
        Object token = tokens[i + field];
        if (token instanceof Integer id) {
          converter.addValueFromDictionary(id);
        } else {
          converter.addBinary(Binary.fromString((String) token));
        }
      }
      entries.end();
    }
    map.end();
    root.end();
    return (String) materializer.getCurrentRecord().getValue("properties");
  }

  @Test
  public void testColumnarReaderOptInAndFallback()
      throws IOException {
    assertFalse(new ParquetRecordReaderConfig().useColumnMajorSegmentBuilder());
    File file = writeColumnarReaderFixture(false);
    var schema = columnarSchema().build();
    for (boolean direct : new boolean[]{false, true}) {
      ParquetRecordReaderConfig config = columnarConfig();
      config.setUseParquetNativeDirectReader(direct);
      config.setUseColumnMajorSegmentBuilder(!direct);
      try (ParquetRecordReader reader = new ParquetRecordReader()) {
        reader.init(file, null, config);
        assertNull(reader.tryCreateColumnReaderFactory(schema), "Both native direct and explicit opt-in are required");
        assertEquals(reader.next().getValue("eventId"), 9000000000L);
      }
    }
    ParquetRecordReaderConfig avroConfig = columnarConfig();
    avroConfig.setUseParquetAvroRecordReader(true);
    try (ParquetRecordReader reader = new ParquetRecordReader()) {
      reader.init(file, null, avroConfig);
      assertTrue(reader.useAvroParquetRecordReader());
      assertNull(reader.tryCreateColumnReaderFactory(schema), "Explicit Avro wins over all native/columnar options");
    }
    ParquetRecordReaderConfig config = columnarConfig();
    File unrelatedAnnotation = writeColumnarReaderFixture(false, "LIST");
    try (ParquetNativeRecordReader reader = new ParquetNativeRecordReader()) {
      reader.init(unrelatedAnnotation, null, config);
      assertTrue(reader.isDirectRecordReader());
      assertNull(reader.tryCreateColumnReaderFactory(schema), "Unrelated entry annotations retain the row reader");
      assertEquals(reader.next().getValue("eventId"), 9000000000L, "Rejected annotations leave row position unchanged");
    }
    try (ParquetNativeRecordReader reader = new ParquetNativeRecordReader()) {
      reader.init(file, null, config);
      assertNull(reader.tryCreateColumnReaderFactory(
          columnarSchema().addSingleValueDimension("missing", DataType.STRING).build()));
      assertNull(reader.tryCreateColumnReaderFactory(
          new SchemaBuilder().addSingleValueDimension("unused", DataType.STRING).build()),
          "A physically OPTIONAL field cannot opt into this path");
      assertNull(reader.tryCreateColumnReaderFactory(
          new SchemaBuilder().addMultiValueDimension("description", DataType.STRING).build()),
          "Multi-value targets retain the row reader");
      assertNull(reader.tryCreateColumnReaderFactory(
          new SchemaBuilder().addSingleValueDimension("properties", DataType.STRING).build()),
          "MAP materialization is offered only to a JSON target");
      assertEquals(reader.next().getValue("eventId"), 9000000000L,
          "Unsupported selection leaves row position unchanged");
    }
    try (ParquetNativeRecordReader reader = new ParquetNativeRecordReader()) {
      Set<String> restricted = new HashSet<>(Set.of("eventId"));
      reader.init(file, restricted, config);
      restricted.addAll(schema.getPhysicalColumnNames());
      assertNull(reader.tryCreateColumnReaderFactory(schema), "Initial source-field restrictions cannot be expanded");
      assertEquals(reader.next().getFieldToValueMap().keySet(), Set.of("eventId"));
    }
    try (ParquetNativeRecordReader reader = new ParquetNativeRecordReader()) {
      reader.init(file, null, config);
      try (ColumnReaderFactory factory = reader.tryCreateColumnReaderFactory(schema)) {
        assertNotNull(factory);
        factory.init(schema);
        // Both construction and init leave data pages unopened; a later source I/O failure must propagate.
        var moved = file.toPath().resolveSibling(file.getName() + ".moved");
        Files.move(file.toPath(), moved);
        try {
          assertThrows(IOException.class, () -> factory.getColumnReader("eventId").getValue(0));
        } finally {
          Files.move(moved, file.toPath());
        }
      }
    }
  }

  @Test
  public void testColumnarReaderRejectsNullableOrNonStringMapValues()
      throws IOException {
    var schema = new SchemaBuilder().addSingleValueDimension("renamedMap", DataType.JSON).build();
    for (boolean nullable : new boolean[]{true, false}) {
      String valueField = nullable ? "optional binary value (STRING);" : "required int32 value;";
      File file = writeParquet("columnar-unsupported-map-" + nullable + ".parquet", """
          message UnsupportedMap {
            required group renamedMap (MAP) {
              repeated group entries {
                required binary key (STRING);
                %s
              }
            }
          }
          """.formatted(valueField), row -> {
          Group entry = row.addGroup("renamedMap").addGroup("entries").append("key", "present");
          if (!nullable) {
            entry.append("value", 17);
          }
        });
      try (ParquetNativeRecordReader reader = new ParquetNativeRecordReader()) {
        reader.init(file, null, columnarConfig());
        assertTrue(reader.isDirectRecordReader());
        assertNull(reader.tryCreateColumnReaderFactory(schema));
        Map<?, ?> value = (Map<?, ?>) reader.next().getValue("renamedMap");
        assertTrue(value.containsKey("present"));
        assertEquals(value.get("present"), nullable ? null : 17);
      }
    }
  }

  private static SchemaBuilder columnarSchema() {
    return new SchemaBuilder().setSchemaName("renamedColumns").addSingleValueDimension("eventId", DataType.LONG)
        .addSingleValueDimension("code", DataType.INT).addSingleValueDimension("description", DataType.STRING)
        .addSingleValueDimension("properties", DataType.JSON)
        .addDateTime("moment", DataType.TIMESTAMP, "TIMESTAMP", "1:MILLISECONDS");
  }

  private static ParquetRecordReaderConfig columnarConfig() {
    ParquetRecordReaderConfig config = new ParquetRecordReaderConfig();
    config.setUseParquetNativeRecordReader(true);
    config.setUseParquetNativeDirectReader(true);
    config.setUseColumnMajorSegmentBuilder(true);
    return config;
  }

  private File writeColumnarReaderFixture(boolean dictionary)
      throws IOException {
    return writeColumnarReaderFixture(dictionary, "");
  }

  private File writeColumnarReaderFixture(boolean dictionary, String entryAnnotation)
      throws IOException {
    MessageType schema = MessageTypeParser.parseMessageType("""
        message GenericRequiredColumns {
          required int64 eventId;
          required int32 code;
          required int64 moment (TIMESTAMP(NANOS,true));
          required binary description (STRING);
          required group properties (MAP) {
            repeated group entries %s {
              required binary key (STRING);
              required binary value (STRING);
            }
          }
          optional binary unused (STRING);
        }
        """.formatted(entryAnnotation.isEmpty() ? "" : "(" + entryAnnotation + ")"));
    FileUtils.forceMkdir(_tempDir);
    File file = new File(_tempDir, "columnar-required-" + dictionary + "-" + entryAnnotation + ".parquet");
    FileUtils.deleteQuietly(file);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new Path(file.getAbsolutePath()))
        .withType(schema).withDictionaryEncoding(dictionary).withDictionaryPageSize(1024).withPageSize(1024)
        .withMinRowCountForPageSizeCheck(10).withMaxRowCountForPageSizeCheck(10).withRowGroupRowCountLimit(64)
        .build()) {
      SimpleGroupFactory factory = new SimpleGroupFactory(schema);
      for (int i = 0; i < 1000; i++) {
        Group row = factory.newGroup().append("eventId", 9000000000L + i).append("code", i % 256)
            .append("moment", -1234567890L + i).append("description", "event-" + i);
        Group map = row.addGroup("properties");
        if (i % 3 != 0) {
          map.addGroup("entries").append("key", "z").append("value", "before");
          map.addGroup("entries").append("key", "a").append("value", "value");
          map.addGroup("entries").append("key", "z").append("value", "last-" + i + "雪");
        }
        writer.write(row);
      }
    }
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(
        new Path(file.getAbsolutePath()), ParquetUtils.getParquetHadoopConfiguration()))) {
      LogicalTypeAnnotation expected = switch (entryAnnotation) {
        case "" -> null;
        case "MAP_KEY_VALUE" -> MapKeyValueTypeAnnotation.getInstance();
        case "MAP" -> LogicalTypeAnnotation.mapType();
        case "LIST" -> LogicalTypeAnnotation.listType();
        default -> throw new IllegalArgumentException("Unexpected fixture annotation: " + entryAnnotation);
      };
      assertEquals(reader.getFooter().getFileMetaData().getSchema().getType("properties").asGroupType().getType(0)
          .getLogicalTypeAnnotation(), expected, "Written footer must retain the entry annotation under test");
      assertTrue(reader.getFooter().getBlocks().size() > 1, "Equivalence must cross row-group boundaries");
    }
    return file;
  }

  private static Object actualTimestamp(int row, boolean rawTimeValues) {
    long nanos = -1234567890L + row;
    return rawTimeValues ? nanos : ParquetUtils.convertLongToTimestamp(nanos, TimeUnit.NANOS);
  }

  @Test
  public void testDirectAndGroupNativeReadersAgreeOnAllTestResources()
      throws Exception {
    ParquetRecordReaderConfig directConfig = new ParquetRecordReaderConfig();
    directConfig.setUseParquetNativeDirectReader(true);
    int directFiles = 0;
    int fallbackFiles = 0;
    for (File file : listAllTestParquetFiles()) {
      if (KNOWN_FAILURES_BOTH.containsKey(file.getName())) {
        continue;
      }
      try (ParquetNativeRecordReader groupReader = new ParquetNativeRecordReader();
          ParquetNativeRecordReader directReader = new ParquetNativeRecordReader()) {
        List<GenericRow> expected = readAll(groupReader, file);
        assertFalse(groupReader.rewind(Set.of("missing")), "Default native reader must keep its existing path");
        List<GenericRow> actual = readAll(directReader, file, directConfig);
        assertReadersAgree(file.getName(), expected, actual);
        if (directReader.isDirectRecordReader()) {
          directFiles++;
        } else {
          fallbackFiles++;
          assertFalse(directReader.rewind(Set.of("missing")), "Unsupported schemas cannot use physical projection");
        }
      }
    }
    assertTrue(directFiles > 5, "Must exercise real direct materialization");
    assertTrue(fallbackFiles > 5, "Must preserve legacy/unsupported logical-type fallback");
  }

  private File writeDirectReaderFixture(boolean dictionary)
      throws IOException {
    MessageType schema = MessageTypeParser.parseMessageType("""
        message DirectReader {
          required int64 timestampNanos (TIMESTAMP(NANOS,true));
          required int64 timestampMicros (TIMESTAMP(MICROS,true));
          required int64 timestampMillis (TIMESTAMP(MILLIS,true));
          required int32 flags (INTEGER(8,false));
          required boolean active;
          required float floatValue;
          required double doubleValue;
          required binary bytes;
          optional binary label (STRING);
          required binary body (STRING);
          optional group attributes (MAP) {
            repeated group key_value {
              required binary key (STRING);
              optional binary value (STRING);
            }
          }
          optional group tags (LIST) {
            repeated group list {
              optional binary element (STRING);
            }
          }
        }
        """);
    FileUtils.forceMkdir(_tempDir);
    File file = new File(_tempDir, "direct-reader-" + dictionary + ".parquet");
    FileUtils.deleteQuietly(file);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new Path(file.getAbsolutePath()))
        .withType(schema).withDictionaryEncoding(dictionary).withDictionaryPageSize(1024).withPageSize(1024)
        .withMinRowCountForPageSizeCheck(10).withMaxRowCountForPageSizeCheck(10).withRowGroupRowCountLimit(64)
        .build()) {
      SimpleGroupFactory factory = new SimpleGroupFactory(schema);
      for (int i = 0; i < 1000; i++) {
        Group row = factory.newGroup().append("timestampNanos", -1234567890L + i)
            .append("timestampMicros", -1234567L + i).append("timestampMillis", -1234L + i)
            .append("flags", i % 256).append("active", i % 2 == 0).append("floatValue", i / 10F)
            .append("doubleValue", i / 10D).append("bytes", Binary.fromConstantByteArray(new byte[]{(byte) i, 0}))
            .append("body", i + "x".repeat(200));
        if (i % 3 != 0) {
          row.append("label", "label-" + i % 2);
          Group attributes = row.addGroup("attributes");
          Group tags = row.addGroup("tags");
          if (i % 3 == 2) {
            attributes.addGroup("key_value").append("key", "z").append("value", "first");
            attributes.addGroup("key_value").append("key", "null");
            attributes.addGroup("key_value").append("key", "a").append("value", "value");
            attributes.addGroup("key_value").append("key", "z").append("value", "last\\\"\n雪");
            tags.addGroup("list").append("element", "tag");
            tags.addGroup("list");
          }
        }
        writer.write(row);
      }
    }
    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(
        new Path(file.getAbsolutePath()), ParquetUtils.getParquetHadoopConfiguration()))) {
      ParquetMetadata footer = reader.getFooter();
      assertTrue(footer.getBlocks().size() > 1, "Fixture must cross row-group boundaries");
      assertEquals(footer.getBlocks().stream().flatMap(block -> block.getColumns().stream())
          .anyMatch(column -> column.getEncodingStats().hasDictionaryEncodedPages()), dictionary);
      assertTrue(footer.getBlocks().stream().flatMap(block -> block.getColumns().stream())
          .anyMatch(column -> column.getEncodingStats().hasNonDictionaryEncodedPages()));
    }
    return file;
  }

  /// Regression: nested `LIST<LIST<STRING>>` written with the standard 3-level encoding must have BOTH the
  /// outer and inner wrappers stripped by the Avro reader. Also covers null inner elements and null inner
  /// wrappers.
  @Test
  public void testNestedListWrappersAreUnwrapped()
      throws IOException {
    String nestedSchema = """
        message NestedListExample {
          optional group nestedTags (LIST) {
            repeated group list {
              optional group element (LIST) {
                repeated group list {
                  optional binary element (STRING);
                }
              }
            }
          }
        }
        """;
    File dataFile = writeParquet("nested-list-of-lists.parquet", nestedSchema, group -> {
      Group nestedTags = group.addGroup("nestedTags");
      Group outerList0 = nestedTags.addGroup("list").addGroup("element");
      outerList0.addGroup("list").append("element", "a");
      outerList0.addGroup("list").append("element", "b");
      Group outerList1 = nestedTags.addGroup("list").addGroup("element");
      outerList1.addGroup("list").append("element", "c");
      // Inner list with a null element (writer skips the optional binary inside the inner wrapper).
      Group outerList2 = nestedTags.addGroup("list").addGroup("element");
      outerList2.addGroup("list");
      outerList2.addGroup("list").append("element", "d");
      // Outer wrapper with no `element` group at all — reads back as a null inner list.
      nestedTags.addGroup("list");
    });
    try (ParquetAvroRecordReader recordReader = new ParquetAvroRecordReader()) {
      recordReader.init(dataFile, null, null);
      assertTrue(recordReader.hasNext());
      GenericRow row = recordReader.next();
      Object[] outer = (Object[]) row.getValue("nestedTags");
      assertEquals(outer.length, 4);
      assertEquals(Arrays.asList((Object[]) outer[0]), List.of("a", "b"));
      assertEquals(Arrays.asList((Object[]) outer[1]), List.of("c"));
      assertEquals(Arrays.asList((Object[]) outer[2]), Arrays.asList(null, "d"));
      assertNull(outer[3]);
      assertFalse(recordReader.hasNext());
    }
  }

  /// Regression: an Avro schema authored as `array<record<UserTag, fields=[element: string]>>` — where the
  /// inner record is real user data, NOT a parquet-avro-synthesized LIST wrapper — must NOT be flattened.
  /// The shape matches the wrapper convention but the file's Avro schema in metadata identifies the records
  /// as user data.
  @Test
  public void testUserRecordWithSingleElementFieldIsPreserved()
      throws IOException {
    Schema avroSchema = new Schema.Parser().parse("""
        {
          "type": "record",
          "name": "UserOuter",
          "namespace": "com.example",
          "fields": [{
            "name": "tags",
            "type": {
              "type": "array",
              "items": {
                "type": "record",
                "name": "UserTag",
                "namespace": "com.example",
                "fields": [{"name": "element", "type": "string"}]
              }
            }
          }]
        }
        """);
    FileUtils.forceMkdir(_tempDir);
    File dataFile = new File(_tempDir, "user-record-with-element-field.parquet");
    FileUtils.deleteQuietly(dataFile);
    GenericRecord row = new GenericData.Record(avroSchema);
    Schema tagSchema = avroSchema.getField("tags").schema().getElementType();
    GenericRecord tag1 = new GenericData.Record(tagSchema);
    tag1.put("element", "user-1");
    GenericRecord tag2 = new GenericData.Record(tagSchema);
    tag2.put("element", "user-2");
    row.put("tags", List.of(tag1, tag2));
    try (ParquetWriter<GenericRecord> writer = ParquetTestUtils.getParquetAvroWriter(
        new Path(dataFile.getAbsolutePath()), avroSchema)) {
      writer.write(row);
    }
    try (ParquetAvroRecordReader recordReader = new ParquetAvroRecordReader()) {
      recordReader.init(dataFile, null, null);
      assertTrue(recordReader.hasNext());
      GenericRow out = recordReader.next();
      Object[] tags = (Object[]) out.getValue("tags");
      assertEquals(tags.length, 2);
      assertEquals(tags[0], Map.of("element", "user-1"));
      assertEquals(tags[1], Map.of("element", "user-2"));
      assertFalse(recordReader.hasNext());
    }
  }

  /// Test resources that fail both [ParquetAvroRecordReader] and [ParquetNativeRecordReader], mapped to the documented
  /// reason. Both readers share parquet-mr's core (`ParquetFileReader.readFooter` / chunk-page reading), so a malformed
  /// file at that layer fails both equally.
  private static final Map<String, String> KNOWN_FAILURES_BOTH = Map.ofEntries(
      // Malformed page boundary — parquet-mr's chunk reader runs short on bytes.
      Map.entry("nation.dict.parquet", """
          Parquet-mr's WorkaroundChunk.readAsBytesInput throws EOFException with 7 bytes short of the expected \
          chunk length — malformed page boundary in this fixture"""),
      Map.entry("nation.dict-malformed.parquet", """
          Same EOF/malformed-page-boundary failure as nation.dict.parquet — parquet-mr's \
          WorkaroundChunk.readAsBytesInput throws EOFException with 7 bytes short"""),
      // Footer / column-list edge cases.
      Map.entry("no_columns.parquet", """
          File has no columns; parquet-mr's ParquetMetadataConverter.fromParquetMetadata indexes the empty \
          column list and throws IndexOutOfBoundsException while reading the footer"""),
      // Codec not available / mismatched.
      Map.entry("non_hadoop_lz4_compressed.parquet", """
          File uses the non-Hadoop framed LZ4 codec (LZ4_RAW); parquet-mr fails to decompress with \
          ParquetDecodingException 'Can not read value at 0 in block -1'. Affects both readers since \
          decompression happens in parquet-mr's column reader path""")
  );

  /// Test resources that fail [ParquetAvroRecordReader] but are read fine by [ParquetNativeRecordReader], mapped to the
  /// documented reason. These are Avro-specific issues (schema-name validation, type incompatibility with the Avro
  /// spec, etc.) that the schema-walking native reader sidesteps.
  private static final Map<String, String> KNOWN_FAILURES_AVRO_ONLY = Map.ofEntries(
      // AvroSchemaConverter rejects names that violate Avro's identifier rules.
      Map.entry("decimals.parquet", """
          Parquet-dotnet writes the message-type name 'parquet-dotnet-schema'; Avro forbids '-' in record names so \
          AvroSchemaConverter rejects it before any data is read"""),
      Map.entry("delta_encoding_required_column.parquet", """
          Field name 'c_customer_sk:' contains a ':' character that Avro forbids in field names; \
          AvroSchemaConverter rejects it before any data is read"""),
      Map.entry("hadoop_lz4_compressed.parquet", """
          The Parquet message-type name is empty; Avro requires non-empty record names so AvroSchemaConverter \
          rejects it before any data is read"""),
      // AvroSchemaConverter rejects Parquet shapes that have no Avro spec equivalent.
      Map.entry("nested_maps.snappy.parquet", """
          MAP field has a non-binary key (int32). Avro requires map keys to be strings, so AvroSchemaConverter \
          throws 'Map key type must be binary (UTF8)' before any data is read. The native reader handles \
          non-string map keys via stringifyMapKey."""),
      Map.entry("repeated_no_annotation.parquet", """
          Schema uses the legacy 'repeated <primitive>' encoding (a top-level repeated primitive without \
          LIST/group wrapping); parquet-avro's AvroCollectionConverter calls asGroupType() on the element and \
          throws ClassCastException for the primitive"""),
      // parquet-java 1.17 schema/value mismatch for int32/int64 DECIMAL columns.
      Map.entry("int32_decimal.parquet", """
          Parquet-java 1.17 (apache/parquet-java#3306, GH-3149) added FieldDecimalIntConverter, which \
          materializes BigDecimal for int32/int64 columns carrying a parquet-side DECIMAL annotation. But \
          AvroSchemaConverter strips that annotation from the derived avro schema (the avro spec doesn't allow \
          `decimal` on int/long), so the avro schema reads `[null, int]` while the runtime value is BigDecimal. \
          AvroRecordExtractor's GenericData.resolveUnion then throws 'Unknown datum type java.math.BigDecimal'. \
          The native reader handles the file without going through avro."""),
      Map.entry("int64_decimal.parquet", """
          Same parquet-java 1.17 issue as int32_decimal.parquet, but via FieldDecimalLongConverter — \
          parquet-side DECIMAL annotation on a long column produces BigDecimal while the avro schema reads \
          `[null, long]`""")
  );

  /// Cross-checks every existing Parquet test resource through both readers and asserts the two produce the same Pinot
  /// rows. Locks in that any encoding subtlety in the fixtures (LIST / MAP variants, INT96, decimals, dictionary
  /// encoding, legacy Impala / Hive output, etc.) is interpreted the same way by both code paths. Files that either
  /// reader cannot handle must be enumerated in [#KNOWN_FAILURES_BOTH] / [#KNOWN_FAILURES_AVRO_ONLY] with a reason —
  /// any unexplained failure (or stale allow-list entry) fails the test.
  @Test
  public void testAvroAndNativeReadersAgreeOnAllTestResources()
      throws Exception {
    List<File> parquetFiles = listAllTestParquetFiles();
    assertTrue(parquetFiles.size() > 50, "expected to find a sizable corpus of test Parquet files, got "
        + parquetFiles.size());

    List<String> allowListIssues = new ArrayList<>();
    List<String> unexpectedFailures = new ArrayList<>();
    List<String> disagreements = new ArrayList<>();
    Set<String> bothFailuresHit = new HashSet<>();
    Set<String> avroOnlyFailuresHit = new HashSet<>();
    int compared = 0;
    for (File file : parquetFiles) {
      String fileName = file.getName();
      boolean inBoth = KNOWN_FAILURES_BOTH.containsKey(fileName);
      boolean inAvroOnly = KNOWN_FAILURES_AVRO_ONLY.containsKey(fileName);
      List<GenericRow> rowsAvro;
      try (ParquetAvroRecordReader avroReader = new ParquetAvroRecordReader()) {
        rowsAvro = readAll(avroReader, file);
      } catch (Throwable avroError) {
        if (inBoth) {
          // Verify the BOTH classification: native must also fail.
          if (nativeReadFails(file)) {
            bothFailuresHit.add(fileName);
          } else {
            allowListIssues.add(fileName
                + ": listed in KNOWN_FAILURES_BOTH but native reader succeeded — move to KNOWN_FAILURES_AVRO_ONLY");
          }
        } else if (inAvroOnly) {
          // Verify the AVRO_ONLY classification: native must succeed.
          if (nativeReadFails(file)) {
            allowListIssues.add(fileName
                + ": listed in KNOWN_FAILURES_AVRO_ONLY but native reader also failed — move to KNOWN_FAILURES_BOTH");
          } else {
            avroOnlyFailuresHit.add(fileName);
          }
        } else {
          unexpectedFailures.add(fileName + ": Avro reader failed (" + describe(avroError)
              + ") — add to KNOWN_FAILURES_BOTH or KNOWN_FAILURES_AVRO_ONLY with a reason if this is expected");
        }
        continue;
      }
      // Avro reader succeeded.
      if (inBoth || inAvroOnly) {
        allowListIssues.add(fileName + ": listed in "
            + (inBoth ? "KNOWN_FAILURES_BOTH" : "KNOWN_FAILURES_AVRO_ONLY")
            + " but the Avro reader succeeded — remove the entry");
        continue;
      }
      List<GenericRow> rowsNative;
      try (ParquetNativeRecordReader nativeReader = new ParquetNativeRecordReader()) {
        rowsNative = readAll(nativeReader, file);
      } catch (Throwable nativeError) {
        unexpectedFailures.add(fileName + ": Avro reader succeeded but native reader failed ("
            + describe(nativeError) + ")");
        continue;
      }
      try {
        assertReadersAgree(fileName, rowsAvro, rowsNative);
        compared++;
      } catch (AssertionError diff) {
        Map<String, Object> firstAvro = rowsAvro.isEmpty() ? Map.of() : wrapArray(rowsAvro.get(0).getFieldToValueMap());
        Map<String, Object> firstNative =
            rowsNative.isEmpty() ? Map.of() : wrapArray(rowsNative.get(0).getFieldToValueMap());
        disagreements.add(fileName + ": " + diff.getMessage()
            + "\n        avro  =" + firstAvro
            + "\n        native=" + firstNative);
      }
    }
    // Detect stale allow-list entries (file removed from corpus, or the reader now handles it).
    KNOWN_FAILURES_BOTH.keySet().stream()
        .filter(k -> !bothFailuresHit.contains(k))
        .forEach(k -> allowListIssues.add(
            k + ": listed in KNOWN_FAILURES_BOTH but file is not in the corpus or no longer fails — remove the entry"));
    KNOWN_FAILURES_AVRO_ONLY.keySet().stream()
        .filter(k -> !avroOnlyFailuresHit.contains(k))
        .forEach(k -> allowListIssues.add(k
            + ": listed in KNOWN_FAILURES_AVRO_ONLY but file is not in the corpus or no longer fails — remove the "
            + "entry"));

    if (!allowListIssues.isEmpty() || !unexpectedFailures.isEmpty() || !disagreements.isEmpty()) {
      fail(buildFailureReport(compared, allowListIssues, unexpectedFailures, disagreements));
    }
  }

  private static boolean nativeReadFails(File file) {
    try (ParquetNativeRecordReader nativeReader = new ParquetNativeRecordReader()) {
      readAll(nativeReader, file);
      return false;
    } catch (Throwable t) {
      return true;
    }
  }

  private static String describe(Throwable t) {
    String message = t.getMessage();
    return t.getClass().getSimpleName() + (message == null ? "" : ": " + message);
  }

  private static String buildFailureReport(int compared, List<String> allowListIssues,
      List<String> unexpectedFailures, List<String> disagreements) {
    StringBuilder msg = new StringBuilder();
    msg.append(String.format(
        "Compared %d file(s); %d allow-list issue(s), %d unexpected reader failure(s), %d disagreement(s).%n",
        compared, allowListIssues.size(), unexpectedFailures.size(), disagreements.size()));
    appendSection(msg, "ALLOW-LIST ISSUES", allowListIssues);
    appendSection(msg, "UNEXPECTED READER FAILURES", unexpectedFailures);
    appendSection(msg, "READER DISAGREEMENTS", disagreements);
    return msg.toString();
  }

  private static void appendSection(StringBuilder msg, String title, List<String> items) {
    if (items.isEmpty()) {
      return;
    }
    msg.append(String.format("%n[%s]%n", title));
    items.forEach(item -> msg.append("  - ").append(item).append(System.lineSeparator()));
  }

  // === Writers and per-test row assertions ===

  private File writeHandAuthoredParquetFile()
      throws IOException {
    return writeParquet("hand-authored-collections.parquet", SCHEMA, group -> {
      Group topLevelTags = group.addGroup("topLevelTags");
      topLevelTags.addGroup("list").append("element", "top-a");
      topLevelTags.addGroup("list").append("element", "top-b");
      Group topLevelProperties = group.addGroup("topLevelProperties");
      topLevelProperties.addGroup("key_value").append("key", "top-key-a").append("value", "top-value-a");
      topLevelProperties.addGroup("key_value").append("key", "top-key-b").append("value", "top-value-b");
      group.addGroup("emptyProperties");
      Group metadata = group.addGroup("metadata");
      metadata.append("element", "real-element-field");
      metadata.addGroup("tags").addGroup("list").append("element", "abc");
      metadata.getGroup("tags", 0).addGroup("list").append("element", "xyz");
      metadata.addGroup("nullableTags").addGroup("list");
      metadata.getGroup("nullableTags", 0).addGroup("list").append("element", "nonnull");
      metadata.addGroup("legacySingleFieldStructs").addGroup("list").append("item", "legacy-a");
      metadata.getGroup("legacySingleFieldStructs", 0).addGroup("list").append("item", "legacy-b");
      metadata.addGroup("legacyMultiFieldStructs").addGroup("list").append("name", "legacy-alpha").append("score", 1);
      metadata.getGroup("legacyMultiFieldStructs", 0).addGroup("list").append("name", "legacy-beta").append("score",
          2);
      metadata.addGroup("tagStructs").addGroup("list").addGroup("element").append("element", "inner-a");
      metadata.getGroup("tagStructs", 0).addGroup("list").addGroup("element").append("element", "inner-b");
      Group properties = metadata.addGroup("properties");
      properties.addGroup("key_value").append("key", "key-a").append("value", "value-a");
      properties.addGroup("key_value").append("key", "key-b").append("value", "value-b");
    });
  }

  /// Writes a single-row Parquet file at `_tempDir/fileName` with the given `schemaText`. The `populator`
  /// runs against a fresh [Group] which is then written to the file.
  private File writeParquet(String fileName, String schemaText, Consumer<Group> populator)
      throws IOException {
    FileUtils.forceMkdir(_tempDir);
    File dataFile = new File(_tempDir, fileName);
    FileUtils.deleteQuietly(dataFile);
    MessageType schema = MessageTypeParser.parseMessageType(schemaText);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new Path(dataFile.getAbsolutePath()))
        .withType(schema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build()) {
      Group group = new SimpleGroupFactory(schema).newGroup();
      populator.accept(group);
      writer.write(group);
    }
    return dataFile;
  }

  private void assertHandAuthoredCollectionValues(RecordReader recordReader)
      throws IOException {
    assertTrue(recordReader.hasNext());
    GenericRow row = recordReader.next();
    assertEquals(Arrays.asList((Object[]) row.getValue("topLevelTags")), List.of("top-a", "top-b"));
    assertEquals(row.getValue("topLevelProperties"), Map.of(
        "top-key-a", "top-value-a",
        "top-key-b", "top-value-b"
    ));
    assertEquals(row.getValue("emptyProperties"), Map.of());

    //noinspection unchecked
    Map<String, Object> metadata = (Map<String, Object>) row.getValue("metadata");
    assertEquals(metadata.get("element"), "real-element-field");
    assertEquals(Arrays.asList((Object[]) metadata.get("tags")), List.of("abc", "xyz"));
    assertEquals(Arrays.asList((Object[]) metadata.get("nullableTags")), Arrays.asList(null, "nonnull"));
    assertEquals(metadata.get("properties"), Map.of("key-a", "value-a", "key-b", "value-b"));

    // Per the Parquet LogicalTypes spec backward-compat rules, a single-field repeated wrapper whose name is
    // NOT `array` or `<list>_tuple` is a wrapper — its inner field is the element regardless of the inner
    // field's name. Apache Arrow / parquet-cpp / parquet-avro (with add-list-element-records=false) all
    // flatten this case to the primitive value list. Preserved struct shapes require the multi-field group
    // form (see legacyMultiFieldStructs).
    Object[] legacySingleFieldStructs = (Object[]) metadata.get("legacySingleFieldStructs");
    assertEquals(legacySingleFieldStructs, new Object[]{"legacy-a", "legacy-b"});

    Object[] legacyMultiFieldStructs = (Object[]) metadata.get("legacyMultiFieldStructs");
    assertEquals(legacyMultiFieldStructs, new Object[]{
        Map.of("name", "legacy-alpha", "score", 1),
        Map.of("name", "legacy-beta", "score", 2)
    });

    Object[] tagStructs = (Object[]) metadata.get("tagStructs");
    assertEquals(tagStructs, new Object[]{
        Map.of("element", "inner-a"),
        Map.of("element", "inner-b")
    });
    assertFalse(recordReader.hasNext());
  }

  private void assertFixtureValues(RecordReader recordReader)
      throws IOException {
    assertTrue(recordReader.hasNext());
    GenericRow row = recordReader.next();
    assertEquals(row.getFieldToValueMap().size(), 17);
    assertEquals(row.getValue("intField"), 123);
    assertEquals(row.getValue("longField"), 1234567890123L);
    assertEquals(row.getValue("floatField"), 1.5F);
    assertEquals(row.getValue("doubleField"), 2.25D);
    assertEquals(row.getValue("boolField"), true);
    assertEquals(row.getValue("stringField"), "hello parquet");
    assertEquals(row.getValue("decimalField"), new BigDecimal("123.45"));
    assertEquals(row.getValue("dateField"), LocalDate.of(2024, 1, 1));
    assertEquals(row.getValue("timestampMillisField"), new Timestamp(1700000000123L));

    //noinspection unchecked
    Map<String, Object> struct = (Map<String, Object>) row.getValue("structField");
    assertEquals(struct.get("nestedString"), "nested-value");
    assertEquals(struct.get("nestedInt"), 99);
    assertEquals(struct.get("nestedElementStruct"), Map.of("element", "preserved-element"));

    assertEquals(Arrays.asList((Object[]) row.getValue("stringList")), List.of("red", "blue"));
    assertEquals(Arrays.asList((Object[]) row.getValue("intList")), List.of(7, 11));
    assertEquals(((Object[]) row.getValue("emptyStringList")).length, 0);

    assertEquals(row.getValue("stringMap"), Map.of("k1", "v1", "k2", "v2"));
    assertEquals(row.getValue("emptyStringMap"), Map.of());

    Object[] structList = (Object[]) row.getValue("structList");
    assertEquals(structList, new Object[]{
        Map.of("name", "alpha", "score", 10),
        Map.of("name", "beta", "score", 20)
    });

    //noinspection unchecked
    Map<String, Object> mapOfLists = (Map<String, Object>) row.getValue("mapOfLists");
    assertEquals(Arrays.asList((Object[]) mapOfLists.get("letters")), List.of("a", "b"));
    assertEquals(Arrays.asList((Object[]) mapOfLists.get("single")), List.of("z"));
    assertFalse(recordReader.hasNext());
  }

  // === Cross-reader equivalence: helpers ===

  private static void assertReadersAgree(String fileName, List<GenericRow> rowsAvro, List<GenericRow> rowsNative) {
    assertEquals(rowsAvro.size(), rowsNative.size(), fileName + ": row count");
    for (int i = 0; i < rowsAvro.size(); i++) {
      Map<String, Object> avroRow = wrapArray(rowsAvro.get(i).getFieldToValueMap());
      Map<String, Object> nativeRow = wrapArray(rowsNative.get(i).getFieldToValueMap());
      assertEquals(avroRow, nativeRow, fileName + " row " + i);
    }
  }

  private static List<File> listAllTestParquetFiles()
      throws Exception {
    URL marker = ParquetCollectionAndEquivalenceTest.class.getClassLoader()
        .getResource("collection-reader-fixture.parquet");
    assertNotNull(marker, "collection-reader-fixture.parquet must be on the classpath");
    java.nio.file.Path root = new File(marker.toURI()).getParentFile().toPath();
    try (Stream<java.nio.file.Path> stream = Files.walk(root)) {
      return stream.filter(Files::isRegularFile)
          .filter(p -> p.getFileName().toString().endsWith(".parquet"))
          .map(java.nio.file.Path::toFile)
          .sorted()
          .collect(Collectors.toList());
    }
  }

  private static List<GenericRow> readAll(RecordReader reader, File file)
      throws IOException {
    return readAll(reader, file, null);
  }

  private static List<GenericRow> readAll(RecordReader reader, File file, ParquetRecordReaderConfig config)
      throws IOException {
    reader.init(file, null, config);
    return readRows(reader);
  }

  private static List<GenericRow> readRows(RecordReader reader)
      throws IOException {
    List<GenericRow> rows = new ArrayList<>();
    GenericRow reuse = new GenericRow();
    while (reader.hasNext()) {
      reuse.clear();
      reader.next(reuse);
      rows.add(reuse.copy());
    }
    return rows;
  }

  /// Wraps `byte[]` as [ByteArray] and converts `Object[]` to `List`, recursing through nested [Map] / [List]
  /// values, so the row map can be compared via [Map#equals] without tripping on JVM array
  /// reference-equality.
  private static Map<String, Object> wrapArray(Map<String, Object> row) {
    Map<String, Object> out = Maps.newHashMapWithExpectedSize(row.size());
    for (Map.Entry<String, Object> e : row.entrySet()) {
      out.put(e.getKey(), wrapArrayValue(e.getValue()));
    }
    return out;
  }

  private static Object wrapArrayValue(Object value) {
    if (value instanceof byte[]) {
      return new ByteArray((byte[]) value);
    }
    if (value instanceof Object[]) {
      Object[] array = (Object[]) value;
      List<Object> list = new ArrayList<>(array.length);
      for (Object v : array) {
        list.add(wrapArrayValue(v));
      }
      return list;
    }
    if (value instanceof List) {
      //noinspection unchecked
      List<Object> in = (List<Object>) value;
      List<Object> out = new ArrayList<>(in.size());
      for (Object v : in) {
        out.add(wrapArrayValue(v));
      }
      return out;
    }
    if (value instanceof Map) {
      //noinspection unchecked
      Map<Object, Object> in = (Map<Object, Object>) value;
      Map<Object, Object> out = Maps.newHashMapWithExpectedSize(in.size());
      for (Map.Entry<Object, Object> e : in.entrySet()) {
        out.put(e.getKey(), wrapArrayValue(e.getValue()));
      }
      return out;
    }
    return value;
  }
}
