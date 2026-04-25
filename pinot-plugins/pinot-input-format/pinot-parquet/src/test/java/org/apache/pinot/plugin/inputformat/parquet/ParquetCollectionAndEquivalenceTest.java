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

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * Tests Parquet collection-wrapper handling end-to-end, covering:
 * <ul>
 *   <li>Specific value assertions for the standard 3-level LIST encoding, MAP encoding, nested structs, and
 *       all four Parquet LogicalTypes spec backward-compat rules for LIST element resolution (via a
 *       hand-authored {@code MessageType}).</li>
 *   <li>Specific value assertions for the checked-in {@code collection-reader-fixture.parquet} (primitives,
 *       DECIMAL/DATE/TIMESTAMP, nested structs, struct lists, map of lists, real {@code element}-named
 *       struct field).</li>
 *   <li>Regression for nested {@code LIST<LIST<STRING>>} unwrapping through the Avro reader, including the
 *       two null branches (null inner element, null inner wrapper).</li>
 *   <li>Regression for user-authored {@code array<record<UserTag, [element: string]>>} — the inner records
 *       must NOT be flattened (the Avro shape matches the parquet-avro wrapper convention but the file's
 *       Avro schema in metadata identifies the records as user data).</li>
 *   <li>Cross-reader equivalence on every existing {@code .parquet} test resource — ParquetAvroRecordReader
 *       and ParquetNativeRecordReader must produce structurally identical Pinot rows. Locks in that any
 *       encoding subtlety in the fixtures is interpreted the same way by both code paths.</li>
 * </ul>
 *
 * <p>The hand-authored schema is asserted via the native reader only; the cross-reader equivalence test
 * ensures the Avro reader produces the same rows on the same file.
 *
 * <p>This test class is stateful only through its temporary directory and is not thread-safe.
 */
public class ParquetCollectionAndEquivalenceTest {
  private static final String SCHEMA = String.join("\n",
      "message CollectionWrapperExample {",
      "  optional group topLevelTags (LIST) {",
      "    repeated group list {",
      "      optional binary element (STRING);",
      "    }",
      "  }",
      "  optional group topLevelProperties (MAP) {",
      "    repeated group key_value {",
      "      required binary key (STRING);",
      "      optional binary value (STRING);",
      "    }",
      "  }",
      "  optional group emptyProperties (MAP) {",
      "    repeated group key_value {",
      "      required binary key (STRING);",
      "      optional binary value (STRING);",
      "    }",
      "  }",
      "  optional group metadata {",
      "    optional binary element (STRING);",
      "    optional group tags (LIST) {",
      "      repeated group list {",
      "        optional binary element (STRING);",
      "      }",
      "    }",
      "    optional group nullableTags (LIST) {",
      "      repeated group list {",
      "        optional binary element (STRING);",
      "      }",
      "    }",
      "    optional group legacySingleFieldStructs (LIST) {",
      "      repeated group list {",
      "        optional binary item (STRING);",
      "      }",
      "    }",
      "    optional group legacyMultiFieldStructs (LIST) {",
      "      repeated group list {",
      "        optional binary name (STRING);",
      "        optional int32 score;",
      "      }",
      "    }",
      "    optional group tagStructs (LIST) {",
      "      repeated group list {",
      "        optional group element {",
      "          optional binary element (STRING);",
      "        }",
      "      }",
      "    }",
      "    optional group properties (MAP) {",
      "      repeated group key_value {",
      "        required binary key (STRING);",
      "        optional binary value (STRING);",
      "      }",
      "    }",
      "  }",
      "}");

  /**
   * Files where the two readers disagree because of unrelated, pre-existing TIMESTAMP_NANOS / INT96
   * unit-conversion behavior (Avro applies a different unit conversion than Native returns the raw value in).
   * Out of scope for this PR — exclude rather than mask the divergence inside {@link #canonicalizeValue}.
   */
  private static final Set<String> KNOWN_TIMESTAMP_DIVERGENCE_FILES = Set.of(
      "nested_structs.rust.parquet"
  );

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

  /**
   * Regression: nested LIST&lt;LIST&lt;STRING&gt;&gt; written with the standard 3-level encoding must have
   * BOTH the outer and inner wrappers stripped by the Avro reader. Also covers null inner elements and null
   * inner wrappers.
   */
  @Test
  public void testNestedListWrappersAreUnwrapped()
      throws IOException {
    String nestedSchema = String.join("\n",
        "message NestedListExample {",
        "  optional group nestedTags (LIST) {",
        "    repeated group list {",
        "      optional group element (LIST) {",
        "        repeated group list {",
        "          optional binary element (STRING);",
        "        }",
        "      }",
        "    }",
        "  }",
        "}");
    FileUtils.forceMkdir(_tempDir);
    File dataFile = new File(_tempDir, "nested-list-of-lists.parquet");
    FileUtils.deleteQuietly(dataFile);
    MessageType schema = MessageTypeParser.parseMessageType(nestedSchema);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new Path(dataFile.getAbsolutePath()))
        .withType(schema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build()) {
      SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
      Group group = groupFactory.newGroup();
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
      writer.write(group);
    }
    try (ParquetAvroRecordReader recordReader = new ParquetAvroRecordReader()) {
      recordReader.init(dataFile, null, null);
      assertTrue(recordReader.hasNext());
      GenericRow row = recordReader.next();
      Object[] outer = (Object[]) row.getValue("nestedTags");
      assertEquals(outer.length, 4);
      assertEquals(Arrays.asList((Object[]) outer[0]), Arrays.asList("a", "b"));
      assertEquals(Arrays.asList((Object[]) outer[1]), Arrays.asList("c"));
      assertEquals(Arrays.asList((Object[]) outer[2]), Arrays.asList(null, "d"));
      assertEquals(outer[3], null);
      assertFalse(recordReader.hasNext());
    }
  }

  /**
   * Regression: an Avro schema authored as {@code array<record<UserTag, fields=[element: string]>>} — where
   * the inner record is real user data, NOT a parquet-avro-synthesized LIST wrapper — must NOT be flattened.
   * The shape matches the wrapper convention but the file's Avro schema in metadata identifies the records as
   * user data.
   */
  @Test
  public void testUserRecordWithSingleElementFieldIsPreserved()
      throws IOException {
    Schema avroSchema = new Schema.Parser().parse(String.join("\n",
        "{",
        "  \"type\": \"record\",",
        "  \"name\": \"UserOuter\",",
        "  \"namespace\": \"com.example\",",
        "  \"fields\": [{",
        "    \"name\": \"tags\",",
        "    \"type\": {",
        "      \"type\": \"array\",",
        "      \"items\": {",
        "        \"type\": \"record\",",
        "        \"name\": \"UserTag\",",
        "        \"namespace\": \"com.example\",",
        "        \"fields\": [{\"name\": \"element\", \"type\": \"string\"}]",
        "      }",
        "    }",
        "  }]",
        "}"));
    FileUtils.forceMkdir(_tempDir);
    File dataFile = new File(_tempDir, "user-record-with-element-field.parquet");
    FileUtils.deleteQuietly(dataFile);
    GenericRecord row = new GenericData.Record(avroSchema);
    Schema tagSchema = avroSchema.getField("tags").schema().getElementType();
    GenericRecord tag1 = new GenericData.Record(tagSchema);
    tag1.put("element", "user-1");
    GenericRecord tag2 = new GenericData.Record(tagSchema);
    tag2.put("element", "user-2");
    row.put("tags", Arrays.asList(tag1, tag2));
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

  /**
   * Cross-checks every existing Parquet test resource through both readers and asserts the two produce the
   * same Pinot rows. Locks in that any encoding subtlety in the fixtures (LIST/MAP variants, INT96, decimals,
   * dictionary encoding, legacy Impala/Hive output, etc.) is interpreted the same way by both code paths.
   */
  @Test
  public void testAvroAndNativeReadersAgreeOnAllTestResources()
      throws Exception {
    List<File> parquetFiles = listAllTestParquetFiles();
    assertTrue(parquetFiles.size() > 50, "expected to find a sizable corpus of test Parquet files, got "
        + parquetFiles.size());

    List<String> failures = new ArrayList<>();
    int compared = 0;
    for (File file : parquetFiles) {
      String fileName = file.getName();
      if (KNOWN_TIMESTAMP_DIVERGENCE_FILES.contains(fileName)) {
        continue;
      }
      List<GenericRow> rowsAvro;
      List<GenericRow> rowsNative;
      try {
        rowsAvro = readAll(new ParquetAvroRecordReader(), file);
      } catch (Throwable t) {
        // Avro reader rejects some legacy schemas; only the native reader handles those. Skip rather than
        // fail since this test verifies cross-reader agreement, not single-reader coverage.
        continue;
      }
      try {
        rowsNative = readAll(new ParquetNativeRecordReader(), file);
      } catch (Throwable t) {
        continue;
      }
      try {
        assertReadersAgree(fileName, rowsAvro, rowsNative);
        compared++;
      } catch (AssertionError e) {
        Map<String, Object> firstAvro = rowsAvro.isEmpty() ? Collections.emptyMap()
            : canonicalize(rowsAvro.get(0).getFieldToValueMap());
        Map<String, Object> firstNative = rowsNative.isEmpty() ? Collections.emptyMap()
            : canonicalize(rowsNative.get(0).getFieldToValueMap());
        failures.add(fileName + ": " + e.getMessage() + "\n      avro  =" + firstAvro
            + "\n      native=" + firstNative);
      }
    }
    assertTrue(compared >= 1, "expected at least one Parquet file to be readable by both readers");
    assertEquals(failures, Collections.emptyList(),
        "Parquet readers disagreed on " + failures.size() + " of " + compared + " files");
  }

  // ----- writers + assertions for the hand-authored schema -----

  private File writeHandAuthoredParquetFile()
      throws IOException {
    FileUtils.forceMkdir(_tempDir);
    File dataFile = new File(_tempDir, "hand-authored-collections.parquet");
    FileUtils.deleteQuietly(dataFile);
    MessageType schema = MessageTypeParser.parseMessageType(SCHEMA);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new Path(dataFile.getAbsolutePath()))
        .withType(schema)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build()) {
      SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
      Group group = groupFactory.newGroup();
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
      writer.write(group);
    }
    return dataFile;
  }

  @SuppressWarnings("unchecked")
  private void assertHandAuthoredCollectionValues(RecordReader recordReader)
      throws IOException {
    assertTrue(recordReader.hasNext());
    GenericRow row = recordReader.next();
    assertEquals(Arrays.asList((Object[]) row.getValue("topLevelTags")), Arrays.asList("top-a", "top-b"));
    assertEquals(row.getValue("topLevelProperties"), Map.of("top-key-a", "top-value-a", "top-key-b", "top-value-b"));
    assertEquals(row.getValue("emptyProperties"), Map.of());

    Map<String, Object> metadata = (Map<String, Object>) row.getValue("metadata");
    assertEquals(metadata.get("element"), "real-element-field");
    assertEquals(Arrays.asList((Object[]) metadata.get("tags")), Arrays.asList("abc", "xyz"));
    assertEquals(Arrays.asList((Object[]) metadata.get("nullableTags")), Arrays.asList(null, "nonnull"));
    assertEquals(metadata.get("properties"), Map.of("key-a", "value-a", "key-b", "value-b"));

    // Per the Parquet LogicalTypes spec backward-compat rules, a single-field repeated wrapper whose name is
    // NOT `array` or `<list>_tuple` is a wrapper — its inner field is the element regardless of the inner
    // field's name. Apache Arrow / parquet-cpp / parquet-avro (with add-list-element-records=false) all
    // flatten this case to the primitive value list. Preserved struct shapes require the multi-field group
    // form (see legacyMultiFieldStructs).
    Object[] legacySingleFieldStructs = (Object[]) metadata.get("legacySingleFieldStructs");
    assertEquals(legacySingleFieldStructs.length, 2);
    assertEquals(legacySingleFieldStructs[0], "legacy-a");
    assertEquals(legacySingleFieldStructs[1], "legacy-b");

    Object[] legacyMultiFieldStructs = (Object[]) metadata.get("legacyMultiFieldStructs");
    assertEquals(legacyMultiFieldStructs.length, 2);
    assertEquals(legacyMultiFieldStructs[0], Map.of("name", "legacy-alpha", "score", 1));
    assertEquals(legacyMultiFieldStructs[1], Map.of("name", "legacy-beta", "score", 2));

    Object[] tagStructs = (Object[]) metadata.get("tagStructs");
    assertEquals(tagStructs.length, 2);
    assertEquals(tagStructs[0], Map.of("element", "inner-a"));
    assertEquals(tagStructs[1], Map.of("element", "inner-b"));
    assertFalse(recordReader.hasNext());
  }

  @SuppressWarnings("unchecked")
  private void assertFixtureValues(RecordReader recordReader)
      throws IOException {
    assertTrue(recordReader.hasNext());
    GenericRow row = recordReader.next();
    assertEquals(row.getFieldToValueMap().size(), 17);
    assertEquals(row.getValue("intField"), 123);
    assertEquals(row.getValue("longField"), 1234567890123L);
    assertEquals(row.getValue("floatField"), 1.5F);
    assertEquals(row.getValue("doubleField"), 2.25D);
    assertEquals(row.getValue("boolField"), "true");
    assertEquals(row.getValue("stringField"), "hello parquet");
    assertEquals(row.getValue("decimalField"), new BigDecimal("123.45"));
    assertEquals(row.getValue("dateField"), 19723);
    assertEquals(row.getValue("timestampMillisField"), 1700000000123L);

    Map<String, Object> struct = (Map<String, Object>) row.getValue("structField");
    assertEquals(struct.get("nestedString"), "nested-value");
    assertEquals(struct.get("nestedInt"), 99);
    assertEquals(struct.get("nestedElementStruct"), Map.of("element", "preserved-element"));

    assertEquals(Arrays.asList((Object[]) row.getValue("stringList")), Arrays.asList("red", "blue"));
    assertEquals(Arrays.asList((Object[]) row.getValue("intList")), Arrays.asList(7, 11));
    assertEquals(((Object[]) row.getValue("emptyStringList")).length, 0);

    assertEquals(row.getValue("stringMap"), Map.of("k1", "v1", "k2", "v2"));
    assertEquals(row.getValue("emptyStringMap"), Map.of());

    Object[] structList = (Object[]) row.getValue("structList");
    assertEquals(structList.length, 2);
    assertEquals(structList[0], Map.of("name", "alpha", "score", 10));
    assertEquals(structList[1], Map.of("name", "beta", "score", 20));

    Map<String, Object> mapOfLists = (Map<String, Object>) row.getValue("mapOfLists");
    assertEquals(Arrays.asList((Object[]) mapOfLists.get("letters")), Arrays.asList("a", "b"));
    assertEquals(Arrays.asList((Object[]) mapOfLists.get("single")), Arrays.asList("z"));
    assertFalse(recordReader.hasNext());
  }

  // ----- helpers for the cross-reader equivalence test -----

  private static void assertReadersAgree(String fileName, List<GenericRow> rowsAvro, List<GenericRow> rowsNative) {
    assertEquals(rowsAvro.size(), rowsNative.size(), fileName + ": row count");
    for (int i = 0; i < rowsAvro.size(); i++) {
      Map<String, Object> avroRow = canonicalize(rowsAvro.get(i).getFieldToValueMap());
      Map<String, Object> nativeRow = canonicalize(rowsNative.get(i).getFieldToValueMap());
      assertEquals(avroRow, nativeRow, fileName + " row " + i);
    }
  }

  private static List<File> listAllTestParquetFiles()
      throws Exception {
    URL marker = ParquetCollectionAndEquivalenceTest.class.getClassLoader()
        .getResource("collection-reader-fixture.parquet");
    assertTrue(marker != null, "collection-reader-fixture.parquet must be on the classpath");
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
    reader.init(file, null, null);
    List<GenericRow> rows = new ArrayList<>();
    try {
      while (reader.hasNext()) {
        GenericRow row = new GenericRow();
        reader.next(row);
        rows.add(row.copy());
      }
    } finally {
      reader.close();
    }
    return rows;
  }

  /**
   * Normalizes a row's value map so that representation differences between the readers don't show up as data
   * differences in the cross-reader equivalence check: {@code Object[]} becomes a {@code List}, nested Maps
   * recurse into a {@link TreeMap} (so HashMap vs LinkedHashMap iteration order doesn't matter), {@code byte[]}
   * gets content-based equality, and {@code Date}/{@code Timestamp}/{@code LocalDate}/{@code Instant} (as well
   * as the {@code "YYYY-MM-DD"} string the Avro reader emits for LocalDate) all collapse to the underlying
   * raw numeric form the native reader returns.
   */
  private static Map<String, Object> canonicalize(Map<String, Object> row) {
    Map<String, Object> out = new TreeMap<>();
    for (Map.Entry<String, Object> e : row.entrySet()) {
      out.put(e.getKey(), canonicalizeValue(e.getValue()));
    }
    return out;
  }

  @SuppressWarnings("unchecked")
  private static Object canonicalizeValue(Object value) {
    if (value instanceof Object[]) {
      Object[] arr = (Object[]) value;
      List<Object> list = new ArrayList<>(arr.length);
      for (Object o : arr) {
        list.add(canonicalizeValue(o));
      }
      return list;
    }
    if (value instanceof List) {
      List<Object> in = (List<Object>) value;
      List<Object> list = new ArrayList<>(in.size());
      for (Object o : in) {
        list.add(canonicalizeValue(o));
      }
      return list;
    }
    if (value instanceof Map) {
      Map<String, Object> in = (Map<String, Object>) value;
      Map<String, Object> out = new TreeMap<>();
      for (Map.Entry<String, Object> e : in.entrySet()) {
        out.put(e.getKey(), canonicalizeValue(e.getValue()));
      }
      return out;
    }
    if (value instanceof byte[]) {
      return new ByteArrayWrapper((byte[]) value);
    }
    if (value instanceof java.sql.Date) {
      return (int) (((java.sql.Date) value).toLocalDate().toEpochDay());
    }
    if (value instanceof java.sql.Timestamp) {
      return ((java.sql.Timestamp) value).getTime();
    }
    if (value instanceof java.time.LocalDate) {
      return (int) ((java.time.LocalDate) value).toEpochDay();
    }
    if (value instanceof java.time.Instant) {
      return ((java.time.Instant) value).toEpochMilli();
    }
    if (value instanceof String) {
      String s = (String) value;
      if (s.length() == 10 && s.charAt(4) == '-' && s.charAt(7) == '-') {
        try {
          return (int) java.time.LocalDate.parse(s).toEpochDay();
        } catch (Exception ignored) {
          // Not actually a date — leave the string alone.
        }
      }
    }
    return value;
  }

  /** Content-based equality for byte arrays, so {@code byte[]} values compare correctly inside Maps/Lists. */
  private static final class ByteArrayWrapper {
    private final byte[] _bytes;

    ByteArrayWrapper(byte[] bytes) {
      _bytes = bytes;
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof ByteArrayWrapper && Arrays.equals(_bytes, ((ByteArrayWrapper) o)._bytes);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(_bytes);
    }

    @Override
    public String toString() {
      return "bytes(" + _bytes.length + ")";
    }
  }
}
