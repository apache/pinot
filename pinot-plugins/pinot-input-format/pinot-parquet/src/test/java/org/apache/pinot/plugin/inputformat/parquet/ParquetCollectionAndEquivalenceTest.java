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
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.ByteArray;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


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
    reader.init(file, null, null);
    List<GenericRow> rows = new ArrayList<>();
    while (reader.hasNext()) {
      GenericRow row = new GenericRow();
      reader.next(row);
      rows.add(row.copy());
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
