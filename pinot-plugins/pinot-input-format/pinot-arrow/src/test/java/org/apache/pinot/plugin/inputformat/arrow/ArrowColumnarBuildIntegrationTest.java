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
package org.apache.pinot.plugin.inputformat.arrow;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


/**
 * End-to-end tests that build a Pinot segment from the same Arrow data via row-major and
 * column-major paths and assert the produced segments carry equivalent metadata.
 *
 * <p>Paths exercised:
 * <ul>
 *   <li>Row-major: {@link ArrowRecordReader} → {@code driver.init(config, recordReader)} →
 *       {@code driver.build()}</li>
 *   <li>Column-major (file): {@link ArrowFileColumnReaderFactory} →
 *       {@code driver.init(config, columnReaderFactory)} → {@code driver.build()} →
 *       {@code buildColumnar()}</li>
 *   <li>Column-major (in-memory, disk-free): {@link ArrowColumnReaderFactory} over an
 *       {@link ArrowStreamReader} backed by a {@link ByteArrayInputStream} → same driver path.
 *       Proves the disk-free path produces the same segment metadata as the file path.</li>
 * </ul>
 *
 * <p>The test asserts identical per-column cardinality, min, max, totalDocs, data type, and
 * segment-level doc count across each compared pair.
 */
public class ArrowColumnarBuildIntegrationTest {

  private static final String TABLE_NAME = "arrowColumnarBuildTest";
  private static final int ROW_COUNT = 64;

  private Path _tempDir;

  @BeforeMethod
  public void setUp()
      throws IOException {
    _tempDir = Files.createTempDirectory("arrow-columnar-integration");
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    if (_tempDir != null) {
      try (var stream = Files.walk(_tempDir)) {
        stream.sorted((a, b) -> b.compareTo(a)).forEach(p -> {
          try {
            Files.deleteIfExists(p);
          } catch (IOException ignored) {
            // best effort
          }
        });
      }
    }
  }

  @Test
  public void testSegmentMetadataEquivalenceRowMajorVsFileColumnar()
      throws Exception {
    File arrowFile = writeArrowFileFixture("equivalence-file.arrow");

    File rowMajorSegmentDir = buildSegmentRowMajor(arrowFile, "rowMajor");
    File columnarSegmentDir = buildSegmentFileColumnar(arrowFile, "fileColumnar");

    assertSegmentMetadataEquivalence(rowMajorSegmentDir, columnarSegmentDir);
  }

  @Test
  public void testSegmentMetadataEquivalenceRowMajorVsInMemoryColumnar()
      throws Exception {
    // Same data is materialised two ways. The row-major path needs a file because
    // ArrowRecordReader.init only accepts File; the in-memory column-major path consumes the
    // same data as Arrow IPC stream bytes via ArrowStreamReader — no file touched on that path.
    File arrowFile = writeArrowFileFixture("equivalence-inmem-source.arrow");
    byte[] streamBytes = writeArrowStreamBytes();

    File rowMajorSegmentDir = buildSegmentRowMajor(arrowFile, "rowMajor");
    File columnarSegmentDir = buildSegmentInMemoryColumnar(streamBytes, "inMemColumnar");

    assertSegmentMetadataEquivalence(rowMajorSegmentDir, columnarSegmentDir);
  }

  /**
   * BOOLEAN and TIMESTAMP columns exercise the column-major type-normalization path: the Arrow source
   * surfaces {@code Boolean} / {@code Timestamp} (or {@code LocalDateTime}) values that must be coerced
   * to the stored {@code INT} / {@code LONG} to match what the row-major {@code DataTypeTransformer}
   * produces. Before normalization the column-major build crashed on these types (the typed stats
   * collectors / index creators cast-failed on the source Java type). Asserts both per-column metadata
   * and per-docId record values match the row-major segment built from the same file.
   */
  @Test
  public void testTypeNormalizationRowMajorVsColumnar()
      throws Exception {
    File arrowFile = writeTypedArrowFileFixture("typed-equivalence.arrow");
    org.apache.pinot.spi.data.Schema schema = typedPinotSchema();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();

    File rowMajorDir = _tempDir.resolve("rm-typed").toFile();
    try (ArrowRecordReader reader = new ArrowRecordReader()) {
      reader.init(arrowFile, null, null);
      SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
      config.setOutDir(rowMajorDir.getAbsolutePath());
      config.setSegmentName("rmTyped");
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(config, reader);
      driver.build();
    }

    File columnarDir = _tempDir.resolve("col-typed").toFile();
    try (ArrowFileColumnReaderFactory factory = new ArrowFileColumnReaderFactory(arrowFile)) {
      SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
      config.setOutDir(columnarDir.getAbsolutePath());
      config.setSegmentName("colTyped");
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(config, factory);
      driver.build();
    }

    File rowMajorSeg = new File(rowMajorDir, "rmTyped");
    File columnarSeg = new File(columnarDir, "colTyped");
    assertSegmentMetadataEquivalence(rowMajorSeg, columnarSeg);
    assertPerDocRecordEquivalence(rowMajorSeg, columnarSeg,
        new String[]{"intCol", "boolCol", "tsCol", "stringCol"});
  }

  private void assertPerDocRecordEquivalence(File segDirA, File segDirB, String[] columns)
      throws Exception {
    IndexSegment segA = ImmutableSegmentLoader.load(segDirA, ReadMode.heap);
    IndexSegment segB = ImmutableSegmentLoader.load(segDirB, ReadMode.heap);
    try {
      int numDocs = segA.getSegmentMetadata().getTotalDocs();
      assertEquals(segB.getSegmentMetadata().getTotalDocs(), numDocs, "doc count mismatch");
      GenericRow rowA = new GenericRow();
      GenericRow rowB = new GenericRow();
      for (int docId = 0; docId < numDocs; docId++) {
        rowA.clear();
        rowB.clear();
        segA.getRecord(docId, rowA);
        segB.getRecord(docId, rowB);
        for (String column : columns) {
          assertEquals(rowB.getValue(column), rowA.getValue(column),
              "value mismatch on column " + column + " at docId " + docId);
        }
      }
    } finally {
      segA.destroy();
      segB.destroy();
    }
  }

  private org.apache.pinot.spi.data.Schema typedPinotSchema() {
    org.apache.pinot.spi.data.Schema schema = new org.apache.pinot.spi.data.Schema();
    schema.setSchemaName(TABLE_NAME);
    schema.addField(new DimensionFieldSpec("intCol", DataType.INT, true));
    schema.addField(new DimensionFieldSpec("boolCol", DataType.BOOLEAN, true));
    schema.addField(new DimensionFieldSpec("tsCol", DataType.TIMESTAMP, true));
    schema.addField(new DimensionFieldSpec("stringCol", DataType.STRING, true));
    return schema;
  }

  private File writeTypedArrowFileFixture(String fileName)
      throws IOException {
    Field intField = new Field("intCol", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Field boolField = new Field("boolCol", FieldType.nullable(new ArrowType.Bool()), null);
    Field tsField =
        new Field("tsCol", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)), null);
    Field stringField = new Field("stringCol", FieldType.nullable(new ArrowType.Utf8()), null);
    Schema arrowSchema = new Schema(Arrays.asList(intField, boolField, tsField, stringField));

    File out = _tempDir.resolve(fileName).toFile();
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator);
        FileOutputStream fos = new FileOutputStream(out);
        FileChannel channel = fos.getChannel();
        ArrowFileWriter writer = new ArrowFileWriter(root, null, channel)) {
      IntVector intVec = (IntVector) root.getVector("intCol");
      BitVector boolVec = (BitVector) root.getVector("boolCol");
      TimeStampMilliVector tsVec = (TimeStampMilliVector) root.getVector("tsCol");
      VarCharVector stringVec = (VarCharVector) root.getVector("stringCol");

      intVec.allocateNew(ROW_COUNT);
      boolVec.allocateNew(ROW_COUNT);
      tsVec.allocateNew(ROW_COUNT);
      stringVec.allocateNew(ROW_COUNT * 8, ROW_COUNT);

      for (int i = 0; i < ROW_COUNT; i++) {
        intVec.set(i, i);
        boolVec.set(i, i % 2);                     // alternating false/true -> stored INT 0/1
        tsVec.set(i, 1_700_000_000_000L + i);      // epoch millis
        stringVec.set(i, ("row_" + i).getBytes(StandardCharsets.UTF_8));
      }
      intVec.setValueCount(ROW_COUNT);
      boolVec.setValueCount(ROW_COUNT);
      tsVec.setValueCount(ROW_COUNT);
      stringVec.setValueCount(ROW_COUNT);
      root.setRowCount(ROW_COUNT);

      writer.start();
      writer.writeBatch();
      writer.end();
    }
    return out;
  }

  private void assertSegmentMetadataEquivalence(File rowMajorSegmentDir, File columnarSegmentDir)
      throws Exception {
    IndexSegment rowMajorSegment = ImmutableSegmentLoader.load(rowMajorSegmentDir, ReadMode.heap);
    IndexSegment columnarSegment = ImmutableSegmentLoader.load(columnarSegmentDir, ReadMode.heap);
    try {
      SegmentMetadata rowMajorMeta = rowMajorSegment.getSegmentMetadata();
      SegmentMetadata columnarMeta = columnarSegment.getSegmentMetadata();

      assertEquals(columnarMeta.getTotalDocs(), rowMajorMeta.getTotalDocs(),
          "Segment-level doc count must match between row-major and columnar builds");

      // Compare only the user-defined columns — virtual columns ($segmentName, $docId, ...)
      // are populated post-build and their min/max naturally differ across segments.
      for (String column : rowMajorMeta.getAllColumns()) {
        if (column.startsWith("$")) {
          continue;
        }
        ColumnMetadata rmCol = rowMajorMeta.getColumnMetadataFor(column);
        ColumnMetadata colCol = columnarMeta.getColumnMetadataFor(column);
        assertNotNull(rmCol, "Row-major missing column metadata for " + column);
        assertNotNull(colCol, "Columnar missing column metadata for " + column);
        assertEquals(colCol.getCardinality(), rmCol.getCardinality(),
            "Cardinality mismatch on column " + column);
        assertEquals(colCol.getMinValue(), rmCol.getMinValue(),
            "Min value mismatch on column " + column);
        assertEquals(colCol.getMaxValue(), rmCol.getMaxValue(),
            "Max value mismatch on column " + column);
        assertEquals(colCol.getTotalDocs(), rmCol.getTotalDocs(),
            "Per-column doc count mismatch on column " + column);
        assertEquals(colCol.getDataType(), rmCol.getDataType(),
            "Data type mismatch on column " + column);
      }
    } finally {
      rowMajorSegment.destroy();
      columnarSegment.destroy();
    }
  }

  private File buildSegmentRowMajor(File arrowFile, String segmentName)
      throws Exception {
    File outputDir = _tempDir.resolve("rm-out-" + segmentName).toFile();
    SegmentGeneratorConfig config = newSegmentConfig(outputDir, segmentName);
    try (ArrowRecordReader recordReader = new ArrowRecordReader()) {
      recordReader.init(arrowFile, null, null);
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(config, recordReader);
      driver.build();
    }
    return new File(outputDir, segmentName);
  }

  private File buildSegmentFileColumnar(File arrowFile, String segmentName)
      throws Exception {
    File outputDir = _tempDir.resolve("col-file-out-" + segmentName).toFile();
    SegmentGeneratorConfig config = newSegmentConfig(outputDir, segmentName);
    try (ArrowFileColumnReaderFactory factory = new ArrowFileColumnReaderFactory(arrowFile)) {
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(config, factory);
      driver.build();
    }
    return new File(outputDir, segmentName);
  }

  private File buildSegmentInMemoryColumnar(byte[] streamBytes, String segmentName)
      throws Exception {
    File outputDir = _tempDir.resolve("col-inmem-out-" + segmentName).toFile();
    SegmentGeneratorConfig config = newSegmentConfig(outputDir, segmentName);
    // Caller manages the allocator and the ArrowStreamReader; the factory borrows them.
    try (RootAllocator callerAllocator = new RootAllocator(Long.MAX_VALUE);
        ArrowStreamReader streamReader = new ArrowStreamReader(
            Channels.newChannel(new ByteArrayInputStream(streamBytes)), callerAllocator);
        ArrowColumnReaderFactory factory =
            new ArrowColumnReaderFactory(streamReader, callerAllocator)) {
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(config, factory);
      driver.build();
    }
    return new File(outputDir, segmentName);
  }

  private SegmentGeneratorConfig newSegmentConfig(File outputDir, String segmentName) {
    org.apache.pinot.spi.data.Schema schema = new org.apache.pinot.spi.data.Schema();
    schema.setSchemaName(TABLE_NAME);
    schema.addField(new DimensionFieldSpec("intCol", DataType.INT, true));
    schema.addField(new DimensionFieldSpec("longCol", DataType.LONG, true));
    schema.addField(new DimensionFieldSpec("stringCol", DataType.STRING, true));

    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(outputDir.getAbsolutePath());
    config.setSegmentName(segmentName);
    return config;
  }

  private File writeArrowFileFixture(String fileName)
      throws IOException {
    File out = _tempDir.resolve(fileName).toFile();
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot root = VectorSchemaRoot.create(fixtureArrowSchema(), allocator);
        FileOutputStream fos = new FileOutputStream(out);
        FileChannel channel = fos.getChannel();
        ArrowFileWriter writer = new ArrowFileWriter(root, null, channel)) {
      populateFixtureVectors(root);
      writer.start();
      writer.writeBatch();
      writer.end();
    }
    return out;
  }

  private byte[] writeArrowStreamBytes()
      throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot root = VectorSchemaRoot.create(fixtureArrowSchema(), allocator);
        ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(out))) {
      populateFixtureVectors(root);
      writer.start();
      writer.writeBatch();
      writer.end();
    }
    return out.toByteArray();
  }

  private Schema fixtureArrowSchema() {
    Field intField = new Field("intCol", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Field longField = new Field("longCol", FieldType.nullable(new ArrowType.Int(64, true)), null);
    Field stringField = new Field("stringCol", FieldType.nullable(new ArrowType.Utf8()), null);
    return new Schema(Arrays.asList(intField, longField, stringField));
  }

  private void populateFixtureVectors(VectorSchemaRoot root) {
    IntVector intVec = (IntVector) root.getVector("intCol");
    BigIntVector longVec = (BigIntVector) root.getVector("longCol");
    VarCharVector stringVec = (VarCharVector) root.getVector("stringCol");

    intVec.allocateNew(ROW_COUNT);
    longVec.allocateNew(ROW_COUNT);
    stringVec.allocateNew(ROW_COUNT * 8, ROW_COUNT);

    for (int i = 0; i < ROW_COUNT; i++) {
      intVec.set(i, i);
      longVec.set(i, (long) i * 1000);
      stringVec.set(i, ("row_" + i).getBytes(StandardCharsets.UTF_8));
    }
    intVec.setValueCount(ROW_COUNT);
    longVec.setValueCount(ROW_COUNT);
    stringVec.setValueCount(ROW_COUNT);
    root.setRowCount(ROW_COUNT);
  }
}
