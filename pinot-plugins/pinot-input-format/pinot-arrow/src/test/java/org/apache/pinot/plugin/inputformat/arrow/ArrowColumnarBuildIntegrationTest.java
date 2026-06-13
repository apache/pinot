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
import java.util.Collections;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.utils.CrcUtils;
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
import static org.testng.Assert.assertTrue;


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
          Object expected = rowA.getValue(column);
          Object actual = rowB.getValue(column);
          String message = "value mismatch on column " + column + " at docId " + docId;
          if (expected instanceof Object[] && actual instanceof Object[]) {
            // Multi-value columns surface as Object[]; compare element-wise.
            assertEquals((Object[]) actual, (Object[]) expected, message);
          } else {
            assertEquals(actual, expected, message);
          }
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

  /**
   * Guardrail for any change that processes the Arrow source one record batch at a time: a segment
   * built from a multi-batch file must be byte-identical (same-path data CRC) and per-docId equal to
   * one built from a single-batch file carrying the same logical rows, and the naturally-ascending
   * column must stay sorted across the batch boundary. A batch-boundary ordering bug would change
   * {@code isSorted}, the inverted index on {@code category}, or the per-doc values. Also cross-checks
   * the multi-batch columnar segment against the row-major segment from the same file.
   *
   * <p>Passes on the current materialize-all build; its purpose is to fail loudly if a future
   * batch-bounded reader misorders docs across a boundary.
   */
  @Test
  public void testMultiBatchColumnarEquivalence()
      throws Exception {
    String[] columns = {"id", "category", "label"};
    File singleBatchFile = writeGuardrailArrowFile("guardrail-single.arrow", 1, 60);
    File multiBatchFile = writeGuardrailArrowFile("guardrail-multi.arrow", 3, 20);

    File singleColSeg = buildGuardrailSegment(singleBatchFile, "guardSingleCol", true);
    File multiColSeg = buildGuardrailSegment(multiBatchFile, "guardMultiCol", true);
    File multiRowSeg = buildGuardrailSegment(multiBatchFile, "guardMultiRow", false);

    // Same logical data, different batching -> byte-identical data files and per-doc values.
    assertEquals(CrcUtils.computeDataCrc(multiColSeg), CrcUtils.computeDataCrc(singleColSeg),
        "Multi-batch and single-batch columnar segments must have identical data CRC");
    assertPerDocRecordEquivalence(singleColSeg, multiColSeg, columns);
    // Cross-path: multi-batch columnar == row-major.
    assertPerDocRecordEquivalence(multiRowSeg, multiColSeg, columns);

    // The ascending column stays sorted across the batch boundary.
    IndexSegment seg = ImmutableSegmentLoader.load(multiColSeg, ReadMode.heap);
    try {
      assertTrue(seg.getSegmentMetadata().getColumnMetadataFor("id").isSorted(),
          "id must be detected as sorted across batches");
    } finally {
      seg.destroy();
    }
  }

  private File buildGuardrailSegment(File arrowFile, String segmentName, boolean columnar)
      throws Exception {
    org.apache.pinot.spi.data.Schema schema = new org.apache.pinot.spi.data.Schema();
    schema.setSchemaName(TABLE_NAME);
    schema.addField(new DimensionFieldSpec("id", DataType.INT, true));
    schema.addField(new DimensionFieldSpec("category", DataType.INT, true));
    schema.addField(new DimensionFieldSpec("label", DataType.STRING, true));
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setInvertedIndexColumns(List.of("category"))
        .setCreateInvertedIndexDuringSegmentGeneration(true)
        .build();

    File outDir = _tempDir.resolve("g-" + segmentName).toFile();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(outDir.getAbsolutePath());
    config.setSegmentName(segmentName);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    if (columnar) {
      try (ArrowFileColumnReaderFactory factory = new ArrowFileColumnReaderFactory(arrowFile)) {
        driver.init(config, factory);
        driver.build();
      }
    } else {
      try (ArrowRecordReader reader = new ArrowRecordReader()) {
        reader.init(arrowFile, null, null);
        driver.init(config, reader);
        driver.build();
      }
    }
    return new File(outDir, segmentName);
  }

  private File writeGuardrailArrowFile(String fileName, int batchCount, int rowsPerBatch)
      throws IOException {
    Field idField = new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Field catField = new Field("category", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Field labelField = new Field("label", FieldType.nullable(new ArrowType.Utf8()), null);
    Schema arrowSchema = new Schema(Arrays.asList(idField, catField, labelField));

    File out = _tempDir.resolve(fileName).toFile();
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator);
        FileOutputStream fos = new FileOutputStream(out);
        FileChannel channel = fos.getChannel();
        ArrowFileWriter writer = new ArrowFileWriter(root, null, channel)) {
      IntVector idVec = (IntVector) root.getVector("id");
      IntVector catVec = (IntVector) root.getVector("category");
      VarCharVector labelVec = (VarCharVector) root.getVector("label");

      writer.start();
      int globalRow = 0;
      for (int batch = 0; batch < batchCount; batch++) {
        root.allocateNew();
        idVec.allocateNew(rowsPerBatch);
        catVec.allocateNew(rowsPerBatch);
        labelVec.allocateNew(rowsPerBatch * 10L, rowsPerBatch);
        for (int row = 0; row < rowsPerBatch; row++) {
          idVec.set(row, globalRow);             // globally ascending -> sorted across batches
          catVec.set(row, globalRow % 4);        // low cardinality -> inverted index
          labelVec.set(row, ("row_" + globalRow).getBytes(StandardCharsets.UTF_8));
          globalRow++;
        }
        idVec.setValueCount(rowsPerBatch);
        catVec.setValueCount(rowsPerBatch);
        labelVec.setValueCount(rowsPerBatch);
        root.setRowCount(rowsPerBatch);
        writer.writeBatch();
      }
      writer.end();
    }
    return out;
  }

  /**
   * The richest end-to-end equivalence: a multi-batch Arrow file combining a dictionary-encoded
   * string, a nullable int with nulls crossing batch boundaries, a BOOLEAN, and a TIMESTAMP, built
   * both row-major and column-major. Exercises together and across batch boundaries: the
   * batch-bounded reader's per-batch dictionary decode, the null-value vector + default substitution,
   * and Boolean/Timestamp stored-type coercion. Asserts per-docId record equality, metadata, and that
   * the ascending id stays sorted across batches.
   */
  @Test
  public void testRichMultiBatchEquivalence()
      throws Exception {
    File arrowFile = writeRichMultiBatchArrowFile("rich-multi.arrow", 3, 10); // 30 docs, nulls at 0,5,10,...
    String[] columns = {"id", "color", "score", "flag", "ts"};

    org.apache.pinot.spi.data.Schema schema = new org.apache.pinot.spi.data.Schema();
    schema.setSchemaName(TABLE_NAME);
    schema.addField(new DimensionFieldSpec("id", DataType.INT, true));
    schema.addField(new DimensionFieldSpec("color", DataType.STRING, true));
    schema.addField(new DimensionFieldSpec("score", DataType.INT, true));
    schema.addField(new DimensionFieldSpec("flag", DataType.BOOLEAN, true));
    schema.addField(new DimensionFieldSpec("ts", DataType.TIMESTAMP, true));
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNullHandlingEnabled(true).build();

    File rowMajorSeg = buildSegment(arrowFile, "richRow", schema, tableConfig, false);
    File columnarSeg = buildSegment(arrowFile, "richCol", schema, tableConfig, true);

    assertSegmentMetadataEquivalence(rowMajorSeg, columnarSeg);
    assertPerDocRecordEquivalence(rowMajorSeg, columnarSeg, columns);

    IndexSegment seg = ImmutableSegmentLoader.load(columnarSeg, ReadMode.heap);
    try {
      assertTrue(seg.getSegmentMetadata().getColumnMetadataFor("id").isSorted(),
          "id must be sorted across batches");
    } finally {
      seg.destroy();
    }
  }

  /**
   * Multi-value column end-to-end across a batch boundary: a multi-batch Arrow file with an MV int
   * column, built row-major and column-major. Exercises the column-major MV path through
   * {@code ColumnarValueNormalizer} (Object[] standardisation) and the batch-bounded reader's MV reads
   * across batches, asserting per-docId MV equality with the row-major segment.
   */
  @Test
  public void testMultiValueMultiBatchEquivalence()
      throws Exception {
    File arrowFile = writeMultiValueMultiBatchArrowFile("mv-multi.arrow", 2, 4); // 8 docs
    String[] columns = {"id", "intArr"};

    org.apache.pinot.spi.data.Schema schema = new org.apache.pinot.spi.data.Schema.SchemaBuilder()
        .setSchemaName(TABLE_NAME)
        .addSingleValueDimension("id", DataType.INT)
        .addMultiValueDimension("intArr", DataType.INT)
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();

    File rowMajorSeg = buildSegment(arrowFile, "mvRow", schema, tableConfig, false);
    File columnarSeg = buildSegment(arrowFile, "mvCol", schema, tableConfig, true);

    assertSegmentMetadataEquivalence(rowMajorSeg, columnarSeg);
    assertPerDocRecordEquivalence(rowMajorSeg, columnarSeg, columns);
  }

  private File writeMultiValueMultiBatchArrowFile(String fileName, int batchCount, int rowsPerBatch)
      throws IOException {
    File out = _tempDir.resolve(fileName).toFile();
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Field idField = new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null);
      Field elementField = new Field("element", FieldType.nullable(new ArrowType.Int(32, true)), null);
      Field listField = new Field("intArr", FieldType.nullable(ArrowType.List.INSTANCE),
          Collections.singletonList(elementField));
      Schema arrowSchema = new Schema(Arrays.asList(idField, listField));

      try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator);
          FileOutputStream fos = new FileOutputStream(out);
          FileChannel channel = fos.getChannel();
          ArrowFileWriter writer = new ArrowFileWriter(root, null, channel)) {
        IntVector idVec = (IntVector) root.getVector("id");
        ListVector listVec = (ListVector) root.getVector("intArr");

        writer.start();
        int g = 0;
        for (int b = 0; b < batchCount; b++) {
          idVec.allocateNew(rowsPerBatch);
          listVec.allocateNew();
          UnionListWriter listWriter = listVec.getWriter();
          for (int i = 0; i < rowsPerBatch; i++) {
            idVec.set(i, g);
            listWriter.setPosition(i);
            listWriter.startList();
            for (int k = 0; k <= g % 2; k++) {   // length 1 or 2; no null / empty elements
              listWriter.integer().writeInt(g * 10 + k);
            }
            listWriter.endList();
            g++;
          }
          idVec.setValueCount(rowsPerBatch);
          listVec.setValueCount(rowsPerBatch);
          root.setRowCount(rowsPerBatch);
          writer.writeBatch();
        }
        writer.end();
      }
    }
    return out;
  }

  private File buildSegment(File arrowFile, String segmentName, org.apache.pinot.spi.data.Schema schema,
      TableConfig tableConfig, boolean columnar)
      throws Exception {
    File outDir = _tempDir.resolve("build-" + segmentName).toFile();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(outDir.getAbsolutePath());
    config.setSegmentName(segmentName);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    if (columnar) {
      try (ArrowFileColumnReaderFactory factory = new ArrowFileColumnReaderFactory(arrowFile)) {
        driver.init(config, factory);
        driver.build();
      }
    } else {
      try (ArrowRecordReader reader = new ArrowRecordReader()) {
        reader.init(arrowFile, null, null);
        driver.init(config, reader);
        driver.build();
      }
    }
    return new File(outDir, segmentName);
  }

  private File writeRichMultiBatchArrowFile(String fileName, int batchCount, int rowsPerBatch)
      throws IOException {
    String[] palette = {"red", "green", "blue"};
    DictionaryEncoding encoding = new DictionaryEncoding(1L, false, new ArrowType.Int(32, true));
    File out = _tempDir.resolve(fileName).toFile();
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      VarCharVector dictVec = new VarCharVector("color-dict", allocator);
      dictVec.allocateNew();
      for (int i = 0; i < palette.length; i++) {
        dictVec.setSafe(i, palette[i].getBytes(StandardCharsets.UTF_8));
      }
      dictVec.setValueCount(palette.length);
      Dictionary dictionary = new Dictionary(dictVec, encoding);
      DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
      provider.put(dictionary);
      try {
        Field idField = new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null);
        // "color" is an Int32 index vector carrying the dictionary encoding.
        Field colorField = new Field("color", new FieldType(true, new ArrowType.Int(32, true), encoding), null);
        Field scoreField = new Field("score", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field flagField = new Field("flag", FieldType.nullable(new ArrowType.Bool()), null);
        Field tsField =
            new Field("ts", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)), null);
        Schema arrowSchema = new Schema(Arrays.asList(idField, colorField, scoreField, flagField, tsField));

        try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator);
            FileOutputStream fos = new FileOutputStream(out);
            FileChannel channel = fos.getChannel();
            ArrowFileWriter writer = new ArrowFileWriter(root, provider, channel)) {
          IntVector idVec = (IntVector) root.getVector("id");
          IntVector colorVec = (IntVector) root.getVector("color");
          IntVector scoreVec = (IntVector) root.getVector("score");
          BitVector flagVec = (BitVector) root.getVector("flag");
          TimeStampMilliVector tsVec = (TimeStampMilliVector) root.getVector("ts");

          writer.start();
          int g = 0;
          for (int b = 0; b < batchCount; b++) {
            idVec.allocateNew(rowsPerBatch);
            colorVec.allocateNew(rowsPerBatch);
            scoreVec.allocateNew(rowsPerBatch);
            flagVec.allocateNew(rowsPerBatch);
            tsVec.allocateNew(rowsPerBatch);
            for (int i = 0; i < rowsPerBatch; i++) {
              idVec.set(i, g);                        // ascending -> sorted across batches
              colorVec.set(i, g % palette.length);    // dictionary index
              if (g % 5 == 0) {
                scoreVec.setNull(i);                  // nulls at 0,5,10,15,20,25 -> cross batch boundaries
              } else {
                scoreVec.set(i, g * 7);
              }
              flagVec.set(i, g % 2);
              tsVec.set(i, 1_700_000_000_000L + g);
              g++;
            }
            idVec.setValueCount(rowsPerBatch);
            colorVec.setValueCount(rowsPerBatch);
            scoreVec.setValueCount(rowsPerBatch);
            flagVec.setValueCount(rowsPerBatch);
            tsVec.setValueCount(rowsPerBatch);
            root.setRowCount(rowsPerBatch);
            writer.writeBatch();
          }
          writer.end();
        }
      } finally {
        dictVec.close();
      }
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
