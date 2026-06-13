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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.ColumnReader;
import org.apache.pinot.spi.data.readers.MultiValueResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/**
 * Unit tests for {@link ArrowFileColumnReaderFactory} and {@link ArrowColumnReader}.
 *
 * <p>Each test materialises a small Arrow IPC file on disk with a known fixture and verifies
 * that the factory can read it back column-by-column using all three documented patterns:
 * generic sequential {@code next()}, typed sequential {@code nextInt() / nextLong() / ...},
 * and random-access {@code getInt(docId) / getLong(docId) / ...}.
 */
public class ArrowFileColumnReaderFactoryTest {

  private static final int ROWS = 8;
  private Path _tempDir;

  @BeforeMethod
  public void setUp()
      throws IOException {
    _tempDir = Files.createTempDirectory("arrow-column-reader-test");
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
  public void testSequentialReadAllPrimitiveTypes()
      throws IOException {
    File arrowFile = writePrimitiveFixture("primitive.arrow");
    org.apache.pinot.spi.data.Schema schema = primitiveSchema();

    try (ArrowFileColumnReaderFactory factory = new ArrowFileColumnReaderFactory(arrowFile)) {
      factory.init(schema);

      Map<String, ColumnReader> readers = factory.getAllColumnReaders();
      assertEquals(readers.size(), 6);
      for (ColumnReader r : readers.values()) {
        assertEquals(r.getTotalDocs(), ROWS);
        assertTrue(r.isSingleValue());
      }

      ColumnReader intReader = readers.get("intCol");
      ColumnReader longReader = readers.get("longCol");
      ColumnReader floatReader = readers.get("floatCol");
      ColumnReader doubleReader = readers.get("doubleCol");
      ColumnReader stringReader = readers.get("stringCol");
      ColumnReader bytesReader = readers.get("bytesCol");

      assertTrue(intReader.isInt());
      assertTrue(longReader.isLong());
      assertTrue(floatReader.isFloat());
      assertTrue(doubleReader.isDouble());
      assertTrue(stringReader.isString());
      assertTrue(bytesReader.isBytes());

      // Pattern 2: typed sequential read with null handling
      for (int i = 0; i < ROWS; i++) {
        if (i == 3) {
          assertTrue(intReader.isNextNull(), "Row 3 INT should be null");
          intReader.skipNext();
          assertTrue(longReader.isNextNull(), "Row 3 LONG should be null");
          longReader.skipNext();
          assertTrue(stringReader.isNextNull(), "Row 3 STRING should be null");
          stringReader.skipNext();
          floatReader.nextFloat();
          doubleReader.nextDouble();
          bytesReader.nextBytes();
        } else {
          assertFalse(intReader.isNextNull());
          assertEquals(intReader.nextInt(), i * 10);
          assertEquals(longReader.nextLong(), (long) i * 100);
          assertEquals(floatReader.nextFloat(), i + 0.5f, 0.0001f);
          assertEquals(doubleReader.nextDouble(), i + 0.25, 0.0001);
          assertEquals(stringReader.nextString(), "s_" + i);
          assertEquals(new String(bytesReader.nextBytes()), "b_" + i);
        }
      }

      // Verify rewind enables a second pass.
      intReader.rewind();
      stringReader.rewind();
      assertTrue(intReader.hasNext());
      assertEquals(intReader.nextInt(), 0);
      assertEquals(stringReader.nextString(), "s_0");
    }
  }

  @Test
  public void testRandomAccessByDocId()
      throws IOException {
    File arrowFile = writePrimitiveFixture("random.arrow");
    org.apache.pinot.spi.data.Schema schema = primitiveSchema();

    try (ArrowFileColumnReaderFactory factory = new ArrowFileColumnReaderFactory(arrowFile)) {
      factory.init(schema);
      ColumnReader intReader = factory.getColumnReader("intCol");
      ColumnReader longReader = factory.getColumnReader("longCol");

      // Pattern 3: read out of order.
      assertEquals(intReader.getInt(5), 50);
      assertEquals(intReader.getInt(0), 0);
      assertEquals(intReader.getInt(7), 70);
      assertTrue(intReader.isNull(3));
      assertEquals(longReader.getLong(2), 200L);
      assertEquals(longReader.getValue(3), null);

      // Sequential read is unaffected by prior random-access reads on the same reader.
      assertTrue(intReader.hasNext());
      assertEquals(intReader.nextInt(), 0);
    }
  }

  @Test
  public void testGenericNextHandlesNulls()
      throws IOException {
    File arrowFile = writePrimitiveFixture("generic.arrow");
    org.apache.pinot.spi.data.Schema schema = primitiveSchema();

    try (ArrowFileColumnReaderFactory factory = new ArrowFileColumnReaderFactory(arrowFile)) {
      factory.init(schema);
      ColumnReader stringReader = factory.getColumnReader("stringCol");

      int nullCount = 0;
      int nonNullCount = 0;
      while (stringReader.hasNext()) {
        Object value = stringReader.next();
        if (value == null) {
          nullCount++;
        } else {
          nonNullCount++;
        }
      }
      assertEquals(nullCount, 1);
      assertEquals(nonNullCount, ROWS - 1);
    }
  }

  @Test
  public void testMultiValueIntColumn()
      throws IOException {
    File arrowFile = writeMultiValueIntFixture("mv-int.arrow");
    org.apache.pinot.spi.data.Schema schema = new org.apache.pinot.spi.data.Schema.SchemaBuilder()
        .setSchemaName("mv")
        .addMultiValueDimension("intArr", FieldSpec.DataType.INT)
        .build();

    try (ArrowFileColumnReaderFactory factory = new ArrowFileColumnReaderFactory(arrowFile)) {
      factory.init(schema);
      ColumnReader reader = factory.getColumnReader("intArr");
      assertNotNull(reader);
      assertFalse(reader.isSingleValue());
      assertEquals(reader.getTotalDocs(), 3);

      MultiValueResult<int[]> doc0 = reader.getIntMV(0);
      assertEquals(doc0.getValues(), new int[]{1, 2, 3});
      assertFalse(doc0.hasNulls());

      MultiValueResult<int[]> doc1 = reader.getIntMV(1);
      assertEquals(doc1.getValues().length, 2);
      assertTrue(doc1.hasNulls());
      assertTrue(doc1.isNull(1));
      assertFalse(doc1.isNull(0));
      assertEquals(doc1.getValues()[0], 4);

      MultiValueResult<int[]> doc2 = reader.getIntMV(2);
      assertEquals(doc2.getValues().length, 0);
    }
  }

  /**
   * Type predicates on a multi-value reader reflect the list's <i>element</i> type, not the
   * {@code ListVector} itself — so an INT multi-value column reports {@code isInt()} (and is not
   * single-value), letting the build pick the right typed {@code getXxxMV} accessor.
   */
  @Test
  public void testMultiValueColumnTypePredicates()
      throws IOException {
    File arrowFile = writeMultiValueIntFixture("mv-predicates.arrow");
    org.apache.pinot.spi.data.Schema schema = new org.apache.pinot.spi.data.Schema.SchemaBuilder()
        .setSchemaName("mv")
        .addMultiValueDimension("intArr", FieldSpec.DataType.INT)
        .build();

    try (ArrowFileColumnReaderFactory factory = new ArrowFileColumnReaderFactory(arrowFile)) {
      factory.init(schema);
      ColumnReader reader = factory.getColumnReader("intArr");
      assertNotNull(reader);
      assertFalse(reader.isSingleValue());
      assertTrue(reader.isInt(), "INT element type should report isInt()");
      assertFalse(reader.isLong());
      assertFalse(reader.isString());
      assertFalse(reader.isBytes());
    }
  }

  /**
   * A wrong-type accessor must fail with {@link IOException} (the reader's {@code typeMismatch}
   * contract), not return garbage or throw an opaque cast error: calling a STRING accessor on an INT
   * column, a scalar accessor mismatch, and a multi-value accessor on a single-value column.
   */
  @Test
  public void testTypeMismatchAccessorsThrow()
      throws IOException {
    File arrowFile = writePrimitiveFixture("type-mismatch.arrow");
    org.apache.pinot.spi.data.Schema schema = primitiveSchema();

    try (ArrowFileColumnReaderFactory factory = new ArrowFileColumnReaderFactory(arrowFile)) {
      factory.init(schema);
      ColumnReader intReader = factory.getColumnReader("intCol");
      ColumnReader stringReader = factory.getColumnReader("stringCol");

      // Wrong scalar type on a non-null doc (doc 0 is populated for both columns).
      assertThrows(IOException.class, () -> intReader.getString(0));
      assertThrows(IOException.class, () -> intReader.getLong(0));
      assertThrows(IOException.class, () -> stringReader.getInt(0));
      // Multi-value accessor on a single-value column (no ListVector to read).
      assertThrows(IOException.class, () -> intReader.getIntMV(0));
    }
  }

  /**
   * {@code CONFIG_EXTRACT_RAW_TIME_VALUES} must flow through the factory to each column reader,
   * mirroring the row-major {@code ArrowRecordExtractorConfig.EXTRACT_RAW_TIME_VALUES}: with it set, a
   * TIMESTAMP column surfaces the raw epoch {@code long}; without it (the default), it surfaces a
   * canonical {@link Timestamp}. Same fixture, two factories — proves the flag flips the behavior.
   */
  @Test
  public void testExtractRawTimeValuesSurfacesRawEpoch()
      throws IOException {
    long[] epochMillis = {0L, 1_000L, 1_718_265_600_000L};
    File arrowFile = writeTimestampFixture("timestamps.arrow", epochMillis);
    org.apache.pinot.spi.data.Schema schema = new org.apache.pinot.spi.data.Schema.SchemaBuilder()
        .setSchemaName("ts")
        .addSingleValueDimension("tsCol", FieldSpec.DataType.TIMESTAMP)
        .build();

    // Raw mode: surfaces the raw epoch long.
    try (ArrowFileColumnReaderFactory factory = new ArrowFileColumnReaderFactory(arrowFile)) {
      factory.init(schema, null,
          Map.of(ArrowFileColumnReaderFactory.CONFIG_EXTRACT_RAW_TIME_VALUES, "true"));
      ColumnReader reader = factory.getColumnReader("tsCol");
      assertNotNull(reader);
      for (int i = 0; i < epochMillis.length; i++) {
        Object value = reader.getValue(i);
        assertTrue(value instanceof Long, "raw mode should surface a Long epoch, got " + value);
        assertEquals(((Long) value).longValue(), epochMillis[i]);
      }
    }

    // Default mode: surfaces a java.sql.Timestamp carrying the same instant.
    try (ArrowFileColumnReaderFactory factory = new ArrowFileColumnReaderFactory(arrowFile)) {
      factory.init(schema);
      ColumnReader reader = factory.getColumnReader("tsCol");
      assertNotNull(reader);
      for (int i = 0; i < epochMillis.length; i++) {
        Object value = reader.getValue(i);
        assertTrue(value instanceof Timestamp, "default mode should surface a Timestamp, got " + value);
        assertEquals(((Timestamp) value).getTime(), epochMillis[i]);
      }
    }
  }

  @Test
  public void testInitSubsetOfColumns()
      throws IOException {
    File arrowFile = writePrimitiveFixture("subset.arrow");
    org.apache.pinot.spi.data.Schema schema = primitiveSchema();

    try (ArrowFileColumnReaderFactory factory = new ArrowFileColumnReaderFactory(arrowFile)) {
      factory.init(schema, java.util.Set.of("intCol", "stringCol"), Collections.emptyMap());
      Map<String, ColumnReader> readers = factory.getAllColumnReaders();
      assertEquals(readers.size(), 2);
      assertTrue(readers.containsKey("intCol"));
      assertTrue(readers.containsKey("stringCol"));
      assertNull(factory.getColumnReader("longCol"));
    }
  }

  @Test
  public void testMultiBatchConcatenation()
      throws IOException {
    File arrowFile = writeMultiBatchFixture("multi-batch.arrow", 3, 4);
    org.apache.pinot.spi.data.Schema schema = new org.apache.pinot.spi.data.Schema.SchemaBuilder()
        .setSchemaName("multiBatch")
        .addSingleValueDimension("seqCol", FieldSpec.DataType.INT)
        .build();

    try (ArrowFileColumnReaderFactory factory = new ArrowFileColumnReaderFactory(arrowFile)) {
      factory.init(schema);
      ColumnReader reader = factory.getColumnReader("seqCol");
      assertNotNull(reader);
      assertEquals(reader.getTotalDocs(), 12, "Expected 3 batches of 4 rows = 12 docs total");

      // Sequential traversal yields the global doc order.
      for (int i = 0; i < 12; i++) {
        assertEquals(reader.nextInt(), i);
      }
      assertFalse(reader.hasNext());

      // Random access across batch boundaries.
      reader.rewind();
      assertEquals(reader.getInt(0), 0);
      assertEquals(reader.getInt(3), 3);   // last row of batch 0
      assertEquals(reader.getInt(4), 4);   // first row of batch 1
      assertEquals(reader.getInt(7), 7);   // last row of batch 1
      assertEquals(reader.getInt(8), 8);   // first row of batch 2
      assertEquals(reader.getInt(11), 11); // last row overall
    }
  }

  @Test
  public void testGetAvailableColumnsIncludesAllSourceColumns()
      throws IOException {
    File arrowFile = writePrimitiveFixture("available.arrow");
    org.apache.pinot.spi.data.Schema schema = primitiveSchema();

    try (ArrowFileColumnReaderFactory factory = new ArrowFileColumnReaderFactory(arrowFile)) {
      factory.init(schema, java.util.Set.of("intCol"), Collections.emptyMap());
      // getAvailableColumns reflects the source schema, not the requested subset.
      assertTrue(factory.getAvailableColumns().containsAll(
          Arrays.asList("intCol", "longCol", "floatCol", "doubleCol", "stringCol", "bytesCol")));
    }
  }

  @Test
  public void testInterleavedReadersAcrossBatchesStayConsistent()
      throws Exception {
    // Two readers over a multi-batch file, accessed interleaved so they fight over the single shared
    // batch cursor. Each must still return its own correct value — the per-batch delegate is rebuilt
    // when the shared source has been moved by the other reader, rather than reading whichever batch
    // the other reader last loaded.
    File arrowFile = writeTwoColumnMultiBatchFixture("interleave.arrow", 3, 4); // 12 docs
    try (ArrowFileColumnReaderFactory factory = new ArrowFileColumnReaderFactory(arrowFile)) {
      factory.init(twoColumnSchema());
      ColumnReader a = factory.getColumnReader("aCol");
      ColumnReader b = factory.getColumnReader("bCol");
      assertNotNull(a);
      assertNotNull(b);
      // aCol[i] == i, bCol[i] == 1000 + i.
      assertEquals(a.getInt(9), 9);     // loads batch 2 for a
      assertEquals(b.getInt(0), 1000);  // moves the shared cursor back to batch 0
      assertEquals(a.getInt(9), 9);     // a must rebuild (source moved) -> batch 2, not batch 0
      assertEquals(b.getInt(11), 1011); // batch 2
      assertEquals(a.getInt(1), 1);     // batch 0
    }
  }

  @Test
  public void testEmptyFileRandomAccessThrowsBounds()
      throws Exception {
    File emptyFile = writeEmptyFixture("empty.arrow");
    try (ArrowFileColumnReaderFactory factory = new ArrowFileColumnReaderFactory(emptyFile)) {
      factory.init(oneIntColumnSchema("seqCol"));
      ColumnReader reader = factory.getColumnReader("seqCol");
      assertNotNull(reader);
      assertEquals(reader.getTotalDocs(), 0);
      assertFalse(reader.hasNext());
      // SPI contract: an out-of-range docId yields IndexOutOfBoundsException, not an opaque internal
      // error from seeking a non-existent batch.
      assertThrows(IndexOutOfBoundsException.class, () -> reader.getValue(0));
      assertThrows(IndexOutOfBoundsException.class, () -> reader.isNull(0));
    }
  }

  @Test
  public void testBatchBoundedConsumptionUnderTightAllocatorLimit()
      throws Exception {
    // The total data far exceeds the allocator limit, but each record batch fits. Because the reader
    // holds one batch at a time, consuming every value twice (the stats + index passes of the build)
    // succeeds under a limit a materialize-all reader would blow past — demonstrating the
    // file-size-independent, one-batch-resident memory profile.
    int batchCount = 8;
    int rowsPerBatch = 50_000;                 // ~1.6 MB total int data; ~200 KB per batch
    int total = batchCount * rowsPerBatch;
    File arrowFile = writeMultiBatchFixture("mem-bound.arrow", batchCount, rowsPerBatch);
    long oneMebibyte = 1L << 20;               // below the total footprint, well above one batch

    org.apache.pinot.spi.data.Schema schema = oneIntColumnSchema("seqCol");
    try (ArrowFileColumnReaderFactory factory = new ArrowFileColumnReaderFactory(arrowFile)) {
      factory.init(schema, null,
          Map.of(ArrowFileColumnReaderFactory.CONFIG_ALLOCATOR_LIMIT, Long.toString(oneMebibyte)));
      ColumnReader reader = factory.getColumnReader("seqCol");
      assertNotNull(reader);
      assertEquals(reader.getTotalDocs(), total);
      for (int pass = 0; pass < 2; pass++) {
        reader.rewind();
        int count = 0;
        while (reader.hasNext()) {
          assertEquals(reader.nextInt(), count);
          count++;
        }
        assertEquals(count, total, "pass " + pass + " did not read every doc");
      }
    }
  }

  // ===== Fixture builders =====

  private org.apache.pinot.spi.data.Schema oneIntColumnSchema(String columnName) {
    return new org.apache.pinot.spi.data.Schema.SchemaBuilder()
        .setSchemaName("oneInt")
        .addSingleValueDimension(columnName, FieldSpec.DataType.INT)
        .build();
  }

  private org.apache.pinot.spi.data.Schema twoColumnSchema() {
    return new org.apache.pinot.spi.data.Schema.SchemaBuilder()
        .setSchemaName("twoCol")
        .addSingleValueDimension("aCol", FieldSpec.DataType.INT)
        .addSingleValueDimension("bCol", FieldSpec.DataType.INT)
        .build();
  }

  private File writeEmptyFixture(String fileName)
      throws IOException {
    File out = _tempDir.resolve(fileName).toFile();
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Field seqField = new Field("seqCol", FieldType.nullable(new ArrowType.Int(32, true)), null);
      Schema schema = new Schema(Collections.singletonList(seqField));
      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
          FileOutputStream fos = new FileOutputStream(out);
          FileChannel channel = fos.getChannel();
          ArrowFileWriter writer = new ArrowFileWriter(root, null, channel)) {
        // Schema header + footer, no record batches.
        writer.start();
        writer.end();
      }
    }
    return out;
  }

  private File writeTwoColumnMultiBatchFixture(String fileName, int batchCount, int rowsPerBatch)
      throws IOException {
    File out = _tempDir.resolve(fileName).toFile();
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Field aField = new Field("aCol", FieldType.nullable(new ArrowType.Int(32, true)), null);
      Field bField = new Field("bCol", FieldType.nullable(new ArrowType.Int(32, true)), null);
      Schema schema = new Schema(Arrays.asList(aField, bField));
      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
          FileOutputStream fos = new FileOutputStream(out);
          FileChannel channel = fos.getChannel();
          ArrowFileWriter writer = new ArrowFileWriter(root, null, channel)) {
        writer.start();
        IntVector aVec = (IntVector) root.getVector("aCol");
        IntVector bVec = (IntVector) root.getVector("bCol");
        int global = 0;
        for (int b = 0; b < batchCount; b++) {
          aVec.allocateNew(rowsPerBatch);
          bVec.allocateNew(rowsPerBatch);
          for (int i = 0; i < rowsPerBatch; i++) {
            aVec.set(i, global);
            bVec.set(i, 1000 + global);
            global++;
          }
          aVec.setValueCount(rowsPerBatch);
          bVec.setValueCount(rowsPerBatch);
          root.setRowCount(rowsPerBatch);
          writer.writeBatch();
        }
        writer.end();
      }
    }
    return out;
  }

  private org.apache.pinot.spi.data.Schema primitiveSchema() {
    org.apache.pinot.spi.data.Schema schema = new org.apache.pinot.spi.data.Schema();
    schema.setSchemaName("primitive");
    schema.addField(new DimensionFieldSpec("intCol", FieldSpec.DataType.INT, true));
    schema.addField(new DimensionFieldSpec("longCol", FieldSpec.DataType.LONG, true));
    schema.addField(new DimensionFieldSpec("floatCol", FieldSpec.DataType.FLOAT, true));
    schema.addField(new DimensionFieldSpec("doubleCol", FieldSpec.DataType.DOUBLE, true));
    schema.addField(new DimensionFieldSpec("stringCol", FieldSpec.DataType.STRING, true));
    schema.addField(new DimensionFieldSpec("bytesCol", FieldSpec.DataType.BYTES, true));
    return schema;
  }

  private File writePrimitiveFixture(String fileName)
      throws IOException {
    File out = _tempDir.resolve(fileName).toFile();
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Field intField = new Field("intCol", FieldType.nullable(new ArrowType.Int(32, true)), null);
      Field longField = new Field("longCol", FieldType.nullable(new ArrowType.Int(64, true)), null);
      Field floatField =
          new Field("floatCol", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null);
      Field doubleField =
          new Field("doubleCol", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null);
      Field stringField = new Field("stringCol", FieldType.nullable(new ArrowType.Utf8()), null);
      Field bytesField = new Field("bytesCol", FieldType.nullable(new ArrowType.Binary()), null);
      Schema schema =
          new Schema(Arrays.asList(intField, longField, floatField, doubleField, stringField, bytesField));

      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        IntVector intVec = (IntVector) root.getVector("intCol");
        BigIntVector longVec = (BigIntVector) root.getVector("longCol");
        Float4Vector floatVec = (Float4Vector) root.getVector("floatCol");
        Float8Vector doubleVec = (Float8Vector) root.getVector("doubleCol");
        VarCharVector stringVec = (VarCharVector) root.getVector("stringCol");
        VarBinaryVector bytesVec = (VarBinaryVector) root.getVector("bytesCol");

        intVec.allocateNew(ROWS);
        longVec.allocateNew(ROWS);
        floatVec.allocateNew(ROWS);
        doubleVec.allocateNew(ROWS);
        stringVec.allocateNew(ROWS * 8, ROWS);
        bytesVec.allocateNew(ROWS * 8, ROWS);

        for (int i = 0; i < ROWS; i++) {
          if (i == 3) {
            intVec.setNull(i);
            longVec.setNull(i);
            stringVec.setNull(i);
          } else {
            intVec.set(i, i * 10);
            longVec.set(i, (long) i * 100);
            stringVec.set(i, ("s_" + i).getBytes(StandardCharsets.UTF_8));
          }
          floatVec.set(i, i + 0.5f);
          doubleVec.set(i, i + 0.25);
          bytesVec.set(i, ("b_" + i).getBytes(StandardCharsets.UTF_8));
        }
        intVec.setValueCount(ROWS);
        longVec.setValueCount(ROWS);
        floatVec.setValueCount(ROWS);
        doubleVec.setValueCount(ROWS);
        stringVec.setValueCount(ROWS);
        bytesVec.setValueCount(ROWS);
        root.setRowCount(ROWS);

        writeIpc(out, root);
      }
    }
    return out;
  }

  private File writeMultiBatchFixture(String fileName, int batchCount, int rowsPerBatch)
      throws IOException {
    File out = _tempDir.resolve(fileName).toFile();
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Field seqField = new Field("seqCol", FieldType.nullable(new ArrowType.Int(32, true)), null);
      Schema schema = new Schema(Collections.singletonList(seqField));

      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
          FileOutputStream fos = new FileOutputStream(out);
          FileChannel channel = fos.getChannel();
          ArrowFileWriter writer = new ArrowFileWriter(root, null, channel)) {
        writer.start();
        IntVector seqVec = (IntVector) root.getVector("seqCol");

        int globalSeq = 0;
        for (int b = 0; b < batchCount; b++) {
          seqVec.allocateNew(rowsPerBatch);
          for (int i = 0; i < rowsPerBatch; i++) {
            seqVec.set(i, globalSeq++);
          }
          seqVec.setValueCount(rowsPerBatch);
          root.setRowCount(rowsPerBatch);
          writer.writeBatch();
        }
        writer.end();
      }
    }
    return out;
  }

  private File writeMultiValueIntFixture(String fileName)
      throws IOException {
    File out = _tempDir.resolve(fileName).toFile();
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      Field elementField =
          new Field("element", FieldType.nullable(new ArrowType.Int(32, true)), null);
      Field listField = new Field("intArr", FieldType.nullable(ArrowType.List.INSTANCE),
          Collections.singletonList(elementField));
      Schema schema = new Schema(Collections.singletonList(listField));

      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        ListVector listVec = (ListVector) root.getVector("intArr");
        listVec.allocateNew();
        UnionListWriter writer = listVec.getWriter();

        // Canonical Arrow list-writer pattern: setPosition(row) -> startList -> writes -> endList.
        writer.setPosition(0);
        writer.startList();
        writer.integer().writeInt(1);
        writer.integer().writeInt(2);
        writer.integer().writeInt(3);
        writer.endList();

        writer.setPosition(1);
        writer.startList();
        writer.integer().writeInt(4);
        writer.writeNull();
        writer.endList();

        writer.setPosition(2);
        writer.startList();
        writer.endList();

        listVec.setValueCount(3);
        root.setRowCount(3);

        writeIpc(out, root);
      }
    }
    return out;
  }

  private File writeTimestampFixture(String fileName, long[] epochMillis)
      throws IOException {
    File out = _tempDir.resolve(fileName).toFile();
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
      // No-timezone Timestamp vector in millisecond units (matches Pinot's TIMESTAMP storage unit).
      Field tsField =
          new Field("tsCol", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)), null);
      Schema schema = new Schema(Collections.singletonList(tsField));

      try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
        TimeStampMilliVector tsVec = (TimeStampMilliVector) root.getVector("tsCol");
        tsVec.allocateNew(epochMillis.length);
        for (int i = 0; i < epochMillis.length; i++) {
          tsVec.set(i, epochMillis[i]);
        }
        tsVec.setValueCount(epochMillis.length);
        root.setRowCount(epochMillis.length);

        writeIpc(out, root);
      }
    }
    return out;
  }

  private void writeIpc(File out, VectorSchemaRoot root)
      throws IOException {
    try (FileOutputStream fos = new FileOutputStream(out);
        FileChannel channel = fos.getChannel();
        ArrowFileWriter writer = new ArrowFileWriter(root, null, channel)) {
      writer.start();
      writer.writeBatch();
      writer.end();
    }
  }
}
