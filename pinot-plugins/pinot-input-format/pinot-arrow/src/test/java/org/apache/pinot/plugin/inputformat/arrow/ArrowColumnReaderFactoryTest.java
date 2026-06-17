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
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.readers.ColumnReader;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/**
 * Verifies {@link ArrowColumnReaderFactory} can consume a caller-managed {@link ArrowStreamReader}
 * backed by a {@link ByteArrayInputStream}, without disk I/O, and that the caller-owned
 * {@link RootAllocator} remains usable after the factory is closed.
 */
public class ArrowColumnReaderFactoryTest {

  private static final int ROW_COUNT = 32;

  @Test
  public void testReadsValuesFromInMemoryReader()
      throws Exception {
    // Caller owns this allocator. Verify the factory does not close it.
    try (RootAllocator callerAllocator = new RootAllocator(Long.MAX_VALUE)) {
      byte[] ipcBytes = writeArrowStreamFixture(callerAllocator);

      try (ArrowStreamReader streamReader = new ArrowStreamReader(
          Channels.newChannel(new ByteArrayInputStream(ipcBytes)), callerAllocator);
          ArrowColumnReaderFactory factory =
              new ArrowColumnReaderFactory(streamReader, callerAllocator)) {

        factory.init(newSchema());

        try (ColumnReader intCol = factory.getColumnReader("intCol");
            ColumnReader longCol = factory.getColumnReader("longCol");
            ColumnReader stringCol = factory.getColumnReader("stringCol")) {

          assertNotNull(intCol);
          assertNotNull(longCol);
          assertNotNull(stringCol);
          assertEquals(intCol.getTotalDocs(), ROW_COUNT);
          assertEquals(longCol.getTotalDocs(), ROW_COUNT);
          assertEquals(stringCol.getTotalDocs(), ROW_COUNT);

          for (int i = 0; i < ROW_COUNT; i++) {
            assertEquals(intCol.getInt(i), i);
            assertEquals(longCol.getLong(i), (long) i * 1000);
            assertEquals(stringCol.getString(i), "row_" + i);
          }
        }
      }

      // Caller's allocator should still be usable after the factory closes — proves the factory
      // did not close it. Allocate-and-release a small buffer to confirm.
      try (IntVector probe = new IntVector("probe", callerAllocator)) {
        probe.allocateNew(1);
        probe.set(0, 42);
        probe.setValueCount(1);
        assertEquals(probe.get(0), 42);
      }
    }
  }

  @Test
  public void testGetAllColumnReadersReflectsSourceSchema()
      throws Exception {
    try (RootAllocator callerAllocator = new RootAllocator(Long.MAX_VALUE)) {
      byte[] ipcBytes = writeArrowStreamFixture(callerAllocator);

      try (ArrowStreamReader streamReader = new ArrowStreamReader(
          Channels.newChannel(new ByteArrayInputStream(ipcBytes)), callerAllocator);
          ArrowColumnReaderFactory factory =
              new ArrowColumnReaderFactory(streamReader, callerAllocator)) {

        factory.init(newSchema());

        assertEquals(factory.getAvailableColumns().size(), 3);
        assertTrue(factory.getAvailableColumns().contains("intCol"));
        assertTrue(factory.getAvailableColumns().contains("longCol"));
        assertTrue(factory.getAvailableColumns().contains("stringCol"));
        assertEquals(factory.getAllColumnReaders().size(), 3);
      }
    }
  }

  @Test
  public void testColumnSubsetFiltering()
      throws Exception {
    try (RootAllocator callerAllocator = new RootAllocator(Long.MAX_VALUE)) {
      byte[] ipcBytes = writeArrowStreamFixture(callerAllocator);

      try (ArrowStreamReader streamReader = new ArrowStreamReader(
          Channels.newChannel(new ByteArrayInputStream(ipcBytes)), callerAllocator);
          ArrowColumnReaderFactory factory =
              new ArrowColumnReaderFactory(streamReader, callerAllocator)) {

        factory.init(newSchema(), Collections.singleton("intCol"));

        // Only the requested column has a reader; the others fall back to null per the SPI.
        assertNotNull(factory.getColumnReader("intCol"));
        assertEquals(factory.getAllColumnReaders().size(), 1);
        assertFalse(factory.getAllColumnReaders().containsKey("longCol"));
        assertFalse(factory.getAllColumnReaders().containsKey("stringCol"));
      }
    }
  }

  /**
   * A second {@code init()} on the same factory must release the first init's off-heap accumulator
   * vectors rather than orphaning them. Without the re-init guard the first set leaks, and the
   * caller-owned allocator throws {@code IllegalStateException} on its own {@code close()} because the
   * outstanding buffers are still allocated — so reaching the end of the caller-allocator
   * try-with-resources cleanly is the leak detector.
   */
  @Test
  public void testReInitReleasesPriorAccumulators()
      throws Exception {
    try (RootAllocator callerAllocator = new RootAllocator(Long.MAX_VALUE)) {
      byte[] ipcBytes = writeArrowStreamFixture(callerAllocator);

      try (ArrowStreamReader streamReader = new ArrowStreamReader(
          Channels.newChannel(new ByteArrayInputStream(ipcBytes)), callerAllocator);
          ArrowColumnReaderFactory factory =
              new ArrowColumnReaderFactory(streamReader, callerAllocator)) {

        factory.init(newSchema());
        // Second init() over the same (now-drained) reader: it allocates a fresh accumulator set and
        // must release the first set instead of overwriting the only reference to it.
        factory.init(newSchema());
        assertTrue(factory.getAvailableColumns().contains("intCol"));
      }

      // factory.close() above released the latest accumulator set; if the first set had leaked, this
      // allocator close (and the probe) would trip on the outstanding buffers.
      try (IntVector probe = new IntVector("probe", callerAllocator)) {
        probe.allocateNew(1);
        probe.set(0, 1);
        probe.setValueCount(1);
        assertEquals(probe.get(0), 1);
      }
    }
  }

  /**
   * After the reader is drained, the sequential accessors ({@code next}, the typed {@code nextXxx},
   * {@code isNextNull}, {@code skipNext}) must throw {@link IllegalStateException} rather than read out
   * of bounds — matching the {@code DefaultValueColumnReader} / {@code PinotSegmentColumnReaderImpl}
   * SPI contract ("No more values available").
   */
  @Test
  public void testSequentialAccessorsThrowPastEnd()
      throws Exception {
    try (RootAllocator callerAllocator = new RootAllocator(Long.MAX_VALUE)) {
      byte[] ipcBytes = writeArrowStreamFixture(callerAllocator);

      try (ArrowStreamReader streamReader = new ArrowStreamReader(
          Channels.newChannel(new ByteArrayInputStream(ipcBytes)), callerAllocator);
          ArrowColumnReaderFactory factory =
              new ArrowColumnReaderFactory(streamReader, callerAllocator)) {

        factory.init(newSchema());

        try (ColumnReader intCol = factory.getColumnReader("intCol")) {
          while (intCol.hasNext()) {
            intCol.next();
          }
          assertFalse(intCol.hasNext());
          assertThrows(IllegalStateException.class, intCol::next);
          assertThrows(IllegalStateException.class, intCol::nextInt);
          assertThrows(IllegalStateException.class, intCol::isNextNull);
          assertThrows(IllegalStateException.class, intCol::skipNext);
        }
      }
    }
  }

  /**
   * Dictionary encoding is the standard Arrow representation for low-cardinality strings. The
   * column-major path must DECODE it — surfacing the logical string values and the STRING type, not
   * the underlying Int32 dictionary indices. Verifies the decode end-to-end through the factory and
   * the resulting {@link ColumnReader} (both the typed {@code getString} and generic {@code getValue}
   * accessors), and that the caller's allocator is left clean afterward.
   */
  @Test
  public void testDecodesDictionaryEncodedStringColumn()
      throws Exception {
    String[] dictValues = {"red", "green", "blue"};
    // Rows cycle through the dictionary so the encoded indices (0,1,2,...) differ from the decoded
    // strings — a reader that forgot to decode would return the indices and fail getString().
    String[] rows = {"red", "green", "blue", "red", "green", "blue", "blue", "red"};

    try (RootAllocator callerAllocator = new RootAllocator(Long.MAX_VALUE)) {
      byte[] ipcBytes = writeDictionaryEncodedStringStream(callerAllocator, dictValues, rows);

      try (ArrowStreamReader streamReader = new ArrowStreamReader(
          Channels.newChannel(new ByteArrayInputStream(ipcBytes)), callerAllocator);
          ArrowColumnReaderFactory factory =
              new ArrowColumnReaderFactory(streamReader, callerAllocator)) {

        factory.init(dictColumnSchema());

        try (ColumnReader colorCol = factory.getColumnReader("color")) {
          assertNotNull(colorCol);
          assertEquals(colorCol.getTotalDocs(), rows.length);
          // Must report the DECODED logical type (STRING), not the Int32 index type.
          assertTrue(colorCol.isString());
          for (int i = 0; i < rows.length; i++) {
            assertEquals(colorCol.getString(i), rows[i], "decoded getString mismatch at doc " + i);
            assertEquals(colorCol.getValue(i), rows[i], "decoded getValue mismatch at doc " + i);
          }
        }
      }

      // Factory closed; the decoded-batch copies must have been released. Confirm the caller's
      // allocator is still clean and usable.
      try (IntVector probe = new IntVector("probe", callerAllocator)) {
        probe.allocateNew(1);
        probe.set(0, 7);
        probe.setValueCount(1);
        assertEquals(probe.get(0), 7);
      }
    }
  }

  /**
   * Writes a single-column ("color") Arrow IPC stream where the column is dictionary-encoded against
   * {@code dictValues}, following the standard Arrow {@code encode → write with DictionaryProvider}
   * pattern. The dictionary vector is owned here and closed; the encoded vector is owned by the
   * VectorSchemaRoot.
   */
  private byte[] writeDictionaryEncodedStringStream(RootAllocator allocator, String[] dictValues, String[] rows)
      throws IOException {
    DictionaryEncoding encoding = new DictionaryEncoding(1L, false, new ArrowType.Int(32, true));

    VarCharVector dictVec = new VarCharVector("color-dict", allocator);
    dictVec.allocateNew();
    for (int i = 0; i < dictValues.length; i++) {
      dictVec.setSafe(i, dictValues[i].getBytes(StandardCharsets.UTF_8));
    }
    dictVec.setValueCount(dictValues.length);
    Dictionary dictionary = new Dictionary(dictVec, encoding);

    VarCharVector unencoded = new VarCharVector("color", allocator);
    unencoded.allocateNew();
    for (int i = 0; i < rows.length; i++) {
      unencoded.setSafe(i, rows[i].getBytes(StandardCharsets.UTF_8));
    }
    unencoded.setValueCount(rows.length);

    DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
    provider.put(dictionary);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (FieldVector encodedVec = (FieldVector) DictionaryEncoder.encode(unencoded, dictionary)) {
      unencoded.close();
      try (VectorSchemaRoot root =
          new VectorSchemaRoot(List.of(encodedVec.getField()), List.of(encodedVec), rows.length);
          ArrowStreamWriter writer = new ArrowStreamWriter(root, provider, Channels.newChannel(out))) {
        writer.start();
        writer.writeBatch();
        writer.end();
      }
    } finally {
      dictVec.close();
    }
    return out.toByteArray();
  }

  private org.apache.pinot.spi.data.Schema dictColumnSchema() {
    org.apache.pinot.spi.data.Schema schema = new org.apache.pinot.spi.data.Schema();
    schema.setSchemaName("dictEncodedArrowTest");
    schema.addField(new DimensionFieldSpec("color", DataType.STRING, true));
    return schema;
  }

  private byte[] writeArrowStreamFixture(RootAllocator allocator)
      throws IOException {
    Field intField = new Field("intCol", FieldType.nullable(new ArrowType.Int(32, true)), null);
    Field longField = new Field("longCol", FieldType.nullable(new ArrowType.Int(64, true)), null);
    Field stringField = new Field("stringCol", FieldType.nullable(new ArrowType.Utf8()), null);
    Schema arrowSchema = new Schema(Arrays.asList(intField, longField, stringField));

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator);
        ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(out))) {
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

      writer.start();
      writer.writeBatch();
      writer.end();
    }
    return out.toByteArray();
  }

  private org.apache.pinot.spi.data.Schema newSchema() {
    org.apache.pinot.spi.data.Schema schema = new org.apache.pinot.spi.data.Schema();
    schema.setSchemaName("inMemoryArrowTest");
    schema.addField(new DimensionFieldSpec("intCol", DataType.INT, true));
    schema.addField(new DimensionFieldSpec("longCol", DataType.LONG, true));
    schema.addField(new DimensionFieldSpec("stringCol", DataType.STRING, true));
    return schema;
  }
}
