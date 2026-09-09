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
package org.apache.pinot.common.datablock;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.segment.spi.memory.CompoundDataBuffer;
import org.apache.pinot.segment.spi.memory.DataBuffer;
import org.apache.pinot.segment.spi.memory.PagedPinotOutputStream;
import org.apache.pinot.segment.spi.memory.PinotByteBuffer;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/** Tests IPC framing, scalar fidelity and independent ownership using real, bounded Arrow allocators. */
public class ArrowDataBlockSerdeTest {
  private static final int VERSION_TYPE = (DataBlock.Type.ARROW.ordinal() << DataBlockUtils.VERSION_TYPE_SHIFT)
      | DataBlockSerde.Version.ARROW_IPC.getVersion();
  private static final DataSchema INT_SCHEMA =
      new DataSchema(new String[]{"i"}, new ColumnDataType[]{ColumnDataType.INT});
  private static final DataSchema SCALAR_SCHEMA = new DataSchema(
      new String[]{"i", "l", "f", "d", "bool", "boolInt", "ts", "bytes", "s", "json", "decimal", "dict", "dictJson"},
      new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.FLOAT, ColumnDataType.DOUBLE,
          ColumnDataType.BOOLEAN, ColumnDataType.BOOLEAN, ColumnDataType.TIMESTAMP, ColumnDataType.BYTES,
          ColumnDataType.STRING, ColumnDataType.JSON, ColumnDataType.BIG_DECIMAL, ColumnDataType.STRING,
          ColumnDataType.JSON});
  private static final DictionaryEncoding STRING_ENCODING =
      new DictionaryEncoding(37, false, new ArrowType.Int(32, true));
  private static final DictionaryEncoding JSON_ENCODING =
      new DictionaryEncoding(9012, true, new ArrowType.Int(32, true));
  private RootAllocator _allocator;

  @BeforeMethod
  public void setUp() {
    _allocator = new RootAllocator(64L * 1024 * 1024);
  }

  @AfterMethod
  public void tearDown() {
    try {
      assertEquals(_allocator.getAllocatedMemory(), 0L, "Every success and failure path must release native memory");
    } finally {
      _allocator.close();
    }
  }

  @Test
  public void testScalarAliasesNullsAndDictionaries()
      throws IOException {
    try (ArrowDataBlock source = scalarBlock(_allocator);
        ArrowDataBlock decoded = (ArrowDataBlock) DataBlockUtils.deserialize(source.serialize(), _allocator)) {
      assertScalars(decoded);
      assertEquals(decoded.getDataSchema(), SCALAR_SCHEMA);
      assertEquals(decoded.getSchema(), source.getSchema());
      assertEquals(decoded.getExceptions(), source.getExceptions());
      assertNotSame(decoded.getRoot(), source.getRoot());
      assertNotSame(decoded.getDictionaryProvider(), source.getDictionaryProvider());
      assertEquals(decoded.getDictionaryProvider().getDictionaryIds(),
          source.getDictionaryProvider().getDictionaryIds());
      assertEquals(decoded.getDictionaryProvider().lookup(37).getEncoding(), STRING_ENCODING);
      assertEquals(decoded.getDictionaryProvider().lookup(9012).getEncoding(), JSON_ENCODING);
    }
  }

  @Test
  public void testEncodingDoesNotChangeSourceOrReferences()
      throws IOException {
    try (ArrowDataBlock source = scalarBlock(_allocator)) {
      List<ArrowBuf> buffers = allBuffers(source);
      List<Integer> references = new ArrayList<>();
      for (ArrowBuf buffer : buffers) {
        references.add(buffer.refCnt());
      }
      long allocated = _allocator.getAllocatedMemory();
      byte[] first = bytes(source.serialize());
      assertEquals(ByteBuffer.wrap(first).getInt(), VERSION_TYPE);
      assertEquals(ByteBuffer.wrap(first).getLong(Integer.BYTES), first.length);
      assertEquals(bytes(DataBlockUtils.serialize(source)), first);
      assertEquals(_allocator.getAllocatedMemory(), allocated);
      for (int i = 0; i < buffers.size(); i++) {
        assertEquals(buffers.get(i).refCnt(), references.get(i).intValue());
      }
      assertScalars(source);
    }
  }

  @Test
  public void testEncodingDoesNotAllocateNativeMemory()
      throws IOException {
    AtomicBoolean rejectAllocations = new AtomicBoolean();
    AllocationListener listener = new AllocationListener() {
      @Override
      public void onPreAllocation(long size) {
        if (rejectAllocations.get()) {
          throw new OutOfMemoryException("Encoding must not allocate native memory");
        }
      }
    };
    try (BufferAllocator allocator = _allocator.newChildAllocator("encode", listener, 0, 1024 * 1024);
        ArrowDataBlock source = scalarBlock(allocator)) {
      rejectAllocations.set(true);
      assertTrue(bytes(source.serialize()).length > 0);
      assertScalars(source);
    }
  }

  @Test
  public void testPagedOutputAndFragmentedInput()
      throws IOException {
    try (ArrowDataBlock source = intBlock(_allocator, 12000)) {
      List<ByteBuffer> serialized = source.serialize();
      assertTrue(serialized.size() > 1, "Exercise the length backpatch after advancing beyond the first page");
      byte[] wire = bytes(serialized);
      List<ByteBuffer> chunks = new ArrayList<>();
      int offset = 0;
      while (offset < wire.length) {
        int count = Math.min(1 + offset % 43, wire.length - offset);
        ByteBuffer chunk = ByteBuffer.allocate(count + 5).position(5);
        chunk.put(wire, offset, count).flip().position(5);
        chunks.add(chunk);
        offset += count;
      }
      DataBuffer[] slices = chunks.stream().map(PinotByteBuffer::slice).toArray(DataBuffer[]::new);
      DataBuffer fragmented = new CompoundDataBuffer(slices, ByteOrder.BIG_ENDIAN, false);
      try (ArrowDataBlock decoded =
          (ArrowDataBlock) DataBlockUtils.deserialize(fragmented, 0, null, _allocator);
          ArrowDataBlock decodedList = (ArrowDataBlock) DataBlockUtils.deserialize(chunks, _allocator)) {
        assertEquals(decoded.getNumberOfRows(), 12000);
        assertEquals(decodedList.getInt(11999, 0), 11999 * 13);
        for (int row = 0; row < 12000; row++) {
          assertEquals(decoded.getInt(row, 0), row * 13);
        }
      }
      for (ByteBuffer chunk : chunks) {
        assertEquals(chunk.position(), 5, "Decoding must not consume caller buffers");
      }
    }
  }

  @Test
  public void testPrefixedOffsetAndSingleBufferPosition()
      throws IOException {
    try (ArrowDataBlock source = intBlock(_allocator, 3)) {
      byte[] wire = bytes(source.serialize());
      ByteBuffer prefixed = ByteBuffer.allocate(7 + 2 * wire.length);
      prefixed.position(7).put(wire).put(wire);
      AtomicLong end = new AtomicLong(-1);
      DataBuffer input = PinotByteBuffer.wrap(prefixed.order(ByteOrder.LITTLE_ENDIAN));
      try (ArrowDataBlock first = (ArrowDataBlock) DataBlockUtils.deserialize(input, 7, end::set, _allocator)) {
        assertEquals(first.getInt(2, 0), 26);
        assertEquals(end.get(), 7L + wire.length);
      }
      try (ArrowDataBlock second =
          (ArrowDataBlock) DataBlockUtils.deserialize(input, end.get(), end::set, _allocator)) {
        assertEquals(second.getInt(1, 0), 13);
        assertEquals(end.get(), prefixed.capacity());
      }
      prefixed.position(7).limit(7 + wire.length);
      try (ArrowDataBlock decoded = (ArrowDataBlock) DataBlockUtils.deserialize(List.of(prefixed), _allocator)) {
        assertEquals(decoded.getNumberOfRows(), 3);
        assertEquals(prefixed.position(), 7);
      }
    }
  }

  @Test
  public void testLegacyVersionAndOffsetsRemainSupported()
      throws IOException {
    MetadataBlock source = new MetadataBlock(List.of(PinotByteBuffer.wrap(new byte[]{1, 2, 3})));
    byte[] wire = bytes(DataBlockUtils.serialize(source));
    assertEquals(DataBlockUtils.getVersion(ByteBuffer.wrap(wire).getInt()), 2);
    ByteBuffer prefixed = ByteBuffer.allocate(11 + wire.length).position(11).put(wire).flip().position(11);
    DataBlock decoded = DataBlockUtils.readFrom(prefixed);
    assertEquals(prefixed.position(), prefixed.capacity());
    assertEquals(decoded.getDataBlockType(), DataBlock.Type.METADATA);
    assertTrue(DataBuffer.sameContent(decoded.getStatsByStage().get(0), source.getStatsByStage().get(0)));
    ByteBuffer legacyV1 = ByteBuffer.wrap(wire);
    legacyV1.putInt(0, (DataBlock.Type.METADATA.ordinal() << DataBlockUtils.VERSION_TYPE_SHIFT) | 1);
    assertEquals(DataBlockUtils.deserialize(List.of(legacyV1), _allocator).getDataBlockType(), DataBlock.Type.METADATA);
  }

  @Test
  public void testLegacyRowsAtNonzeroOffset()
      throws IOException {
    ByteBuffer fixed = ByteBuffer.allocate(16).putInt(0, 7).putInt(4, 42);
    RowDataBlock source = new RowDataBlock(2, INT_SCHEMA, new String[0], PinotByteBuffer.wrap(fixed),
        PinotByteBuffer.EMPTY);
    byte[] wire = bytes(source.serialize());
    ByteBuffer input = ByteBuffer.allocate(5 + wire.length).position(5).put(wire);
    AtomicLong end = new AtomicLong(-1);
    DataBlock decoded = DataBlockUtils.deserialize(PinotByteBuffer.wrap(input), 5, end::set, _allocator);
    assertEquals(decoded.getDataBlockType(), DataBlock.Type.ROW);
    assertEquals(decoded.getDataSchema(), INT_SCHEMA);
    assertEquals(decoded.getInt(0, 0), 7);
    assertEquals(decoded.getInt(1, 0), 42);
    assertEquals(end.get(), input.capacity());
  }

  @Test
  public void testNoAllocatorAndIncompatibleVersionsFailUsefully()
      throws IOException {
    try (ArrowDataBlock source = intBlock(_allocator, 3)) {
      List<ByteBuffer> wire = source.serialize();
      IOException error = expectThrows(IOException.class, () -> DataBlockUtils.deserialize(wire));
      assertTrue(error.getMessage().contains("BufferAllocator"));
      expectThrows(IOException.class, () -> DataBlockUtils.serialize(DataBlockSerde.Version.V1_V2, source));
      expectThrows(IOException.class,
          () -> DataBlockUtils.serialize(DataBlockSerde.Version.ARROW_IPC, MetadataBlock.newEos()));
      expectThrows(IOException.class,
          () -> new ArrowDataBlockSerde().deserialize(PinotByteBuffer.wrap(bytes(wire)), 0, DataBlock.Type.ARROW));
    }
  }

  @DataProvider
  public Object[][] emptyShapes() {
    return new Object[][]{{0, 0}, {0, 3}, {1, 0}};
  }

  @Test(dataProvider = "emptyShapes")
  public void testZeroRowsAndColumns(int columns, int rows)
      throws IOException {
    DataSchema schema = columns == 0 ? new DataSchema(new String[0], new ColumnDataType[0]) : INT_SCHEMA;
    VectorSchemaRoot root = columns == 0 ? new VectorSchemaRoot(new Schema(List.of()), List.of(), rows)
        : VectorSchemaRoot.create(new Schema(List.of(field("i", new ArrowType.Int(32, true)))), _allocator);
    try (ArrowDataBlock source = new ArrowDataBlock(root, schema);
        ArrowDataBlock decoded = (ArrowDataBlock) DataBlockUtils.deserialize(source.serialize(), _allocator);
        ArrowDataBlock retained = source.retainTo(_allocator)) {
      assertEquals(decoded.getNumberOfRows(), rows);
      assertEquals(decoded.getNumberOfColumns(), columns);
      assertEquals(decoded.getDataSchema(), schema);
      assertEquals(retained.getNumberOfRows(), rows);
      assertEquals(retained.getNumberOfColumns(), columns);
    }
  }

  @Test
  public void testZeroRowsWithDictionaries()
      throws IOException {
    try (ArrowDataBlock source = scalarBlock(_allocator)) {
      source.getRoot().setRowCount(0);
      try (ArrowDataBlock decoded = (ArrowDataBlock) DataBlockUtils.deserialize(source.serialize(), _allocator)) {
        assertEquals(decoded.getNumberOfRows(), 0);
        assertEquals(decoded.getDataSchema(), SCALAR_SCHEMA);
        assertEquals(decoded.getDictionaryProvider().lookup(37).getVector().getValueCount(), 2);
        assertEquals(decoded.getDictionaryProvider().lookup(9012).getEncoding(), JSON_ENCODING);
      }
    }
  }

  @DataProvider
  public Object[][] closeOrder() {
    return new Object[][]{{true}, {false}};
  }

  @Test(dataProvider = "closeOrder")
  public void testRetainedStageCanOutliveEitherOwner(boolean sourceFirst) {
    BufferAllocator producer = _allocator.newChildAllocator("producer", 0, 32L * 1024 * 1024);
    BufferAllocator consumer = _allocator.newChildAllocator("consumer", 0, 32L * 1024 * 1024);
    ArrowDataBlock source = null;
    ArrowDataBlock retained = null;
    try {
      source = scalarBlock(producer);
      retained = source.retainTo(consumer);
      assertScalars(source);
      assertScalars(retained);
      assertEquals(retained.getSchema(), source.getSchema());
      assertEquals(retained.getExceptions(), source.getExceptions());
      assertTrue(consumer.getAllocatedMemory() > 0, "Retained buffers must be charged to the receiving stage");
      for (FieldVector vector : retained.getRoot().getFieldVectors()) {
        assertEquals(vector.getAllocator().getParentAllocator(), consumer);
      }
      if (sourceFirst) {
        source.close();
        source = null;
        producer.close();
        producer = null;
        assertScalars(retained);
      } else {
        retained.close();
        retained = null;
        consumer.close();
        consumer = null;
        assertScalars(source);
      }
    } finally {
      if (source != null) {
        source.close();
      }
      if (retained != null) {
        retained.close();
      }
      if (producer != null) {
        producer.close();
      }
      if (consumer != null) {
        consumer.close();
      }
    }
  }

  @Test
  public void testDecodedOutputOutlivesReaderInputAndProducer()
      throws IOException {
    ArrowDataBlock decoded;
    try (BufferAllocator producer = _allocator.newChildAllocator("producer", 0, 32L * 1024 * 1024);
        ArrowDataBlock source = scalarBlock(producer)) {
      byte[] wire = bytes(source.serialize());
      decoded = (ArrowDataBlock) DataBlockUtils.deserialize(List.of(ByteBuffer.wrap(wire)), _allocator);
      Arrays.fill(wire, (byte) 0);
    }
    try (ArrowDataBlock output = decoded) {
      assertScalars(output);
      assertTrue(_allocator.getAllocatedMemory() > 0);
    }
  }

  @Test
  public void testRetainLimitFailureReleasesPartialTransfers() {
    try (ArrowDataBlock source = scalarBlock(_allocator)) {
      long allocated = _allocator.getAllocatedMemory();
      long firstVectorAllocation = source.getRoot().getVector(0).getDataBuffer().getReferenceManager().getSize();
      try (BufferAllocator limited = _allocator.newChildAllocator("limited", 0, firstVectorAllocation)) {
        expectThrows(OutOfMemoryException.class, () -> source.retainTo(limited));
        assertEquals(limited.getAllocatedMemory(), 0L);
        assertEquals(_allocator.getAllocatedMemory(), allocated);
        assertScalars(source);
      }
    }
  }

  @DataProvider
  public Object[][] retentionLimits() {
    return new Object[][]{{0L, false}, {1L, false}, {64L, false}, {0L, true}, {1L, true}, {64L, true}};
  }

  @Test(dataProvider = "retentionLimits")
  public void testDictionaryRetentionLimitsBeforeAndAfterBroadcast(long limit, boolean previouslyRetained) {
    try (BufferAllocator producer = _allocator.newChildAllocator("producer", 0, 1024 * 1024);
        BufferAllocator firstReceiver = _allocator.newChildAllocator("first", 0, 1024 * 1024);
        BufferAllocator limited = _allocator.newChildAllocator("limited", 0, limit);
        ArrowDataBlock source = dictionaryPayloadBlock(producer);
        ArrowDataBlock first = previouslyRetained ? source.retainTo(firstReceiver) : null) {
      long allocated = _allocator.getAllocatedMemory();
      expectThrows(OutOfMemoryException.class, () -> {
        try (ArrowDataBlock unexpected = source.retainTo(limited)) {
          assertEquals(unexpected.getString(0, 0), "x".repeat(8192));
        }
      });
      assertEquals(limited.getAllocatedMemory(), 0L);
      assertTrue(limited.getChildAllocators().isEmpty());
      assertEquals(_allocator.getAllocatedMemory(), allocated);
      assertEquals(source.getString(0, 0), "x".repeat(8192));
      assertNull(source.getString(1, 0));
      if (first != null) {
        assertEquals(first.getString(0, 0), source.getString(0, 0));
      }
    }
  }

  @Test
  public void testBroadcastRetentionKeepsReceiverBudgetReserved() {
    BufferAllocator producer = _allocator.newChildAllocator("producer", 0, 1024 * 1024);
    BufferAllocator firstReceiver = _allocator.newChildAllocator("first", 0, 1024 * 1024);
    BufferAllocator secondReceiver = null;
    ArrowDataBlock source = null;
    ArrowDataBlock first = null;
    ArrowDataBlock second = null;
    try {
      source = dictionaryPayloadBlock(producer);
      long footprint = producer.getAllocatedMemory();
      secondReceiver = _allocator.newChildAllocator("second", 0, footprint + 64);
      first = source.retainTo(firstReceiver);
      second = source.retainTo(secondReceiver);
      assertEquals(secondReceiver.getAllocatedMemory(), footprint);
      BufferAllocator target = secondReceiver;
      expectThrows(OutOfMemoryException.class, () -> {
        try (ArrowBuf ignored = target.buffer(128)) {
          assertEquals(ignored.capacity(), 128L);
        }
      });
      try (ArrowBuf slack = secondReceiver.buffer(64)) {
        source.close();
        source = null;
        producer.close();
        producer = null;
        first.close();
        first = null;
        firstReceiver.close();
        firstReceiver = null;
        assertEquals(second.getString(0, 0), "x".repeat(8192));
        assertEquals(secondReceiver.getAllocatedMemory(), footprint + slack.capacity());
        assertFalse(secondReceiver.isOverLimit());
      }
      second.close();
      second = null;
      assertEquals(secondReceiver.getAllocatedMemory(), 0L);
      assertTrue(secondReceiver.getChildAllocators().isEmpty());
    } finally {
      if (source != null) {
        source.close();
      }
      if (first != null) {
        first.close();
      }
      if (second != null) {
        second.close();
      }
      if (producer != null) {
        producer.close();
      }
      if (firstReceiver != null) {
        firstReceiver.close();
      }
      if (secondReceiver != null) {
        secondReceiver.close();
      }
    }
  }

  @Test
  public void testRetentionReservesRebasedStringOffsets() {
    try (BufferAllocator producer = _allocator.newChildAllocator("producer", 0, 1024 * 1024);
        BufferAllocator receiver = _allocator.newChildAllocator("receiver", 0, 1024 * 1024);
        ArrowDataBlock source = rebasedStringBlock(producer);
        ArrowDataBlock retained = source.retainTo(receiver)) {
      assertEquals(retained.getString(0, 0), "payload");
      assertEquals(source.getString(0, 0), "payload");
      assertEquals(retained.getRoot().getVector(0).getOffsetBuffer().getInt(0), 0);
    }
  }

  @Test
  public void testRetentionAllocationFailureClosesReservationAndPartialVectors() {
    try (BufferAllocator producer = _allocator.newChildAllocator("producer", 0, 1024 * 1024);
        ArrowDataBlock source = rebasedStringBlock(producer)) {
      long allocated = producer.getAllocatedMemory();
      AtomicBoolean partiallyTransferred = new AtomicBoolean();
      AllocationListener listener = new AllocationListener() {
        @Override
        public void onPreAllocation(long size) {
          partiallyTransferred.set(producer.getAllocatedMemory() < allocated);
          throw new OutOfMemoryException("Injected rebased-offset allocation failure");
        }
      };
      try (BufferAllocator receiver = _allocator.newChildAllocator("receiver", listener, 0, 1024 * 1024)) {
        expectThrows(OutOfMemoryException.class, () -> source.retainTo(receiver));
        assertTrue(partiallyTransferred.get());
        assertEquals(receiver.getAllocatedMemory(), 0L);
        assertTrue(receiver.getChildAllocators().isEmpty());
        assertEquals(producer.getAllocatedMemory(), allocated);
        assertEquals(source.getString(0, 0), "payload");
      }
    }
  }

  @Test
  public void testDecodeOomAtSuccessiveAllocationBoundaries()
      throws IOException {
    byte[] wire;
    try (ArrowDataBlock source = scalarBlock(_allocator)) {
      wire = bytes(source.serialize());
    }
    for (int failAfter = 0; failAfter < 7; failAfter++) {
      AtomicInteger remaining = new AtomicInteger(failAfter);
      AllocationListener listener = new AllocationListener() {
        @Override
        public void onPreAllocation(long size) {
          if (remaining.getAndDecrement() == 0) {
            throw new OutOfMemoryException("Injected allocation failure");
          }
        }
      };
      try (BufferAllocator limited = _allocator.newChildAllocator("failure-" + failAfter, listener, 0, 1024 * 1024)) {
        expectThrows(IOException.class, () -> DataBlockUtils.deserialize(List.of(ByteBuffer.wrap(wire)), limited));
        assertEquals(limited.getAllocatedMemory(), 0L, "Failure after allocation " + failAfter);
      }
    }
    try (BufferAllocator limited = _allocator.newChildAllocator("zero-limit", 0, 0)) {
      expectThrows(IOException.class, () -> DataBlockUtils.deserialize(List.of(ByteBuffer.wrap(wire)), limited));
      assertEquals(limited.getAllocatedMemory(), 0L);
    }
  }

  @Test
  public void testFinalOffsetConsumerFailureReleasesDecodedBlock()
      throws IOException {
    byte[] wire;
    try (ArrowDataBlock source = scalarBlock(_allocator)) {
      wire = bytes(source.serialize());
    }
    expectThrows(IOException.class, () -> DataBlockUtils.deserialize(PinotByteBuffer.wrap(wire), 0, end -> {
      throw new IllegalStateException("consumer failed");
    }, _allocator));
    assertEquals(_allocator.getAllocatedMemory(), 0L);
  }

  @Test
  public void testTruncatedFramesAndInvalidOffsets()
      throws IOException {
    byte[] wire;
    try (ArrowDataBlock source = scalarBlock(_allocator)) {
      wire = bytes(source.serialize());
    }
    for (int length : new int[]{0, 3, 11, 16, wire.length - 8, wire.length - 1}) {
      byte[] truncated = Arrays.copyOf(wire, length);
      assertInvalid(truncated);
      if (length >= 12) {
        ByteBuffer.wrap(truncated).putLong(4, length);
        assertInvalid(truncated);
      }
    }
    expectThrows(IOException.class,
        () -> DataBlockUtils.deserialize(PinotByteBuffer.wrap(wire), -1, null, _allocator));
    expectThrows(IOException.class,
        () -> DataBlockUtils.deserialize(PinotByteBuffer.wrap(wire), Long.MAX_VALUE, null, _allocator));
    for (long invalidLength : new long[]{-1, 0, Long.MAX_VALUE}) {
      byte[] invalid = wire.clone();
      ByteBuffer.wrap(invalid).putLong(4, invalidLength);
      assertInvalid(invalid);
    }
  }

  @Test
  public void testRejectsMismatchedSourceSchema()
      throws IOException {
    try (ArrowDataBlock source = intBlock(_allocator, 3)) {
      ArrowDataBlock borrowed = new ArrowDataBlock(source.getRoot(),
          new DataSchema(new String[]{"i"}, new ColumnDataType[]{ColumnDataType.LONG}));
      expectThrows(IOException.class, borrowed::serialize);
      assertEquals(source.getInt(2, 0), 26);
    }
  }

  @DataProvider
  public Object[][] corruptHeaders() {
    return new Object[][]{
        {0, VERSION_TYPE & ~31 | 31}, {0, (31 << 5) | 3}, {0, VERSION_TYPE & ~31 | 2},
        {0, (DataBlock.Type.ROW.ordinal() << 5) | 3}, {12, -1}, {12, Integer.MAX_VALUE},
        {16, Integer.MAX_VALUE}, {20, Integer.MAX_VALUE}
    };
  }

  @Test(dataProvider = "corruptHeaders")
  public void testInvalidVersionTypeAndSchemaFraming(int offset, int value)
      throws IOException {
    byte[] wire;
    try (ArrowDataBlock source = intBlock(_allocator, 3)) {
      wire = bytes(source.serialize());
    }
    ByteBuffer.wrap(wire).putInt(offset, value);
    assertInvalid(wire);
  }

  @Test
  public void testInvalidSchemaNamesAndLogicalTypes()
      throws IOException {
    byte[] wire;
    try (ArrowDataBlock source = intBlock(_allocator, 3)) {
      wire = bytes(source.serialize());
    }
    byte[] wrongName = wire.clone();
    wrongName[24] = 'x';
    assertInvalid(wrongName);
    int schemaLength = ByteBuffer.wrap(wire).getInt(12);
    for (String invalidType : List.of("BAD", "MAP")) {
      byte[] invalid = wire.clone();
      byte[] name = invalidType.getBytes(StandardCharsets.UTF_8);
      System.arraycopy(name, 0, invalid, 16 + schemaLength - name.length, name.length);
      assertInvalid(invalid);
    }
    int ipcStart = 16 + schemaLength + Integer.BYTES;
    for (int invalidLength : new int[]{-1, Integer.MAX_VALUE}) {
      byte[] invalidArrowMetadata = wire.clone();
      ByteBuffer.wrap(invalidArrowMetadata).order(ByteOrder.LITTLE_ENDIAN).putInt(ipcStart + 4, invalidLength);
      assertInvalid(invalidArrowMetadata);
    }
  }

  @Test
  public void testRejectsMissingAndAdditionalRecordBatches()
      throws IOException {
    for (int batches : new int[]{0, 2}) {
      byte[] wire;
      try (ArrowDataBlock source = scalarBlock(_allocator)) {
        wire = frameWithBatches(source, batches);
      }
      assertInvalid(wire);
    }
  }

  @Test
  public void testRejectsTrailingBytesInsideFrame()
      throws IOException {
    byte[] wire;
    try (ArrowDataBlock source = scalarBlock(_allocator)) {
      wire = bytes(source.serialize());
    }
    byte[] trailing = Arrays.copyOf(wire, wire.length + 8);
    ByteBuffer.wrap(trailing).putLong(4, trailing.length).putInt(wire.length, -1);
    assertInvalid(trailing);
  }

  @Test
  public void testUnknownDictionaryMessageDoesNotLeakItsBody()
      throws IOException {
    byte[] wire;
    try (ArrowDataBlock source = scalarBlock(_allocator);
        PagedPinotOutputStream stream = PagedPinotOutputStream.createHeap()) {
      writeFrameHeader(stream, source.getDataSchema());
      try (ArrowStreamWriter writer = new ArrowStreamWriter(source.getRoot(), source.getDictionaryProvider(), stream)) {
        writer.start();
        FieldVector values = source.getDictionaryProvider().lookup(37).getVector();
        VectorSchemaRoot borrowed = new VectorSchemaRoot(List.of(values.getField()), List.of(values));
        try (ArrowDictionaryBatch invalid = new ArrowDictionaryBatch(123456,
            new VectorUnloader(borrowed).getRecordBatch(), false)) {
          MessageSerializer.serialize(new WriteChannel(Channels.newChannel(stream)), invalid);
        }
        writer.end();
      }
      long length = stream.getCurrentOffset();
      stream.seek(4);
      stream.writeLong(length);
      wire = bytes(Arrays.asList(stream.getPages()));
    }
    assertInvalid(wire);
  }

  private void assertInvalid(byte[] wire) {
    AtomicLong finalOffset = new AtomicLong(-1);
    expectThrows(IOException.class,
        () -> DataBlockUtils.deserialize(PinotByteBuffer.wrap(wire), 0, finalOffset::set, _allocator));
    assertEquals(finalOffset.get(), -1L);
    assertEquals(_allocator.getAllocatedMemory(), 0L);
  }

  private static ArrowDataBlock intBlock(BufferAllocator allocator, int rows) {
    IntVector values = new IntVector("i", allocator);
    values.allocateNew(rows);
    for (int i = 0; i < rows; i++) {
      values.set(i, i * 13);
    }
    values.setValueCount(rows);
    return new ArrowDataBlock(new VectorSchemaRoot(List.of(values.getField()), List.of(values), rows), INT_SCHEMA);
  }

  private static ArrowDataBlock scalarBlock(BufferAllocator allocator) {
    List<Field> fields = List.of(
        field("i", new ArrowType.Int(32, true)), field("l", new ArrowType.Int(64, true)),
        field("f", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
        field("d", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), field("bool", new ArrowType.Bool()),
        field("boolInt", new ArrowType.Int(32, true)), field("ts", new ArrowType.Int(64, true)),
        field("bytes", new ArrowType.Binary()), field("s", new ArrowType.Utf8()), field("json", new ArrowType.Utf8()),
        field("decimal", new ArrowType.Utf8()),
        new Field("dict", new FieldType(true, new ArrowType.Int(32, true), STRING_ENCODING,
            Map.of("cardinality", "low")), null),
        new Field("dictJson", new FieldType(true, new ArrowType.Int(32, true), JSON_ENCODING), null));
    VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(fields, Map.of("source", "codec-test")), allocator);
    MapDictionaryProvider dictionaries = new MapDictionaryProvider();
    try {
      for (FieldVector vector : root.getFieldVectors()) {
        vector.setInitialCapacity(3);
      }
      root.allocateNew();
      ((IntVector) root.getVector(0)).set(0, Integer.MIN_VALUE);
      ((IntVector) root.getVector(0)).set(2, Integer.MAX_VALUE);
      ((BigIntVector) root.getVector(1)).set(0, Long.MIN_VALUE);
      ((BigIntVector) root.getVector(1)).set(2, Long.MAX_VALUE);
      ((Float4Vector) root.getVector(2)).set(0, -0.0f);
      ((Float4Vector) root.getVector(2)).set(2, Float.NaN);
      ((Float8Vector) root.getVector(3)).set(0, Double.POSITIVE_INFINITY);
      ((Float8Vector) root.getVector(3)).set(2, Double.NaN);
      ((BitVector) root.getVector(4)).set(0, 1);
      ((BitVector) root.getVector(4)).set(2, 0);
      ((IntVector) root.getVector(5)).set(0, 0);
      ((IntVector) root.getVector(5)).set(2, 1);
      ((BigIntVector) root.getVector(6)).set(0, 1700000000123L);
      ((BigIntVector) root.getVector(6)).set(2, Long.MAX_VALUE);
      ((VarBinaryVector) root.getVector(7)).setSafe(0, new byte[]{0, 127, -1});
      ((VarBinaryVector) root.getVector(7)).setSafe(2, new byte[0]);
      setString((VarCharVector) root.getVector(8), 0, "plain-☺");
      setString((VarCharVector) root.getVector(8), 2, "");
      setString((VarCharVector) root.getVector(9), 0, "{\"k\":1}");
      setString((VarCharVector) root.getVector(9), 2, "null");
      setString((VarCharVector) root.getVector(10), 0, "1.200");
      setString((VarCharVector) root.getVector(10), 2, "-0.0001");
      for (int i = 11; i < 13; i++) {
        ((IntVector) root.getVector(i)).set(0, 1);
        ((IntVector) root.getVector(i)).set(2, 0);
      }
      for (FieldVector vector : root.getFieldVectors()) {
        vector.setNull(1);
      }
      root.setRowCount(3);
      addDictionary(dictionaries, allocator, STRING_ENCODING, "x", "shared-✔");
      addDictionary(dictionaries, allocator, JSON_ENCODING, "{\"k\":2}", "{\"k\":3}");
      ArrowDataBlock block = new ArrowDataBlock(root, SCALAR_SCHEMA, dictionaries);
      block.addException(400, "example-error-☺");
      return block;
    } catch (RuntimeException | Error e) {
      dictionaries.close();
      root.close();
      throw e;
    }
  }

  private static ArrowDataBlock dictionaryPayloadBlock(BufferAllocator allocator) {
    IntVector indices = (IntVector) new Field("s",
        new FieldType(true, new ArrowType.Int(32, true), STRING_ENCODING), null).createVector(allocator);
    indices.allocateNew(2);
    indices.set(0, 0);
    indices.setNull(1);
    indices.setValueCount(2);
    MapDictionaryProvider dictionaries = new MapDictionaryProvider();
    addDictionary(dictionaries, allocator, STRING_ENCODING, "x".repeat(8192), "y");
    return new ArrowDataBlock(new VectorSchemaRoot(List.of(indices.getField()), List.of(indices), 2),
        new DataSchema(new String[]{"s"}, new ColumnDataType[]{ColumnDataType.STRING}), dictionaries);
  }

  private static ArrowDataBlock rebasedStringBlock(BufferAllocator allocator) {
    VarCharVector values = new VarCharVector("s", allocator);
    values.allocateNew(64, 1);
    setString(values, 0, "prefix-payload");
    values.setValueCount(1);
    values.getOffsetBuffer().setInt(0, "prefix-".length());
    return new ArrowDataBlock(new VectorSchemaRoot(List.of(values.getField()), List.of(values), 1),
        new DataSchema(new String[]{"s"}, new ColumnDataType[]{ColumnDataType.STRING}));
  }

  private static void addDictionary(MapDictionaryProvider provider, BufferAllocator allocator,
      DictionaryEncoding encoding, String first, String second) {
    VarCharVector values = new VarCharVector("dictionary-" + encoding.getId(), allocator);
    provider.put(new Dictionary(values, encoding));
    values.allocateNew(64, 2);
    setString(values, 0, first);
    setString(values, 1, second);
    values.setValueCount(2);
  }

  private static Field field(String name, ArrowType type) {
    return new Field(name, FieldType.nullable(type), null);
  }

  private static void setString(VarCharVector vector, int row, String value) {
    vector.setSafe(row, value.getBytes(StandardCharsets.UTF_8));
  }

  private static void assertScalars(ArrowDataBlock block) {
    assertEquals(block.getNumberOfRows(), 3);
    assertEquals(block.getDataSchema(), SCALAR_SCHEMA);
    for (int i = 0; i < SCALAR_SCHEMA.size(); i++) {
      assertEquals(block.getNullRowIds(i), RoaringBitmap.bitmapOf(1), "Nulls in column " + i);
    }
    assertEquals(block.getInt(0, 0), Integer.MIN_VALUE);
    assertEquals(block.getInt(2, 0), Integer.MAX_VALUE);
    assertEquals(block.getLong(0, 1), Long.MIN_VALUE);
    assertEquals(block.getLong(2, 1), Long.MAX_VALUE);
    assertEquals(Float.floatToRawIntBits(block.getFloat(0, 2)), Float.floatToRawIntBits(-0.0f));
    assertTrue(Float.isNaN(block.getFloat(2, 2)));
    assertEquals(block.getDouble(0, 3), Double.POSITIVE_INFINITY);
    assertTrue(Double.isNaN(block.getDouble(2, 3)));
    assertEquals(block.getInt(0, 4), 1);
    assertEquals(block.getInt(2, 4), 0);
    assertEquals(block.getInt(0, 5), 0);
    assertEquals(block.getInt(2, 5), 1);
    assertEquals(block.getLong(0, 6), 1700000000123L);
    assertEquals(block.getLong(2, 6), Long.MAX_VALUE);
    assertEquals(block.getBytes(0, 7), new ByteArray(new byte[]{0, 127, -1}));
    assertEquals(block.getBytes(2, 7), new ByteArray(new byte[0]));
    assertEquals(block.getString(0, 8), "plain-☺");
    assertEquals(block.getString(2, 8), "");
    assertEquals(block.getString(0, 9), "{\"k\":1}");
    assertEquals(block.getString(2, 9), "null");
    assertEquals(block.getBigDecimal(0, 10), new BigDecimal("1.200"));
    assertEquals(block.getBigDecimal(2, 10), new BigDecimal("-0.0001"));
    assertEquals(block.getString(0, 11), "shared-✔");
    assertEquals(block.getString(2, 11), "x");
    assertEquals(block.getString(0, 12), "{\"k\":3}");
    assertEquals(block.getString(2, 12), "{\"k\":2}");
    assertNull(block.getBytes(1, 7));
    assertNull(block.getString(1, 8));
    assertNull(block.getString(1, 9));
    assertNull(block.getBigDecimal(1, 10));
    assertNull(block.getString(1, 11));
    assertNull(block.getString(1, 12));
  }

  private static List<ArrowBuf> allBuffers(ArrowDataBlock block) {
    List<ArrowBuf> buffers = new ArrayList<>();
    for (FieldVector vector : block.getRoot().getFieldVectors()) {
      buffers.addAll(vector.getFieldBuffers());
    }
    for (long id : block.getDictionaryProvider().getDictionaryIds()) {
      buffers.addAll(block.getDictionaryProvider().lookup(id).getVector().getFieldBuffers());
    }
    return buffers;
  }

  private static byte[] frameWithBatches(ArrowDataBlock block, int batches)
      throws IOException {
    try (PagedPinotOutputStream stream = PagedPinotOutputStream.createHeap()) {
      writeFrameHeader(stream, block.getDataSchema());
      try (ArrowStreamWriter writer = new ArrowStreamWriter(block.getRoot(), block.getDictionaryProvider(), stream)) {
        writer.start();
        for (int i = 0; i < batches; i++) {
          writer.writeBatch();
        }
        writer.end();
      }
      long length = stream.getCurrentOffset();
      stream.seek(4);
      stream.writeLong(length);
      return bytes(Arrays.asList(stream.getPages()));
    }
  }

  private static void writeFrameHeader(PagedPinotOutputStream stream, DataSchema schema)
      throws IOException {
    stream.writeInt(VERSION_TYPE);
    stream.writeLong(0);
    byte[] schemaBytes = schema.toBytes();
    stream.writeInt(schemaBytes.length);
    stream.write(schemaBytes);
    stream.writeInt(0);
  }

  private static byte[] bytes(List<ByteBuffer> chunks) {
    int size = chunks.stream().mapToInt(ByteBuffer::remaining).sum();
    ByteBuffer result = ByteBuffer.allocate(size);
    for (ByteBuffer chunk : chunks) {
      result.put(chunk.duplicate());
    }
    return result.array();
  }
}
