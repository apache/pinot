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
package org.apache.pinot.query.runtime.operator.exchange;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
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
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.common.datablock.ArrowDataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.mailbox.SendingMailbox;
import org.apache.pinot.query.runtime.blocks.ArrowBlock;
import org.apache.pinot.query.runtime.blocks.BlockSplitter;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.memory.ArrowBuffers;
import org.apache.pinot.query.runtime.memory.ArrowQueryContext;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.TerminationException;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.ByteArray;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/** Checks destination-level legacy parity and borrowed-source ownership using real Arrow vectors. */
public class ArrowHashExchangeTest {
  private static final long STRING_DICTIONARY_ID = 37;
  private static final long JSON_DICTIONARY_ID = 991;
  private static final String[] STRINGS = {"repeat", "\u20ac\uD83D\uDE00", ""};
  private static final String[] JSON = {"{\"x\":0}", "{\"x\":1}", "null"};
  private static final DataSchema SCHEMA = new DataSchema(
      new String[]{"row", "intKey", "longKey", "floatKey", "doubleKey", "boolKey", "timestampKey",
          "dictionaryText", "dictionaryJson", "plainText", "bytes"},
      new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.FLOAT,
          ColumnDataType.DOUBLE, ColumnDataType.BOOLEAN, ColumnDataType.TIMESTAMP, ColumnDataType.STRING,
          ColumnDataType.JSON, ColumnDataType.STRING, ColumnDataType.BYTES});

  @DataProvider
  public Object[][] numericKeys() {
    List<Object[]> cases = new ArrayList<>();
    for (String algorithm : List.of("absHashCode", "HaShCoDe")) {
      for (List<Integer> keys : List.of(List.of(0), List.of(1), List.of(2), List.of(3), List.of(4), List.of(5),
          List.of(6), List.of(1, 2), List.of(3, 4), List.of(1, 2, 3, 4, 5, 6), List.of(1, 1, 2))) {
        for (boolean dictionaries : new boolean[]{false, true}) {
          for (int destinations : new int[]{3, 7}) {
            cases.add(new Object[]{keys, algorithm, dictionaries, destinations});
          }
        }
      }
    }
    return cases.toArray(new Object[0][]);
  }

  @Test(dataProvider = "numericKeys")
  public void testNumericKeysMatchLegacyWithoutMaterializingSource(List<Integer> keys, String algorithm,
      boolean dictionaries, int destinations) {
    List<Object[]> rows = rows(96);
    try (ArrowBuffers buffers = newBuffers();
        ArrowQueryContext context = newContext(buffers, 0)) {
      ArrowBlock source = spy(newBlock(context.getAllocator(), rows, dictionaries));
      doThrow(new AssertionError("Native hash exchange must not materialize its source"))
          .when(source).asRowHeap();
      try {
        long sourceBytes = context.getAllocator().getAllocatedMemory();
        List<MseBlock.Data> sent = assertDistribution(source, rows, keys, algorithm, context, true, destinations);
        verify(source, never()).asRowHeap();
        assertEquals(context.getAllocator().getAllocatedMemory(), sourceBytes);
        for (MseBlock.Data block : sent) {
          assertNotSame(block, source);
          assertThrows(IllegalStateException.class, ((ArrowBlock) block)::retain);
        }
      } finally {
        source.release();
      }
      assertEquals(context.getAllocator().getAllocatedMemory(), 0L);
    }
  }

  @DataProvider
  public Object[][] fallbackKeys() {
    return new Object[][]{
        {List.of(7), "absHashCode"},
        {List.of(8), "hashcode"},
        {List.of(9), "absHashCode"},
        {List.of(10), "hashcode"},
        {List.of(1, 7), "absHashCode"},
        {List.of(1), "murmur"},
        {List.of(2, 3, 4), "MuRmUr3"},
        {List.of(1), "unrecognized-legacy-default"}
    };
  }

  @Test(dataProvider = "fallbackKeys")
  public void testFallbackMaterializesOnce(List<Integer> keys, String algorithm) {
    List<Object[]> rows = rows(48);
    try (ArrowBuffers buffers = newBuffers();
        ArrowQueryContext context = newContext(buffers, 0)) {
      ArrowBlock source = spy(newBlock(context.getAllocator(), rows, true));
      try {
        long sourceBytes = context.getAllocator().getAllocatedMemory();
        assertDistribution(source, rows, keys, algorithm, context, false, 5);
        verify(source, times(1)).asRowHeap();
        assertEquals(context.getAllocator().getAllocatedMemory(), sourceBytes);
      } finally {
        source.release();
      }
      assertEquals(context.getAllocator().getAllocatedMemory(), 0L);
    }
  }

  @Test
  public void testDisabledContextKeepsLegacyBoundary() {
    List<Object[]> rows = rows(48);
    try (ArrowBuffers buffers = newBuffers();
        ArrowQueryContext context = newContext(buffers, 0)) {
      ArrowBlock source = spy(newBlock(context.getAllocator(), rows, true));
      try {
        assertDistribution(source, rows, List.of(1, 2), "absHashCode", null, false, 5);
        verify(source, times(1)).asRowHeap();
      } finally {
        source.release();
      }
      assertEquals(context.getAllocator().getAllocatedMemory(), 0L);
    }
  }

  @Test
  public void testRowHeapInputStaysOnLegacyPath() {
    List<Object[]> rows = rows(48);
    try (ArrowBuffers buffers = newBuffers();
        ArrowQueryContext context = newContext(buffers, 0)) {
      assertDistribution(new RowHeapDataBlock(rows, SCHEMA), rows, List.of(1, 2), "hashcode", context, false, 5);
      assertEquals(context.getAllocator().getAllocatedMemory(), 0L);
    }
  }

  @DataProvider
  public Object[][] emptyAndSingleton() {
    List<Object[]> cases = new ArrayList<>();
    for (int count : new int[]{0, 13}) {
      for (List<Integer> keys : List.of(List.<Integer>of(), List.of(1))) {
        for (int destinations : new int[]{1, 4}) {
          for (String algorithm : List.of("absHashCode", "hashcode")) {
            cases.add(new Object[]{count, keys, destinations, algorithm});
          }
        }
      }
    }
    return cases.toArray(new Object[0][]);
  }

  @Test(dataProvider = "emptyAndSingleton")
  public void testEmptyAndSingletonPaths(int count, List<Integer> keys, int destinations, String algorithm) {
    List<Object[]> rows = rows(count);
    try (ArrowBuffers buffers = newBuffers();
        ArrowQueryContext context = newContext(buffers, 0)) {
      ArrowBlock source = newBlock(context.getAllocator(), rows, true);
      try {
        long sourceBytes = context.getAllocator().getAllocatedMemory();
        List<MseBlock.Data> sent = assertDistribution(source, rows, keys, algorithm, context, true, destinations);
        assertRows(source.asRowHeap().getRows(), rows);
        assertEquals(context.getAllocator().getAllocatedMemory(), sourceBytes);
        if (destinations == 1 || (keys.isEmpty() && algorithm.equals("absHashCode"))) {
          assertEquals(sent.size(), 1);
          assertSame(sent.get(0), source);
        } else if (count == 0) {
          assertTrue(sent.isEmpty());
        }
      } finally {
        source.release();
      }
      assertEquals(context.getAllocator().getAllocatedMemory(), 0L);
    }
  }

  @Test
  public void testTerminatedPartitionDoesNotAcquireAllocator() {
    try (ArrowBuffers buffers = newBuffers(); ArrowQueryContext context = newContext(buffers, 0)) {
      ArrowBlock source = newBlock(context.getAllocator(), rows(1), true);
      SendingMailbox terminated = mock(SendingMailbox.class);
      SendingMailbox active = mock(SendingMailbox.class);
      when(terminated.isEarlyTerminated()).thenReturn(true);
      BlockExchange exchange = BlockExchange.getExchange(List.of(terminated, active),
          RelDistribution.Type.HASH_DISTRIBUTED, List.of(0), BlockSplitter.DEFAULT, "hashcode", () -> {
            throw new AssertionError("A terminated partition must not acquire an Arrow allocator");
          });
      try {
        exchange.send(source);
        verify(terminated, never()).send(any(MseBlock.Data.class));
        verify(active, never()).send(any(MseBlock.Data.class));
      } finally {
        exchange.close();
        source.release();
      }
    }
  }

  @Test
  public void testSelectedDictionariesAreCompactedAndSharedIdsRemainConsistent() {
    try (ArrowBuffers buffers = newBuffers();
        ArrowQueryContext sourceContext = newContext(buffers, 0);
        ArrowQueryContext targetContext = newContext(buffers, 1)) {
      DictionaryEncoding encoding = new DictionaryEncoding(STRING_DICTIONARY_ID, true, new ArrowType.Int(32, true));
      FieldType indexed = new FieldType(true, new ArrowType.Int(32, true), encoding);
      VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(List.of(
          new Field("left", indexed, null), new Field("right", indexed, null))), sourceContext.getAllocator());
      MapDictionaryProvider dictionaries = new MapDictionaryProvider();
      VarCharVector values = new VarCharVector("values", sourceContext.getAllocator());
      dictionaries.put(new Dictionary(values, encoding));
      DataSchema schema = new DataSchema(new String[]{"left", "right"},
          new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.STRING});
      ArrowDataBlock source = new ArrowDataBlock(root, schema, dictionaries);
      try (source) {
        values.allocateNew();
        String suffix = "x".repeat(1000);
        for (int id = 0; id < 64; id++) {
          values.setSafe(id, ("value-" + (100 + id) + suffix).getBytes(StandardCharsets.UTF_8));
        }
        values.setValueCount(64);
        root.allocateNew();
        IntVector left = (IntVector) root.getVector(0);
        IntVector right = (IntVector) root.getVector(1);
        left.set(0, 50);
        left.set(1, 2);
        left.set(2, 50);
        left.setNull(3);
        right.set(0, 60);
        right.set(1, 50);
        right.setNull(2);
        right.set(3, 2);
        root.setRowCount(4);
        try (ArrowDataBlock selected =
            ArrowRowSelection.select(source, new int[]{0, 1, 2, 3}, targetContext.getAllocator())) {
          assertEquals(selected.getDataSchema(), schema);
          assertEquals(selected.getDictionaryProvider().getDictionaryIds(), dictionaries.getDictionaryIds());
          Dictionary compact = selected.getDictionaryProvider().lookup(STRING_DICTIONARY_ID);
          assertEquals(compact.getVector().getValueCount(), 3);
          assertTrue(compact.getEncoding().isOrdered());
          assertEquals(((IntVector) selected.getRoot().getVector(0)).get(0), 1);
          assertEquals(((IntVector) selected.getRoot().getVector(1)).get(0), 2);
          assertTrue(selected.getRoot().getVector(0).isNull(3));
          assertTrue(selected.getRoot().getVector(1).isNull(2));
          assertEquals(selected.getString(0, 0), source.getString(0, 0));
          assertEquals(selected.getString(0, 1), source.getString(0, 1));
          assertEquals(selected.getString(1, 0), source.getString(1, 0));
          assertEquals(values.getValueCount(), 64);
          assertTrue(compact.getVector().getBufferSize() < values.getBufferSize() / 8);
          try (BufferAllocator limited = targetContext.getAllocator().newChildAllocator("dictionary-limit", 0, 512)) {
            assertThrows(OutOfMemoryException.class,
                () -> ArrowRowSelection.select(source, new int[]{0, 1}, limited));
            assertEquals(limited.getAllocatedMemory(), 0L);
            assertEquals(source.getString(0, 0), selected.getString(0, 0));
          }
        }
      }
    }
  }

  @Test
  public void testPartitionsAndDictionariesOutliveSource() {
    List<Object[]> rows = rows(48);
    try (ArrowBuffers buffers = newBuffers();
        ArrowQueryContext sender = newContext(buffers, 0);
        ArrowQueryContext firstReceiver = newContext(buffers, 1);
        ArrowQueryContext secondReceiver = newContext(buffers, 2)) {
      ArrowBlock source = newBlock(sender.getAllocator(), rows, true);
      List<ArrowBlock> received = new ArrayList<>();
      List<SendingMailbox> mailboxes = new ArrayList<>();
      for (ArrowQueryContext receiver : List.of(firstReceiver, secondReceiver)) {
        SendingMailbox mailbox = mock(SendingMailbox.class);
        when(mailbox.isLocal()).thenReturn(true);
        doAnswer(invocation -> {
          ArrowBlock partition = ((MseBlock.Data) invocation.getArgument(0)).asArrow();
          assertNotSame(partition.getDataBlock().getRoot(), source.getDataBlock().getRoot());
          assertEquals(partition.getNumRows(), rows.size() / 2);
          MapDictionaryProvider dictionaries = partition.getDataBlock().getDictionaryProvider();
          assertNotSame(dictionaries, source.getDataBlock().getDictionaryProvider());
          assertEquals(dictionaries.getDictionaryIds(),
              source.getDataBlock().getDictionaryProvider().getDictionaryIds());
          for (long id : dictionaries.getDictionaryIds()) {
            assertNotSame(dictionaries.lookup(id).getVector(),
                source.getDataBlock().getDictionaryProvider().lookup(id).getVector());
          }
          received.add(receiver.createBlock(partition.getDataBlock().retainTo(receiver.getAllocator())));
          return null;
        }).when(mailbox).send(any(MseBlock.Data.class));
        mailboxes.add(mailbox);
      }
      try {
        BlockExchange.getExchange(mailboxes, RelDistribution.Type.HASH_DISTRIBUTED, List.of(0),
            BlockSplitter.DEFAULT, "hashcode", () -> sender).send(source);
      } finally {
        source.release();
      }
      sender.close();
      assertEquals(received.size(), 2);
      try {
        for (int destination = 0; destination < received.size(); destination++) {
          List<Object[]> expected = new ArrayList<>();
          for (int row = destination; row < rows.size(); row += received.size()) {
            expected.add(rows.get(row));
          }
          assertRows(received.get(destination).asRowHeap().getRows(), expected);
          assertEquals(received.get(destination).getDataBlock().getRoot().getVector(7).getField()
              .getDictionary().getId(), STRING_DICTIONARY_ID);
          assertEquals(received.get(destination).getDataBlock().getRoot().getVector(8).getField()
              .getDictionary().getId(), JSON_DICTIONARY_ID);
        }
      } finally {
        received.get(0).release();
        firstReceiver.close();
        assertEquals(received.get(1).getDataBlock().getString(0, 7), rows.get(1)[7]);
        received.get(1).release();
      }
      assertEquals(buffers.getAllocatedMemory(), 0L);
    }
  }

  @DataProvider
  public Object[][] failurePaths() {
    return new Object[][]{{true, false}, {false, false}, {false, true}};
  }

  @Test(dataProvider = "failurePaths")
  public void testSendOrSplitFailureReleasesOnlyPartition(boolean local, boolean splitFailure) {
    List<Object[]> rows = rows(48);
    try (ArrowBuffers buffers = newBuffers();
        ArrowQueryContext context = newContext(buffers, 0)) {
      ArrowBlock source = newBlock(context.getAllocator(), rows, true);
      List<ArrowBlock> sent = new ArrayList<>();
      RuntimeException failure = new IllegalStateException("send failed");
      SendingMailbox mailbox = mock(SendingMailbox.class);
      when(mailbox.isLocal()).thenReturn(local);
      doAnswer(invocation -> {
        sent.add(((MseBlock.Data) invocation.getArgument(0)).asArrow());
        throw failure;
      }).when(mailbox).send(any(MseBlock.Data.class));
      BlockSplitter splitter = splitFailure ? (block, maxBytes) -> {
        sent.add(block.asArrow());
        throw failure;
      } : BlockSplitter.DEFAULT;
      BlockExchange exchange = BlockExchange.getExchange(List.of(mailbox, mailbox),
          RelDistribution.Type.HASH_DISTRIBUTED, List.of(0), splitter, "absHashCode", () -> context);
      try {
        long sourceBytes = context.getAllocator().getAllocatedMemory();
        assertSame(expectThrows(IllegalStateException.class, () -> exchange.send(source)), failure);
        assertEquals(sent.size(), 1);
        assertNotSame(sent.get(0), source);
        assertThrows(IllegalStateException.class, sent.get(0)::retain);
        assertRows(source.asRowHeap().getRows(), rows);
        assertEquals(context.getAllocator().getAllocatedMemory(), sourceBytes);
      } finally {
        source.release();
      }
      assertEquals(context.getAllocator().getAllocatedMemory(), 0L);
    }
  }

  @Test
  public void testCancellationDuringPartitioningReclaimsPartialGather() {
    List<Object[]> rows = rows(48);
    try (ArrowBuffers buffers = newBuffers();
        ArrowQueryContext context = newContext(buffers, 0);
        QueryThreadContext query = QueryThreadContext.openForMseTest()) {
      ArrowBlock source = newBlock(context.getAllocator(), rows, true);
      SendingMailbox first = mock(SendingMailbox.class);
      SendingMailbox second = mock(SendingMailbox.class);
      when(first.isLocal()).thenReturn(true);
      when(second.isLocal()).thenReturn(true);
      doAnswer(invocation -> {
        query.getExecutionContext().terminate(QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED, "cancel after first send");
        return null;
      }).when(first).send(any(MseBlock.Data.class));
      try {
        long sourceBytes = context.getAllocator().getAllocatedMemory();
        BlockExchange exchange = BlockExchange.getExchange(List.of(first, second),
            RelDistribution.Type.HASH_DISTRIBUTED, List.of(0), BlockSplitter.DEFAULT, "hashcode", () -> context);
        assertThrows(TerminationException.class, () -> exchange.send(source));
        verify(first).send(any(MseBlock.Data.class));
        verify(second, never()).send(any(MseBlock.Data.class));
        assertEquals(context.getAllocator().getAllocatedMemory(), sourceBytes);
      } finally {
        source.release();
      }
      assertEquals(context.getAllocator().getAllocatedMemory(), 0L);
    }
  }

  @Test
  public void testPreterminatedQueryDoesNotSendOrReleaseSource() {
    List<Object[]> rows = rows(QueryThreadContext.CHECK_TERMINATION_AND_SAMPLE_USAGE_RECORD_MASK + 2);
    try (ArrowBuffers buffers = newBuffers();
        ArrowQueryContext context = newContext(buffers, 0);
        QueryThreadContext query = QueryThreadContext.openForMseTest()) {
      ArrowBlock source = newBlock(context.getAllocator(), rows, true);
      SendingMailbox mailbox = mock(SendingMailbox.class);
      try {
        long sourceBytes = context.getAllocator().getAllocatedMemory();
        query.getExecutionContext().terminate(QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED, "cancel before hash");
        BlockExchange exchange = BlockExchange.getExchange(List.of(mailbox, mailbox),
            RelDistribution.Type.HASH_DISTRIBUTED, List.of(1), BlockSplitter.DEFAULT, "hashcode", () -> context);
        assertThrows(TerminationException.class, () -> exchange.send(source));
        verify(mailbox, never()).send(any(MseBlock.Data.class));
        assertEquals(context.getAllocator().getAllocatedMemory(), sourceBytes);
      } finally {
        source.release();
      }
      assertEquals(context.getAllocator().getAllocatedMemory(), 0L);
    }
  }

  @Test
  public void testAllDestinationsTerminatedBorrowWithoutAllocating() {
    try (ArrowBuffers buffers = newBuffers();
        ArrowQueryContext context = newContext(buffers, 0)) {
      ArrowBlock source = newBlock(context.getAllocator(), rows(16), true);
      SendingMailbox mailbox = mock(SendingMailbox.class);
      when(mailbox.isEarlyTerminated()).thenReturn(true);
      try {
        long sourceBytes = context.getAllocator().getAllocatedMemory();
        BlockExchange exchange = BlockExchange.getExchange(List.of(mailbox, mailbox),
            RelDistribution.Type.HASH_DISTRIBUTED, List.of(0), BlockSplitter.DEFAULT, "hashcode", () -> context);
        assertTrue(exchange.send(source));
        verify(mailbox, never()).send(any(MseBlock.Data.class));
        assertEquals(context.getAllocator().getAllocatedMemory(), sourceBytes);
      } finally {
        source.release();
      }
      assertEquals(context.getAllocator().getAllocatedMemory(), 0L);
    }
  }

  @Test
  public void testAllocationFailureClosesPartiallyGatheredRoot() {
    try (ArrowBuffers buffers = newBuffers();
        ArrowQueryContext sourceContext = newContext(buffers, 0);
        ArrowQueryContext partitionContext = new ArrowQueryContext(buffers.newAllocator("partition", 0, 256))) {
      ArrowBlock source = newBlock(sourceContext.getAllocator(), rows(48), true);
      SendingMailbox mailbox = mock(SendingMailbox.class);
      try {
        BlockExchange exchange = BlockExchange.getExchange(List.of(mailbox, mailbox),
            RelDistribution.Type.HASH_DISTRIBUTED, List.of(0), BlockSplitter.DEFAULT, "hashcode",
            () -> partitionContext);
        assertThrows(OutOfMemoryException.class, () -> exchange.send(source));
        verify(mailbox, never()).send(any(MseBlock.Data.class));
        assertEquals(partitionContext.getAllocator().getAllocatedMemory(), 0L);
        assertEquals(source.getDataBlock().getInt(1, 0), 1);
      } finally {
        source.release();
      }
      assertEquals(sourceContext.getAllocator().getAllocatedMemory(), 0L);
    }
  }

  @Test
  public void testDictionaryCopiesAreChargedToEachPartition() {
    try (ArrowBuffers buffers = newBuffers();
        ArrowQueryContext sourceContext = newContext(buffers, 0);
        ArrowQueryContext firstContext = newContext(buffers, 1);
        ArrowQueryContext secondContext = newContext(buffers, 2)) {
      ArrowBlock source = newBlock(sourceContext.getAllocator(), rows(48), true);
      try (ArrowDataBlock first =
          ArrowRowSelection.select(source.getDataBlock(), new int[]{0, 2}, firstContext.getAllocator());
          ArrowDataBlock second =
              ArrowRowSelection.select(source.getDataBlock(), new int[]{1, 3}, secondContext.getAllocator())) {
        for (ArrowDataBlock partition : List.of(first, second)) {
          for (long id : new long[]{STRING_DICTIONARY_ID, JSON_DICTIONARY_ID}) {
            FieldVector dictionary = partition.getDictionaryProvider().lookup(id).getVector();
            assertTrue(dictionary.getDataBuffer().getReferenceManager().getAccountedSize() > 0,
                "Every partition must be charged for its dictionary, regardless of other live recipients");
          }
        }
      } finally {
        source.release();
      }
      assertEquals(sourceContext.getAllocator().getAllocatedMemory(), 0L);
      assertEquals(firstContext.getAllocator().getAllocatedMemory(), 0L);
      assertEquals(secondContext.getAllocator().getAllocatedMemory(), 0L);
    }
  }

  @Test
  public void testDictionaryCompactionAvoidsChargingUnusedCapacity() {
    try (ArrowBuffers buffers = newBuffers();
        ArrowQueryContext sourceContext = newContext(buffers, 0);
        ArrowQueryContext partitionContext = new ArrowQueryContext(buffers.newAllocator("partition", 0, 4096))) {
      ArrowBlock source = newBlock(sourceContext.getAllocator(), rows(48), true);
      SendingMailbox mailbox = mock(SendingMailbox.class);
      try {
        BlockExchange exchange = BlockExchange.getExchange(List.of(mailbox, mailbox),
            RelDistribution.Type.HASH_DISTRIBUTED, List.of(0), BlockSplitter.DEFAULT, "hashcode",
            () -> partitionContext);
        exchange.send(source);
        verify(mailbox, times(2)).send(any(MseBlock.Data.class));
        assertEquals(partitionContext.getAllocator().getAllocatedMemory(), 0L);
        assertEquals(source.getDataBlock().getString(1, 7), STRINGS[1]);
      } finally {
        source.release();
      }
      assertEquals(sourceContext.getAllocator().getAllocatedMemory(), 0L);
    }
  }

  @Test
  public void testRejectedBlockRegistrationReleasesSelection() {
    try (ArrowBuffers buffers = newBuffers();
        ArrowQueryContext context = newContext(buffers, 0)) {
      ArrowBlock source = newBlock(context.getAllocator(), rows(48), true);
      ArrowQueryContext rejectingContext = mock(ArrowQueryContext.class);
      when(rejectingContext.getAllocator()).thenReturn(context.getAllocator());
      IllegalStateException failure = new IllegalStateException("context rejected block");
      when(rejectingContext.createBlock(any(ArrowDataBlock.class))).thenThrow(failure);
      SendingMailbox mailbox = mock(SendingMailbox.class);
      try {
        long sourceBytes = context.getAllocator().getAllocatedMemory();
        BlockExchange exchange = BlockExchange.getExchange(List.of(mailbox, mailbox),
            RelDistribution.Type.HASH_DISTRIBUTED, List.of(0), BlockSplitter.DEFAULT, "hashcode",
            () -> rejectingContext);
        assertSame(expectThrows(IllegalStateException.class, () -> exchange.send(source)), failure);
        verify(mailbox, never()).send(any(MseBlock.Data.class));
        assertEquals(context.getAllocator().getAllocatedMemory(), sourceBytes);
        assertEquals(source.getDataBlock().getString(1, 7), STRINGS[1]);
      } finally {
        source.release();
      }
      assertEquals(context.getAllocator().getAllocatedMemory(), 0L);
    }
  }

  private static List<MseBlock.Data> assertDistribution(MseBlock.Data source, List<Object[]> rows, List<Integer> keys,
      String algorithm, @Nullable ArrowQueryContext context, boolean nativeOutput, int destinations) {
    List<List<Object[]>> expected = emptyDestinations(destinations);
    List<List<Object[]>> actual = emptyDestinations(destinations);
    List<MseBlock.Data> legacySent = new ArrayList<>();
    List<MseBlock.Data> nativeSent = new ArrayList<>();
    BlockExchange.getExchange(capturingMailboxes(expected, legacySent, false), RelDistribution.Type.HASH_DISTRIBUTED,
        keys, BlockSplitter.DEFAULT, algorithm).send(new RowHeapDataBlock(rows, SCHEMA));
    BlockExchange exchange = BlockExchange.getExchange(capturingMailboxes(actual, nativeSent, nativeOutput),
        RelDistribution.Type.HASH_DISTRIBUTED, keys, BlockSplitter.DEFAULT, mailboxes -> 0, algorithm,
        context == null ? null : () -> context);
    exchange.send(source);
    assertEquals(nativeSent.size(), legacySent.size());
    for (int destination = 0; destination < destinations; destination++) {
      assertRows(actual.get(destination), expected.get(destination));
    }
    return nativeSent;
  }

  private static List<SendingMailbox> capturingMailboxes(List<List<Object[]>> destinations, List<MseBlock.Data> sent,
      boolean arrow) {
    List<SendingMailbox> mailboxes = new ArrayList<>();
    for (List<Object[]> rows : destinations) {
      SendingMailbox mailbox = mock(SendingMailbox.class);
      when(mailbox.isLocal()).thenReturn(true);
      doAnswer(invocation -> {
        MseBlock.Data block = invocation.getArgument(0);
        assertEquals(block.isArrow(), arrow);
        assertSame(block.getDataSchema(), SCHEMA);
        if (arrow) {
          for (FieldVector vector : block.asArrow().getDataBlock().getRoot().getFieldVectors()) {
            assertEquals(vector.getValueCount(), block.getNumRows());
          }
        }
        rows.addAll(block.asRowHeap().getRows());
        sent.add(block);
        return null;
      }).when(mailbox).send(any(MseBlock.Data.class));
      mailboxes.add(mailbox);
    }
    return mailboxes;
  }

  private static List<List<Object[]>> emptyDestinations(int count) {
    List<List<Object[]>> destinations = new ArrayList<>();
    for (int destination = 0; destination < count; destination++) {
      destinations.add(new ArrayList<>());
    }
    return destinations;
  }

  private static void assertRows(List<Object[]> actual, List<Object[]> expected) {
    assertEquals(actual.size(), expected.size());
    for (int row = 0; row < actual.size(); row++) {
      assertEquals(actual.get(row), expected.get(row), "row " + row);
      if (expected.get(row)[3] != null) {
        assertEquals(Float.floatToRawIntBits((Float) actual.get(row)[3]),
            Float.floatToRawIntBits((Float) expected.get(row)[3]));
      }
      if (expected.get(row)[4] != null) {
        assertEquals(Double.doubleToRawLongBits((Double) actual.get(row)[4]),
            Double.doubleToRawLongBits((Double) expected.get(row)[4]));
      }
    }
  }

  private static ArrowBuffers newBuffers() {
    return new ArrowBuffers(true, new RootAllocator(32 * 1024 * 1024), 0, 16 * 1024 * 1024);
  }

  private static ArrowQueryContext newContext(ArrowBuffers buffers, int workerId) {
    return buffers.newQueryContext("hash-exchange-" + workerId);
  }

  private static List<Object[]> rows(int count) {
    Integer[] ints = {null, 0, 1, -1, Integer.MIN_VALUE, Integer.MAX_VALUE, 0x40000000, -1234567};
    Long[] longs = {0L, null, 1L, -1L, Long.MIN_VALUE, Long.MAX_VALUE, 0x100000001L, 0x123456789abcdef0L};
    Float[] floats = {0F, -0F, null, 1F, -1F, Float.MIN_VALUE, Float.MIN_NORMAL, Float.MAX_VALUE,
        Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, Float.NaN, Float.intBitsToFloat(0xffc01234)};
    Double[] doubles = {0D, -0D, 1D, null, -1D, Double.MIN_VALUE, Double.MIN_NORMAL, Double.MAX_VALUE,
        Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NaN, Double.longBitsToDouble(0xfff8123456789abcL)};
    List<Object[]> rows = new ArrayList<>(count);
    for (int row = 0; row < count; row++) {
      rows.add(new Object[]{row, ints[row % ints.length], longs[row % longs.length], floats[row % floats.length],
          doubles[row % doubles.length], row % 3 == 0 ? null : row % 2, longs[(row + 3) % longs.length],
          row % 5 == 0 ? null : STRINGS[row % STRINGS.length], row % 7 == 0 ? null : JSON[row % JSON.length],
          row % 4 == 0 ? null : "plain-\u20ac-" + row,
          row % 5 == 0 ? null : new ByteArray(row % 3 == 0 ? new byte[0] : new byte[]{0, -1, (byte) row})});
    }
    return rows;
  }

  private static ArrowBlock newBlock(BufferAllocator allocator, List<Object[]> rows, boolean dictionaries) {
    List<Field> fields = new ArrayList<>();
    for (int col = 0; col < SCHEMA.size(); col++) {
      ArrowType type = switch (SCHEMA.getColumnDataType(col)) {
        case INT -> new ArrowType.Int(32, true);
        case LONG, TIMESTAMP -> new ArrowType.Int(64, true);
        case FLOAT -> new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
        case DOUBLE -> new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        case BOOLEAN -> new ArrowType.Bool();
        case BYTES -> new ArrowType.Binary();
        default -> new ArrowType.Utf8();
      };
      if (dictionaries && (col == 7 || col == 8)) {
        DictionaryEncoding encoding = new DictionaryEncoding(col == 7 ? STRING_DICTIONARY_ID : JSON_DICTIONARY_ID,
            false, new ArrowType.Int(32, true));
        fields.add(new Field(SCHEMA.getColumnName(col),
            new FieldType(true, new ArrowType.Int(32, true), encoding), null));
      } else {
        fields.add(Field.nullable(SCHEMA.getColumnName(col), type));
      }
    }
    VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(fields), allocator);
    MapDictionaryProvider provider = dictionaries ? new MapDictionaryProvider() : null;
    boolean success = false;
    try {
      if (dictionaries) {
        for (int col : new int[]{7, 8}) {
          VarCharVector dictionary = new VarCharVector("dictionary-" + col, allocator);
          provider.put(new Dictionary(dictionary, fields.get(col).getDictionary()));
          String[] values = col == 7 ? STRINGS : JSON;
          dictionary.allocateNew();
          for (int row = 0; row < values.length; row++) {
            dictionary.setSafe(row, values[row].getBytes(StandardCharsets.UTF_8));
          }
          dictionary.setValueCount(values.length);
        }
      }
      for (int col = 0; col < SCHEMA.size(); col++) {
        FieldVector vector = root.getVector(col);
        vector.setInitialCapacity(rows.size());
        vector.allocateNew();
        for (int row = 0; row < rows.size(); row++) {
          Object value = rows.get(row)[col];
          if (value == null) {
            vector.setNull(row);
          } else if (dictionaries && (col == 7 || col == 8)) {
            ((IntVector) vector).setSafe(row, row % STRINGS.length);
          } else {
            switch (SCHEMA.getColumnDataType(col)) {
              case INT -> ((IntVector) vector).setSafe(row, (Integer) value);
              case LONG, TIMESTAMP -> ((BigIntVector) vector).setSafe(row, (Long) value);
              case FLOAT -> ((Float4Vector) vector).setSafe(row, (Float) value);
              case DOUBLE -> ((Float8Vector) vector).setSafe(row, (Double) value);
              case BOOLEAN -> ((BitVector) vector).setSafe(row, (Integer) value);
              case BYTES -> ((VarBinaryVector) vector).setSafe(row, ((ByteArray) value).getBytes());
              default -> ((VarCharVector) vector).setSafe(row, ((String) value).getBytes(StandardCharsets.UTF_8));
            }
          }
        }
        vector.setValueCount(rows.size());
      }
      root.setRowCount(rows.size());
      ArrowBlock block = new ArrowBlock(new ArrowDataBlock(root, SCHEMA, provider));
      success = true;
      return block;
    } finally {
      if (!success) {
        try {
          root.close();
        } finally {
          if (provider != null) {
            provider.close();
          }
        }
      }
    }
  }
}
