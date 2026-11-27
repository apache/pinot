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
package org.apache.pinot.core.common;

import java.io.File;
import java.nio.file.Files;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.operator.dociditerators.MVScanDocIdIterator;
import org.apache.pinot.core.operator.dociditerators.SVScanDocIdIterator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.segment.local.io.writer.impl.FixedByteChunkForwardIndexWriter;
import org.apache.pinot.segment.local.segment.index.readers.forward.ChunkReaderContext;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedBytePower2ChunkSVForwardIndexReader;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


/// Verifies that the reader contexts created during query execution are released when the query task ends before the
/// owner reads to the end (timeout, cancellation, exception or early termination).
public class ReaderContextReleaseTest {
  private static final String COLUMN = "col";
  private static final int NUM_DOCS = 10;

  @Test
  public void testDataFetcherContextReleasedWhenAbandoned() {
    ForwardIndexReaderContext readerContext = mock(ForwardIndexReaderContext.class);
    try (QueryThreadContext ignore = QueryThreadContext.openForSseTest()) {
      DataFetcher dataFetcher = new DataFetcher(Map.of(COLUMN, mockDataSource(readerContext, true)), Map.of());
      dataFetcher.fetchIntValues(COLUMN, new int[]{0, 1}, 2, new int[2]);
      // The projection stops before EOF, so nothing closes the DataFetcher
      verify(readerContext, never()).close();
    }
    verify(readerContext, times(1)).close();
  }

  @Test
  public void testDataFetcherContextClosedOnceWhenClosedByOwner() {
    ForwardIndexReaderContext readerContext = mock(ForwardIndexReaderContext.class);
    try (QueryThreadContext ignore = QueryThreadContext.openForSseTest()) {
      DataFetcher dataFetcher = new DataFetcher(Map.of(COLUMN, mockDataSource(readerContext, true)), Map.of());
      dataFetcher.fetchIntValues(COLUMN, new int[]{0, 1}, 2, new int[2]);
      dataFetcher.close();
      verify(readerContext, times(1)).close();
    }
    verify(readerContext, times(1)).close();
  }

  @Test
  public void testSVScanContextReleasedWhenAbandoned() {
    ForwardIndexReaderContext readerContext = mock(ForwardIndexReaderContext.class);
    try (QueryThreadContext ignore = QueryThreadContext.openForSseTest()) {
      new SVScanDocIdIterator(mock(PredicateEvaluator.class), mockDataSource(readerContext, true), NUM_DOCS,
          BlockDocIdIterator.OPTIMAL_ITERATOR_BATCH_SIZE, Map.of());
      verify(readerContext, never()).close();
    }
    verify(readerContext, times(1)).close();
  }

  @Test
  public void testSVScanContextClosedOnceAtEof() {
    ForwardIndexReaderContext readerContext = mock(ForwardIndexReaderContext.class);
    try (QueryThreadContext ignore = QueryThreadContext.openForSseTest()) {
      // The mocked predicate evaluator matches no document, so the first next() scans to EOF
      SVScanDocIdIterator iterator =
          new SVScanDocIdIterator(mock(PredicateEvaluator.class), mockDataSource(readerContext, true), NUM_DOCS,
              BlockDocIdIterator.OPTIMAL_ITERATOR_BATCH_SIZE, Map.of());
      assertEquals(iterator.next(), Constants.EOF);
      verify(readerContext, times(1)).close();
    }
    verify(readerContext, times(1)).close();
  }

  @Test
  public void testMVScanContextReleasedWhenAbandoned() {
    ForwardIndexReaderContext readerContext = mock(ForwardIndexReaderContext.class);
    try (QueryThreadContext ignore = QueryThreadContext.openForSseTest()) {
      new MVScanDocIdIterator(mock(PredicateEvaluator.class), mockDataSource(readerContext, false), NUM_DOCS,
          Map.of());
      verify(readerContext, never()).close();
    }
    verify(readerContext, times(1)).close();
  }

  @Test
  public void testMVScanContextClosedOnceAtEof() {
    ForwardIndexReaderContext readerContext = mock(ForwardIndexReaderContext.class);
    try (QueryThreadContext ignore = QueryThreadContext.openForSseTest()) {
      // The mocked predicate evaluator matches no document, so the first next() scans to EOF
      MVScanDocIdIterator iterator =
          new MVScanDocIdIterator(mock(PredicateEvaluator.class), mockDataSource(readerContext, false), NUM_DOCS,
              Map.of());
      assertEquals(iterator.next(), Constants.EOF);
      verify(readerContext, times(1)).close();
    }
    verify(readerContext, times(1)).close();
  }

  @Test
  public void testDataFetcherOnlyReleasesCreatedContexts() {
    ForwardIndexReaderContext readContext = mock(ForwardIndexReaderContext.class);
    ForwardIndexReaderContext unreadContext = mock(ForwardIndexReaderContext.class);
    DataSource unreadDataSource = mockDataSource(unreadContext, true);
    try (QueryThreadContext ignore = QueryThreadContext.openForSseTest()) {
      DataFetcher dataFetcher = new DataFetcher(
          Map.of(COLUMN, mockDataSource(readContext, true), "unread", unreadDataSource), Map.of());
      dataFetcher.fetchIntValues(COLUMN, new int[]{0, 1}, 2, new int[2]);
    }
    verify(readContext, times(1)).close();
    // The DataFetcher creates a reader context only when it reads the column
    verify(unreadDataSource.getForwardIndex(), never()).createContext(any());
    verify(unreadContext, never()).close();
  }

  @Test
  public void testOwnersWorkWithoutQueryThreadContext() {
    ForwardIndexReaderContext readerContext = mock(ForwardIndexReaderContext.class);
    DataFetcher dataFetcher = new DataFetcher(Map.of(COLUMN, mockDataSource(readerContext, true)), Map.of());
    dataFetcher.fetchIntValues(COLUMN, new int[]{0, 1}, 2, new int[2]);
    new SVScanDocIdIterator(mock(PredicateEvaluator.class), mockDataSource(readerContext, true), NUM_DOCS,
        BlockDocIdIterator.OPTIMAL_ITERATOR_BATCH_SIZE, Map.of());
    new MVScanDocIdIterator(mock(PredicateEvaluator.class), mockDataSource(readerContext, false), NUM_DOCS,
        Map.of());
    // Without a QueryThreadContext nothing tracks the contexts. Only the owners or the garbage collector release them.
    verify(readerContext, never()).close();
    dataFetcher.close();
    verify(readerContext, times(1)).close();
  }

  /// Uses a real LZ4-compressed raw forward index, whose [ChunkReaderContext] holds a direct chunk buffer.
  @Test
  public void testRealChunkContextReleasedWhenAbandoned()
      throws Exception {
    File indexDir = Files.createTempDirectory("ReaderContextReleaseTest").toFile();
    File indexFile = new File(indexDir, COLUMN + ".sv.raw.fwd");
    try {
      try (FixedByteChunkForwardIndexWriter writer = new FixedByteChunkForwardIndexWriter(indexFile,
          ChunkCompressionType.LZ4, NUM_DOCS, 4, Integer.BYTES, 4)) {
        for (int i = 0; i < NUM_DOCS; i++) {
          writer.putInt(i * 10);
        }
      }
      try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(indexFile);
          FixedBytePower2ChunkSVForwardIndexReader reader =
              spy(new FixedBytePower2ChunkSVForwardIndexReader(buffer, DataType.INT))) {
        // Spy each real context that the reader creates, to count its close() calls
        AtomicReference<ChunkReaderContext> createdContext = new AtomicReference<>();
        doAnswer(invocation -> {
          ChunkReaderContext context = spy((ChunkReaderContext) invocation.callRealMethod());
          createdContext.set(context);
          return context;
        }).when(reader).createContext();
        DataSource dataSource = mockDataSource(reader, true);

        int[] values = new int[2];
        try (QueryThreadContext ignore = QueryThreadContext.openForSseTest()) {
          DataFetcher dataFetcher = new DataFetcher(Map.of(COLUMN, dataSource), Map.of());
          dataFetcher.fetchIntValues(COLUMN, new int[]{1, 5}, 2, values);
          assertEquals(values, new int[]{10, 50});
          assertNotNull(createdContext.get());
          // The projection stops before EOF, so nothing closes the DataFetcher
          verify(createdContext.get(), never()).close();
        }
        verify(createdContext.get(), times(1)).close();

        // An owner that closes after the registry already did must not fail. ChunkReaderContext.close() is idempotent.
        SVScanDocIdIterator iterator;
        try (QueryThreadContext ignore = QueryThreadContext.openForSseTest()) {
          iterator = new SVScanDocIdIterator(mock(PredicateEvaluator.class), dataSource, NUM_DOCS,
              BlockDocIdIterator.OPTIMAL_ITERATOR_BATCH_SIZE, Map.of());
        }
        verify(createdContext.get(), times(1)).close();
        iterator.close();
        verify(createdContext.get(), times(2)).close();
      }
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  /// Returns a data source of a raw INT column whose forward index creates the given reader context.
  private static DataSource mockDataSource(ForwardIndexReaderContext readerContext, boolean singleValue) {
    ForwardIndexReader<?> forwardIndexReader = mock(ForwardIndexReader.class);
    when(forwardIndexReader.isDictionaryEncoded()).thenReturn(false);
    when(forwardIndexReader.isSingleValue()).thenReturn(singleValue);
    when(forwardIndexReader.getStoredType()).thenReturn(DataType.INT);
    when(forwardIndexReader.createContext(any())).thenReturn((ForwardIndexReaderContext) readerContext);
    return mockDataSource(forwardIndexReader, singleValue);
  }

  /// Returns a data source of a raw INT column with the given forward index.
  private static DataSource mockDataSource(ForwardIndexReader<?> forwardIndexReader, boolean singleValue) {
    DataSourceMetadata dataSourceMetadata = mock(DataSourceMetadata.class);
    when(dataSourceMetadata.isSingleValue()).thenReturn(singleValue);
    when(dataSourceMetadata.getMaxNumValuesPerMVEntry()).thenReturn(singleValue ? -1 : 2);
    when(dataSourceMetadata.getCardinality()).thenReturn(NUM_DOCS);
    DataSource dataSource = mock(DataSource.class);
    when(dataSource.getForwardIndex()).thenReturn((ForwardIndexReader) forwardIndexReader);
    when(dataSource.getDataSourceMetadata()).thenReturn(dataSourceMetadata);
    return dataSource;
  }
}
