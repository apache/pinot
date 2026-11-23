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

import java.util.Map;
import org.apache.pinot.core.operator.dociditerators.MVScanDocIdIterator;
import org.apache.pinot.core.operator.dociditerators.SVScanDocIdIterator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


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

  /// Returns a data source of a raw INT column whose forward index creates the given reader context.
  private static DataSource mockDataSource(ForwardIndexReaderContext readerContext, boolean singleValue) {
    ForwardIndexReader<?> forwardIndexReader = mock(ForwardIndexReader.class);
    when(forwardIndexReader.isDictionaryEncoded()).thenReturn(false);
    when(forwardIndexReader.isSingleValue()).thenReturn(singleValue);
    when(forwardIndexReader.getStoredType()).thenReturn(DataType.INT);
    when(forwardIndexReader.createContext(any())).thenReturn((ForwardIndexReaderContext) readerContext);
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
