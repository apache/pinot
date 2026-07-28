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
package org.apache.pinot.core.operator.blocks;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.core.common.DataBlockCache;
import org.apache.pinot.core.common.DataFetcher;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.datasource.OpenStructDataSource;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/**
 * Covers projection over an OPEN_STRUCT column, where the parent data source carries no readers of its own and every
 * value is reached through a per-key child source that {@link ProjectionBlock#getBlockValueSet(String[])} registers
 * lazily. Also exercises the matching {@link DataFetcher} registration skip, since the two halves only make sense
 * together.
 */
public class ProjectionBlockOpenStructTest {
  private static final String OPEN_STRUCT_COLUMN = "metrics";
  private static final String PLAIN_COLUMN = "ts";
  private static final String KEY = "errors";

  /** OPEN_STRUCT parent: an empty index container, so {@code getForwardIndex()} is null. */
  private static OpenStructDataSource mockOpenStructDataSource() {
    return mock(OpenStructDataSource.class);
  }

  private static DataSource mockPlainDataSource() {
    DataSource dataSource = mock(DataSource.class);
    ForwardIndexReader<?> forwardIndex = mock(ForwardIndexReader.class);
    when(forwardIndex.isDictionaryEncoded()).thenReturn(false);
    doReturn(forwardIndex).when(dataSource).getForwardIndex();
    DataSourceMetadata metadata = mock(DataSourceMetadata.class);
    when(metadata.isSingleValue()).thenReturn(true);
    when(metadata.getDataType()).thenReturn(FieldSpec.DataType.LONG);
    when(dataSource.getDataSourceMetadata()).thenReturn(metadata);
    return dataSource;
  }

  /**
   * Regression: registering the OPEN_STRUCT parent with the DataFetcher tripped its forward-index precondition, so
   * every {@code SELECT item(metrics, 'key')} failed at ProjectionOperator construction with
   * "Forward index disabled for column: metrics, cannot create DataFetcher!".
   */
  @Test
  public void testDataFetcherSkipsOpenStructParent() {
    Map<String, DataSource> dataSourceMap = new HashMap<>();
    dataSourceMap.put(OPEN_STRUCT_COLUMN, mockOpenStructDataSource());
    dataSourceMap.put(PLAIN_COLUMN, mockPlainDataSource());

    new DataFetcher(dataSourceMap, Map.of()).close();
  }

  /**
   * The skip is scoped to OPEN_STRUCT parents — an ordinary column with a disabled forward index must still be
   * rejected rather than silently reading nothing.
   */
  @Test
  public void testDataFetcherStillRejectsForwardIndexDisabledColumn() {
    DataSource dataSource = mock(DataSource.class);
    when(dataSource.getForwardIndex()).thenReturn(null);

    assertThrows(IllegalStateException.class,
        () -> new DataFetcher(Map.of(PLAIN_COLUMN, dataSource), Map.of()));
  }

  /**
   * The parent is skipped, but the per-key child source it resolves is registered on first use, so the key remains
   * readable.
   */
  @Test
  public void testPerKeyDataSourceRegisteredOnFirstUse() {
    OpenStructDataSource openStructDataSource = mockOpenStructDataSource();
    DataSource keyDataSource = mockPlainDataSource();
    when(openStructDataSource.getDataSource(KEY)).thenReturn(keyDataSource);

    Map<String, DataSource> dataSourceMap = new HashMap<>();
    dataSourceMap.put(OPEN_STRUCT_COLUMN, openStructDataSource);
    DataBlockCache dataBlockCache = new DataBlockCache(new DataFetcher(dataSourceMap, Map.of()));

    ProjectionBlock projectionBlock = new ProjectionBlock(dataSourceMap, dataBlockCache);
    assertNotNull(projectionBlock.getBlockValueSet(new String[]{OPEN_STRUCT_COLUMN, KEY}));
  }

  /**
   * ProjectionBlock re-resolves the per-key source on every block. Registration must be idempotent, otherwise each
   * block orphans the displaced ColumnValueReader along with its off-heap reader context.
   */
  @Test
  public void testRegisteringSameColumnTwiceKeepsFirstReader() {
    DataFetcher dataFetcher = new DataFetcher(Map.of(), Map.of());
    DataSource first = mockPlainDataSource();
    DataSource second = mockPlainDataSource();

    dataFetcher.addDataSource(PLAIN_COLUMN, first);
    dataFetcher.addDataSource(PLAIN_COLUMN, second);

    verify(second, never()).getForwardIndex();
    dataFetcher.close();
  }

  /**
   * Selecting the parent column itself is not supported. It must fail as a bad request naming the column rather than
   * an NPE from the reader the DataFetcher never registered.
   */
  @Test
  public void testSelectingOpenStructParentFailsWithClearMessage() {
    Map<String, DataSource> dataSourceMap = new HashMap<>();
    dataSourceMap.put(OPEN_STRUCT_COLUMN, mockOpenStructDataSource());
    ProjectionBlock projectionBlock =
        new ProjectionBlock(dataSourceMap, new DataBlockCache(new DataFetcher(dataSourceMap, Map.of())));

    BadQueryRequestException e =
        expectThrows(BadQueryRequestException.class, () -> projectionBlock.getBlockValueSet(OPEN_STRUCT_COLUMN));
    assertTrue(e.getMessage().contains(OPEN_STRUCT_COLUMN), e.getMessage());
  }
}
