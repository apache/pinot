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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.DataBlockCache;
import org.apache.pinot.core.common.DataFetcher;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.datasource.OpenStructDataSource;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Covers projection over an OPEN_STRUCT column, where the parent data source carries no readers of its own and every
/// value is reached through a per-key child source that {@link ProjectionBlock#getBlockValueSet(String[])} registers
/// lazily. Also exercises the matching {@link DataFetcher} registration skip, since the two halves only make sense
/// together.
public class ProjectionBlockOpenStructTest {
  private static final String OPEN_STRUCT_COLUMN = "metrics";
  private static final String PLAIN_COLUMN = "ts";
  private static final String KEY = "errors";

  /// OPEN_STRUCT parent: an empty index container, so {@code getForwardIndex()} is null.
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

  /// Regression: registering the OPEN_STRUCT parent with the DataFetcher tripped its forward-index precondition, so
  /// every {@code SELECT metrics['key']} failed at ProjectionOperator construction with
  /// "Forward index disabled for column: metrics, cannot create DataFetcher!".
  @Test
  public void testDataFetcherSkipsOpenStructParent() {
    Map<String, DataSource> dataSourceMap = new HashMap<>();
    dataSourceMap.put(OPEN_STRUCT_COLUMN, mockOpenStructDataSource());
    dataSourceMap.put(PLAIN_COLUMN, mockPlainDataSource());

    new DataFetcher(dataSourceMap, Map.of()).close();
  }

  /// The skip is scoped to OPEN_STRUCT parents — an ordinary column with a disabled forward index must still be
  /// rejected rather than silently reading nothing.
  @Test
  public void testDataFetcherStillRejectsForwardIndexDisabledColumn() {
    DataSource dataSource = mock(DataSource.class);
    when(dataSource.getForwardIndex()).thenReturn(null);

    assertThrows(IllegalStateException.class,
        () -> new DataFetcher(Map.of(PLAIN_COLUMN, dataSource), Map.of()));
  }

  /// The parent is skipped, but the per-key child source it resolves is registered on first use, so the key remains
  /// readable.
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

  /// ProjectionBlock re-resolves the per-key source on every block. Registration must be idempotent, otherwise each
  /// block orphans the displaced ColumnValueReader along with its off-heap reader context.
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

  /// Selecting the parent column reads the whole struct as a JSON document. It used to throw, which also took out
  /// `SELECT *` on any table carrying an OPEN_STRUCT column — the first query anyone runs.
  @Test
  public void testSelectingOpenStructParentReadsTheWholeDocument() {
    BlockValSet values = project(Map.of("errors", 3L));
    assertEquals(values.getValueType(), FieldSpec.DataType.STRING);
    assertTrue(values.isSingleValue());
    assertEquals(values.getStringValuesSV()[0], "{\"errors\":3}");
  }

  /// A key that lives only in the sparse blob has no DataSource of its own, so walking
  /// {@link OpenStructDataSource#getDataSources()} would drop it from the document without a word. The document has
  /// to come from the reconstruction the data source owns.
  @Test
  public void testSparseOnlyKeyIsInTheDocument() {
    assertEquals(project(Map.of("sparseOnly", "kept")).getStringValuesSV()[0], "{\"sparseOnly\":\"kept\"}");
  }

  /// A nested object reads back nested, the shape the source had, rather than as the flat dotted paths its leaves
  /// are materialized under.
  @Test
  public void testNestedObjectReadsBackNested() {
    Map<String, Object> document = new LinkedHashMap<>();
    document.put("configApi.timeTaken", 106.0);
    document.put("configApi.message", "success_v2");
    document.put("configApi", new LinkedHashMap<>(Map.of("message", "success_v2")));
    document.put("device_os", "android");

    assertEquals(project(document).getStringValuesSV()[0],
        "{\"configApi\":{\"message\":\"success_v2\"},\"device_os\":\"android\"}");
  }

  /// `.` is an ordinary key character with no escape, so a dotted key whose prefix is not itself an object of this
  /// document was never a path and must stay a key spelled with a dot.
  @Test
  public void testLiteralDottedKeyIsNotSplit() {
    assertEquals(project(Map.of("a.b", 1)).getStringValuesSV()[0], "{\"a.b\":1}");
  }

  /// A prefix that is a key but not an object leaves the dotted key alone: only an object can be the thing the path
  /// was a path into.
  @Test
  public void testDottedKeyWhosePrefixIsAScalarIsNotSplit() {
    Map<String, Object> document = new LinkedHashMap<>();
    document.put("a", 1);
    document.put("a.b", 2);

    assertEquals(project(document).getStringValuesSV()[0], "{\"a\":1,\"a.b\":2}");
  }

  /// Nesting is cleaned up at every level, not only the top one.
  @Test
  public void testNestingIsResolvedInsideNestedObjects() {
    Map<String, Object> inner = new LinkedHashMap<>();
    inner.put("b.c", 1);
    inner.put("b", new LinkedHashMap<>(Map.of("c", 2)));

    assertEquals(project(Map.of("a", inner)).getStringValuesSV()[0], "{\"a\":{\"b\":{\"c\":2}}}");
  }

  /// BYTES reads as hex, the way every other path out of Pinot renders it — Jackson would have base64'd it.
  @Test
  public void testBytesRenderAsHex() {
    assertEquals(project(Map.of("raw", new byte[]{0x0a, (byte) 0xff})).getStringValuesSV()[0], "{\"raw\":\"0aff\"}");
  }

  /// A row with no keys at all is an empty document, not a null.
  @Test
  public void testRowWithNoKeysIsAnEmptyDocument() {
    OpenStructDataSource openStruct = mockOpenStructDataSource();
    when(openStruct.openMapValueReader()).thenReturn(docId -> null);
    assertEquals(projectWith(openStruct).getStringValuesSV()[0], "{}");
  }

  /// Arrays keep their element order and are rendered element by element.
  @Test
  public void testListValuesAreRendered() {
    assertEquals(project(Map.of("tags", List.of("a", "b"))).getStringValuesSV()[0], "{\"tags\":[\"a\",\"b\"]}");
  }

  /// Projects a single-row block whose OPEN_STRUCT column reconstructs to `document`.
  private static BlockValSet project(Map<String, Object> document) {
    OpenStructDataSource openStruct = mockOpenStructDataSource();
    when(openStruct.openMapValueReader()).thenReturn(docId -> document);
    return projectWith(openStruct);
  }

  private static BlockValSet projectWith(OpenStructDataSource openStruct) {
    Map<String, DataSource> dataSourceMap = new HashMap<>();
    dataSourceMap.put(OPEN_STRUCT_COLUMN, openStruct);
    DataBlockCache dataBlockCache = new DataBlockCache(new DataFetcher(dataSourceMap, Map.of()));
    dataBlockCache.initNewBlock(new int[]{0}, 1);
    return new ProjectionBlock(dataSourceMap, dataBlockCache).getBlockValueSet(OPEN_STRUCT_COLUMN);
  }
}
