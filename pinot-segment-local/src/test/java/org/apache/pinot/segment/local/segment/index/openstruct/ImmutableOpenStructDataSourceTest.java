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
package org.apache.pinot.segment.local.segment.index.openstruct;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


public class ImmutableOpenStructDataSourceTest {

  private static ComplexFieldSpec openStructSpec(String name) {
    ComplexFieldSpec spec = new ComplexFieldSpec(name, DataType.OPEN_STRUCT, true, Map.of());
    return spec;
  }

  @Test
  public void testGetDataSourceReturnsPerKeyDataSource() {
    DataSource clicksDs = mock(DataSource.class);
    DataSource sparseDs = mock(DataSource.class);
    DataSourceMetadata meta = mock(DataSourceMetadata.class);
    ColumnIndexContainer container = mock(ColumnIndexContainer.class);

    ImmutableOpenStructDataSource ds = new ImmutableOpenStructDataSource(
        openStructSpec("event"),
        Map.of("clicks", clicksDs),
        sparseDs,
        meta,
        container);

    assertSame(ds.getDataSource("clicks"), clicksDs);
    // absent key falls back to sparse
    assertSame(ds.getDataSource("unknown"), sparseDs);
  }

  @Test
  public void testIsMaterializedTrueOnlyForMaterializedKeys() {
    DataSource clicksDs = mock(DataSource.class);
    DataSourceMetadata meta = mock(DataSourceMetadata.class);
    ColumnIndexContainer container = mock(ColumnIndexContainer.class);

    ImmutableOpenStructDataSource ds = new ImmutableOpenStructDataSource(
        openStructSpec("event"),
        Map.of("clicks", clicksDs),
        null,
        meta,
        container);

    assertTrue(ds.isMaterialized("clicks"));
    assertFalse(ds.isMaterialized("absent"));
  }

  @Test
  public void testIsFullyMaterializedTrueWhenNoSparse() {
    DataSourceMetadata meta = mock(DataSourceMetadata.class);
    ColumnIndexContainer container = mock(ColumnIndexContainer.class);

    ImmutableOpenStructDataSource ds = new ImmutableOpenStructDataSource(
        openStructSpec("event"),
        Map.of(),
        null,
        meta,
        container);

    assertTrue(ds.isFullyMaterialized());
  }

  @Test
  public void testIsFullyMaterializedFalseWhenSparsePresent() {
    DataSource sparseDs = mock(DataSource.class);
    DataSourceMetadata meta = mock(DataSourceMetadata.class);
    ColumnIndexContainer container = mock(ColumnIndexContainer.class);

    ImmutableOpenStructDataSource ds = new ImmutableOpenStructDataSource(
        openStructSpec("event"),
        Map.of(),
        sparseDs,
        meta,
        container);

    assertFalse(ds.isFullyMaterialized());
  }

  @Test
  public void testGetFieldSpecReturnsOpenStructView() {
    DataSourceMetadata meta = mock(DataSourceMetadata.class);
    ColumnIndexContainer container = mock(ColumnIndexContainer.class);

    ImmutableOpenStructDataSource ds = new ImmutableOpenStructDataSource(
        openStructSpec("event"),
        Map.of(),
        null,
        meta,
        container);

    ComplexFieldSpec fieldSpec = ds.getFieldSpec();
    assertNotNull(fieldSpec);
    assertEquals(fieldSpec.getName(), "event");
  }

  @Test
  public void testGetDataSourceMetadataByKeyReturnsDelegated() {
    DataSource clicksDs = mock(DataSource.class);
    DataSourceMetadata clicksMeta = mock(DataSourceMetadata.class);
    DataSourceMetadata topMeta = mock(DataSourceMetadata.class);
    ColumnIndexContainer container = mock(ColumnIndexContainer.class);

    org.mockito.Mockito.when(clicksDs.getDataSourceMetadata()).thenReturn(clicksMeta);

    ImmutableOpenStructDataSource ds = new ImmutableOpenStructDataSource(
        openStructSpec("event"),
        Map.of("clicks", clicksDs),
        null,
        topMeta,
        container);

    assertSame(ds.getDataSourceMetadata("clicks"), clicksMeta);
    assertNull(ds.getDataSourceMetadata("absent"));
  }

  @Test
  public void testGetDataSourcesReturnsPerKeyMap() {
    DataSource clicksDs = mock(DataSource.class);
    DataSourceMetadata meta = mock(DataSourceMetadata.class);
    ColumnIndexContainer container = mock(ColumnIndexContainer.class);
    Map<String, DataSource> perKeyMap = Map.of("clicks", clicksDs);

    ImmutableOpenStructDataSource ds = new ImmutableOpenStructDataSource(
        openStructSpec("event"),
        perKeyMap,
        null,
        meta,
        container);

    assertEquals(ds.getDataSources(), perKeyMap);
  }

  @Test
  public void testTopLevelMetadataAndContainerDelegated() {
    DataSourceMetadata meta = mock(DataSourceMetadata.class);
    ColumnIndexContainer container = mock(ColumnIndexContainer.class);

    ImmutableOpenStructDataSource ds = new ImmutableOpenStructDataSource(
        openStructSpec("event"),
        Map.of(),
        null,
        meta,
        container);

    assertSame(ds.getDataSourceMetadata(), meta);
    assertSame(ds.getIndexContainer(), container);
  }

  @Test
  public void testConvenienceConstructorSynthesizesMetadata() {
    DataSource clicksDs = mock(DataSource.class);
    ImmutableOpenStructDataSource ds = new ImmutableOpenStructDataSource(
        openStructSpec("event"),
        Map.of("clicks", clicksDs),
        null,
        42);

    DataSourceMetadata meta = ds.getDataSourceMetadata();
    assertNotNull(meta);
    assertEquals(meta.getNumDocs(), 42);
    assertEquals(meta.getNumValues(), 42);
    assertEquals(meta.getFieldSpec().getDataType(), DataType.OPEN_STRUCT);
    assertNotNull(ds.getIndexContainer());
    assertTrue(ds.isFullyMaterialized());
    assertSame(ds.getDataSource("clicks"), clicksDs);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static DataSource mockDenseDataSource(DataType storedType, Object valueAtDoc0, boolean nullAtDoc1) {
    DataSource ds = mock(DataSource.class);
    ForwardIndexReader fwdReader = mock(ForwardIndexReader.class);
    ForwardIndexReaderContext ctx = mock(ForwardIndexReaderContext.class);
    when(fwdReader.createContext()).thenReturn(ctx);
    when(fwdReader.getStoredType()).thenReturn(storedType);

    Dictionary dictionary = mock(Dictionary.class);
    when(fwdReader.getDictId(eq(0), eq(ctx))).thenReturn(42);
    when(dictionary.get(42)).thenReturn(valueAtDoc0);
    when(ds.getDictionary()).thenReturn(dictionary);

    when(ds.getForwardIndex()).thenReturn(fwdReader);

    NullValueVectorReader nullReader = mock(NullValueVectorReader.class);
    when(nullReader.isNull(0)).thenReturn(false);
    when(nullReader.isNull(1)).thenReturn(nullAtDoc1);
    when(ds.getNullValueVector()).thenReturn(nullReader);

    return ds;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static DataSource mockSparseDataSource(String jsonAtDoc0, boolean nullAtDoc1) {
    DataSource ds = mock(DataSource.class);
    ForwardIndexReader fwdReader = mock(ForwardIndexReader.class);
    ForwardIndexReaderContext ctx = mock(ForwardIndexReaderContext.class);
    when(fwdReader.createContext()).thenReturn(ctx);
    when(fwdReader.getStoredType()).thenReturn(DataType.STRING);
    when(fwdReader.getString(eq(0), eq(ctx))).thenReturn(jsonAtDoc0);
    when(fwdReader.getString(eq(1), eq(ctx))).thenReturn("");
    when(ds.getForwardIndex()).thenReturn(fwdReader);
    when(ds.getDictionary()).thenReturn(null);

    NullValueVectorReader nullReader = mock(NullValueVectorReader.class);
    when(nullReader.isNull(0)).thenReturn(false);
    when(nullReader.isNull(1)).thenReturn(nullAtDoc1);
    when(ds.getNullValueVector()).thenReturn(nullReader);

    return ds;
  }

  @Test
  public void testGetMapValueDenseOnly() {
    DataSource clicksDs = mockDenseDataSource(DataType.INT, 10, true);
    DataSource nameDs = mockDenseDataSource(DataType.STRING, "hello", false);

    Map<String, DataSource> perKey = new HashMap<>();
    perKey.put("clicks", clicksDs);
    perKey.put("name", nameDs);

    ImmutableOpenStructDataSource ds = new ImmutableOpenStructDataSource(
        openStructSpec("event"), perKey, null, 2);

    Map<String, Object> doc0 = ds.getMapValue(0);
    assertNotNull(doc0);
    assertEquals(doc0.get("clicks"), 10);
    assertEquals(doc0.get("name"), "hello");
  }

  @Test
  public void testGetMapValueNullDoc() {
    DataSource clicksDs = mockDenseDataSource(DataType.INT, 10, true);

    ImmutableOpenStructDataSource ds = new ImmutableOpenStructDataSource(
        openStructSpec("event"), Map.of("clicks", clicksDs), null, 2);

    // doc 1 has null for clicks → no keys → null map
    Map<String, Object> doc1 = ds.getMapValue(1);
    assertNull(doc1);
  }

  @Test
  public void testGetMapValueWithSparse() {
    DataSource clicksDs = mockDenseDataSource(DataType.INT, 10, true);
    DataSource sparseDs = mockSparseDataSource("{\"rare_key\":\"val\"}", true);

    ImmutableOpenStructDataSource ds = new ImmutableOpenStructDataSource(
        openStructSpec("event"), Map.of("clicks", clicksDs), sparseDs, 2);

    Map<String, Object> doc0 = ds.getMapValue(0);
    assertNotNull(doc0);
    assertEquals(doc0.get("clicks"), 10);
    assertEquals(doc0.get("rare_key"), "val");
  }

  @Test
  public void testGetMapValueSparseOnlyNullDoc() {
    DataSource sparseDs = mockSparseDataSource("{\"rare_key\":\"val\"}", true);

    ImmutableOpenStructDataSource ds = new ImmutableOpenStructDataSource(
        openStructSpec("event"), Map.of(), sparseDs, 2);

    // doc 1: sparse is null
    Map<String, Object> doc1 = ds.getMapValue(1);
    assertNull(doc1);
  }

  @Test
  public void testGetMapValueEmptySegment() {
    ImmutableOpenStructDataSource ds = new ImmutableOpenStructDataSource(
        openStructSpec("event"), Map.of(), null, 0);

    assertNull(ds.getMapValue(0));
  }
}
