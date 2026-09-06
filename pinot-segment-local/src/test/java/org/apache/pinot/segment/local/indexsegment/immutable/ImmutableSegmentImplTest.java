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
package org.apache.pinot.segment.local.indexsegment.immutable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.index.map.ImmutableMapDataSource;
import org.apache.pinot.segment.local.segment.index.openstruct.ImmutableOpenStructDataSource;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.metadata.ColumnMetadataImpl;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.OpenStructNaming;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Tests for [ImmutableSegmentImpl]: the post-registration lifecycle hook, and the lazy column materialization mode
/// against a mocked [ColumnMaterializer].
public class ImmutableSegmentImplTest {
  private static final int NUM_DOCS = 10;

  /// The hook must reach the directory at most once per segment instance: the same segment can be registered more than
  /// once (e.g. an upsert replacement with a consistency mode other than NONE registers it through a
  /// DuoSegmentDataManager and then directly), and implementations are not required to be idempotent.
  @Test
  public void testOnSegmentAddedNotifiesDirectoryAtMostOnce()
      throws Exception {
    SegmentDirectory segmentDirectory = mock(SegmentDirectory.class);
    ImmutableSegmentImpl segment = createSegment(segmentDirectory);

    segment.onSegmentAdded();
    segment.onSegmentAdded();
    segment.onSegmentAdded();

    verify(segmentDirectory, times(1)).onSegmentAdded();
  }

  /// The hook fires after the segment is already serving, so a directory failure must not propagate out of it.
  @Test
  public void testOnSegmentAddedIsBestEffort()
      throws Exception {
    SegmentDirectory segmentDirectory = mock(SegmentDirectory.class);
    doThrow(new RuntimeException("boom")).when(segmentDirectory).onSegmentAdded();
    ImmutableSegmentImpl segment = createSegment(segmentDirectory);

    // Must not throw.
    segment.onSegmentAdded();

    verify(segmentDirectory).onSegmentAdded();
  }

  /// A failed attempt consumes the single notification: the directory was already told, and retry semantics are the
  /// implementation's business, not the caller's.
  @Test
  public void testFailedOnSegmentAddedIsNotRetried()
      throws Exception {
    SegmentDirectory segmentDirectory = mock(SegmentDirectory.class);
    doThrow(new RuntimeException("boom")).when(segmentDirectory).onSegmentAdded();
    ImmutableSegmentImpl segment = createSegment(segmentDirectory);

    segment.onSegmentAdded();
    segment.onSegmentAdded();

    verify(segmentDirectory, times(1)).onSegmentAdded();
  }

  private static ImmutableSegmentImpl createSegment(SegmentDirectory segmentDirectory) {
    SegmentMetadataImpl segmentMetadata = mock(SegmentMetadataImpl.class);
    when(segmentMetadata.getName()).thenReturn("seg");
    // getColumnMetadataMap() is declared as a TreeMap, so an immutable Map.of() will not do here.
    when(segmentMetadata.getColumnMetadataMap()).thenReturn(new TreeMap<>());
    return new ImmutableSegmentImpl(segmentDirectory, segmentMetadata, Map.of(), null);
  }

  /// The eager (flag off) mode is untouched: every data source exists from construction over the given containers.
  @Test
  public void testEagerModeCreatesDataSourcesAtConstruction() {
    ColumnMetadataImpl a = columnMetadata(intColumn("a"), null);
    ColumnIndexContainer containerA = mock(ColumnIndexContainer.class);
    ImmutableSegmentImpl segment =
        new ImmutableSegmentImpl(mock(SegmentDirectory.class), segmentMetadata(schema(a), a), Map.of("a", containerA),
            null);

    assertSame(segment.getDataSourceNullable("a").getIndexContainer(), containerA);
    assertNull(segment.getDataSourceNullable("unknown"));
  }

  @Test
  public void testLazyModeCreatesNothingAtConstruction()
      throws Exception {
    ColumnMetadataImpl a = columnMetadata(intColumn("a"), null);
    ColumnMetadataImpl b = columnMetadata(intColumn("b"), null);
    ColumnMaterializer materializer = mock(ColumnMaterializer.class);
    SegmentDirectory segmentDirectory = mock(SegmentDirectory.class);
    ImmutableSegmentImpl segment = lazySegment(segmentDirectory, schema(a, b), materializer, a, b);

    verifyNoInteractions(materializer);
    // Column listings come from the metadata schema and never materialize anything
    assertEquals(segment.getColumnNames(), Set.of("a", "b"));
    assertEquals(segment.getPhysicalColumnNames(), Set.of("a", "b"));
    // Neither does asking for a column the segment does not have
    assertNull(segment.getDataSourceNullable("unknown"));
    assertThrows(NullPointerException.class, () -> segment.getIndex("unknown", StandardIndexes.forward()));
    verifyNoInteractions(materializer);

    segment.destroy();
    verify(segmentDirectory).close();
  }

  @Test
  public void testLazyModeMaterializesEachColumnOnceUnderConcurrentAccess()
      throws Exception {
    ColumnMetadataImpl a = columnMetadata(intColumn("a"), null);
    ColumnIndexContainer containerA = mock(ColumnIndexContainer.class);
    ColumnMaterializer materializer = mock(ColumnMaterializer.class);
    AtomicInteger creations = new AtomicInteger();
    when(materializer.createIndexContainer(a)).thenAnswer(invocation -> {
      creations.incrementAndGet();
      // Widen the window in which every other caller must wait for this creation instead of starting its own
      Thread.sleep(50);
      return containerA;
    });
    ImmutableSegmentImpl segment = lazySegment(mock(SegmentDirectory.class), schema(a), materializer, a);

    int numCallers = 16;
    ExecutorService executor = Executors.newFixedThreadPool(numCallers);
    DataSource first;
    try {
      CountDownLatch start = new CountDownLatch(1);
      List<Future<DataSource>> futures = new ArrayList<>(numCallers);
      for (int i = 0; i < numCallers; i++) {
        futures.add(executor.submit(() -> {
          start.await();
          return segment.getDataSourceNullable("a");
        }));
      }
      start.countDown();
      first = futures.get(0).get();
      for (Future<DataSource> future : futures) {
        assertSame(future.get(), first);
      }
    } finally {
      executor.shutdownNow();
    }

    assertNotNull(first);
    assertSame(first.getIndexContainer(), containerA);
    assertEquals(creations.get(), 1);
    verify(materializer, times(1)).createIndexContainer(a);
  }

  @Test
  public void testDestroyClosesOnlyMaterializedContainersAndRefusesLaterMaterialization()
      throws Exception {
    ColumnMetadataImpl a = columnMetadata(intColumn("a"), null);
    ColumnMetadataImpl b = columnMetadata(intColumn("b"), null);
    ColumnIndexContainer containerA = mock(ColumnIndexContainer.class);
    ColumnIndexContainer containerB = mock(ColumnIndexContainer.class);
    ColumnMaterializer materializer = mock(ColumnMaterializer.class);
    when(materializer.createIndexContainer(a)).thenReturn(containerA);
    when(materializer.createIndexContainer(b)).thenReturn(containerB);
    SegmentDirectory segmentDirectory = mock(SegmentDirectory.class);
    ImmutableSegmentImpl segment = lazySegment(segmentDirectory, schema(a, b), materializer, a, b);
    DataSource dataSourceA = segment.getDataSourceNullable("a");
    assertNotNull(dataSourceA);

    segment.destroy();

    verify(containerA).close();
    verify(containerB, never()).close();
    verify(materializer, never()).createIndexContainer(b);
    verify(segmentDirectory).close();
    // A column that was never touched can no longer be materialized ...
    assertThrows(IllegalStateException.class, () -> segment.getDataSourceNullable("b"));
    assertThrows(IllegalStateException.class, () -> segment.getIndex("b", StandardIndexes.forward()));
    // ... while a materialized one keeps returning its (closed) data source as in the eager mode, and a column the
    // segment does not have is still simply absent
    assertSame(segment.getDataSourceNullable("a"), dataSourceA);
    assertNull(segment.getDataSourceNullable("unknown"));
    verify(materializer, times(1)).createIndexContainer(any());
  }

  @Test
  public void testGetIndexMaterializesAndSharesTheContainerWithTheDataSource() {
    ColumnMetadataImpl a = columnMetadata(intColumn("a"), null);
    ForwardIndexReader<?> forwardIndex = mock(ForwardIndexReader.class);
    ColumnIndexContainer containerA = mock(ColumnIndexContainer.class);
    doReturn(forwardIndex).when(containerA).getIndex(StandardIndexes.forward());
    ColumnMaterializer materializer = mock(ColumnMaterializer.class);
    when(materializer.createIndexContainer(a)).thenReturn(containerA);
    ImmutableSegmentImpl segment = lazySegment(mock(SegmentDirectory.class), schema(a), materializer, a);

    assertSame(segment.getForwardIndex("a"), forwardIndex);
    verify(materializer, times(1)).createIndexContainer(a);

    DataSource dataSource = segment.getDataSourceNullable("a");
    assertNotNull(dataSource);
    assertSame(dataSource.getIndexContainer(), containerA);
    assertSame(dataSource.getForwardIndex(), forwardIndex);
    verify(materializer, times(1)).createIndexContainer(a);
  }

  @Test
  public void testLazyModeGroupsOpenStructChildrenUnderTheirParent()
      throws Exception {
    ComplexFieldSpec metrics = new ComplexFieldSpec("metrics", FieldSpec.DataType.OPEN_STRUCT, true,
        Map.of("views", new DimensionFieldSpec("views", FieldSpec.DataType.LONG, true)));
    String viewsColumn = OpenStructNaming.materializedColumnName("metrics", "views");
    String sparseColumn = OpenStructNaming.sparseColumnName("metrics");
    ColumnMetadataImpl parent = columnMetadata(metrics, null);
    ColumnMetadataImpl views = columnMetadata(new DimensionFieldSpec(viewsColumn, FieldSpec.DataType.LONG, true),
        "metrics");
    ColumnMetadataImpl sparse = columnMetadata(new DimensionFieldSpec(sparseColumn, FieldSpec.DataType.STRING, true),
        "metrics");
    ColumnMetadataImpl dim = columnMetadata(intColumn("dim"), null);
    ForwardIndexReader<?> viewsForwardIndex = mock(ForwardIndexReader.class);
    ColumnIndexContainer viewsContainer = mock(ColumnIndexContainer.class);
    doReturn(viewsForwardIndex).when(viewsContainer).getIndex(StandardIndexes.forward());
    ColumnIndexContainer sparseContainer = mock(ColumnIndexContainer.class);
    ColumnMaterializer materializer = mock(ColumnMaterializer.class);
    when(materializer.createIndexContainer(views)).thenReturn(viewsContainer);
    when(materializer.createIndexContainer(sparse)).thenReturn(sparseContainer);
    // Only the parent and the regular column are in the schema; the children live in the segment metadata alone
    ImmutableSegmentImpl segment = lazySegment(mock(SegmentDirectory.class), schema(metrics, dim.getFieldSpec()),
        materializer, parent, views, sparse, dim);

    DataSource parentDataSource = segment.getDataSourceNullable("metrics");
    assertTrue(parentDataSource instanceof ImmutableOpenStructDataSource);
    ImmutableOpenStructDataSource openStruct = (ImmutableOpenStructDataSource) parentDataSource;
    assertTrue(openStruct.isMaterialized("views"));
    assertSame(openStruct.getDataSource("views").getIndexContainer(), viewsContainer);
    assertFalse(openStruct.isFullyMaterialized());
    assertSame(segment.getDataSourceNullable("metrics"), parentDataSource);
    // Children are reachable only through their parent, as in the eager mode
    assertNull(segment.getDataSourceNullable(viewsColumn));
    assertNull(segment.getDataSourceNullable(sparseColumn));
    // Both children were materialized exactly once, together with the parent, and getIndex reuses their containers
    verify(materializer, times(1)).createIndexContainer(views);
    verify(materializer, times(1)).createIndexContainer(sparse);
    verify(materializer, never()).createIndexContainer(parent);
    verify(materializer, never()).createIndexContainer(dim);
    assertSame(segment.getIndex(viewsColumn, StandardIndexes.forward()), viewsForwardIndex);
    verify(materializer, times(1)).createIndexContainer(views);

    segment.destroy();
    verify(viewsContainer).close();
    verify(sparseContainer).close();
  }

  @Test
  public void testLazyModeMapColumnYieldsMapDataSource() {
    ComplexFieldSpec mapSpec = new ComplexFieldSpec("m", FieldSpec.DataType.MAP, true,
        Map.of(ComplexFieldSpec.KEY_FIELD, new DimensionFieldSpec("key", FieldSpec.DataType.STRING, true),
            ComplexFieldSpec.VALUE_FIELD, new DimensionFieldSpec("value", FieldSpec.DataType.INT, true)));
    ColumnMetadataImpl m = columnMetadata(mapSpec, null);
    ColumnIndexContainer container = mock(ColumnIndexContainer.class);
    doReturn(mock(ForwardIndexReader.class)).when(container).getIndex(StandardIndexes.forward());
    ColumnMaterializer materializer = mock(ColumnMaterializer.class);
    when(materializer.createIndexContainer(m)).thenReturn(container);
    ImmutableSegmentImpl segment = lazySegment(mock(SegmentDirectory.class), schema(mapSpec), materializer, m);

    DataSource dataSource = segment.getDataSourceNullable("m");
    assertTrue(dataSource instanceof ImmutableMapDataSource);
    assertSame(dataSource.getIndexContainer(), container);
  }

  @Test
  public void testLazyModeFailedMaterializationLeavesNoMappingAndRetrySucceeds()
      throws Exception {
    ColumnMetadataImpl a = columnMetadata(intColumn("a"), null);
    ColumnIndexContainer containerA = mock(ColumnIndexContainer.class);
    ColumnMaterializer materializer = mock(ColumnMaterializer.class);
    when(materializer.createIndexContainer(a)).thenThrow(new UncheckedIOException(new IOException("boom")))
        .thenReturn(containerA);
    ImmutableSegmentImpl segment = lazySegment(mock(SegmentDirectory.class), schema(a), materializer, a);

    assertThrows(UncheckedIOException.class, () -> segment.getDataSourceNullable("a"));
    DataSource dataSource = segment.getDataSourceNullable("a");
    assertNotNull(dataSource);
    assertSame(dataSource.getIndexContainer(), containerA);
    verify(materializer, times(2)).createIndexContainer(a);

    segment.destroy();
    verify(containerA, times(1)).close();
  }

  /// Containers created at load (virtual columns, star-tree dimensions) are handed in already materialized: their data
  /// sources exist from construction, the materializer is never asked for them, and destroy() closes them.
  @Test
  public void testLazyModeMaterializedContainersGetDataSourcesAtConstruction()
      throws Exception {
    ColumnMetadataImpl a = columnMetadata(intColumn("a"), null);
    ColumnMetadataImpl b = columnMetadata(intColumn("b"), null);
    ColumnIndexContainer containerA = mock(ColumnIndexContainer.class);
    ColumnMaterializer materializer = mock(ColumnMaterializer.class);
    ConcurrentMap<String, ColumnIndexContainer> materialized = new ConcurrentHashMap<>(Map.of("a", containerA));
    ImmutableSegmentImpl segment =
        new ImmutableSegmentImpl(mock(SegmentDirectory.class), segmentMetadata(schema(a, b), a, b), materializer,
            materialized, null, null);

    verifyNoInteractions(materializer);
    assertSame(segment.getDataSourceNullable("a").getIndexContainer(), containerA);
    verifyNoInteractions(materializer);

    segment.destroy();
    verify(containerA).close();
  }

  private static ImmutableSegmentImpl lazySegment(SegmentDirectory segmentDirectory, Schema schema,
      ColumnMaterializer materializer, ColumnMetadata... columns) {
    return new ImmutableSegmentImpl(segmentDirectory, segmentMetadata(schema, columns), materializer,
        new ConcurrentHashMap<>(), null, null);
  }

  private static SegmentMetadataImpl segmentMetadata(Schema schema, ColumnMetadata... columns) {
    SegmentMetadataImpl segmentMetadata = mock(SegmentMetadataImpl.class);
    when(segmentMetadata.getName()).thenReturn("seg");
    when(segmentMetadata.getTotalDocs()).thenReturn(NUM_DOCS);
    when(segmentMetadata.getSchema()).thenReturn(schema);
    TreeMap<String, ColumnMetadata> columnMetadataMap = new TreeMap<>();
    for (ColumnMetadata column : columns) {
      columnMetadataMap.put(column.getColumnName(), column);
    }
    when(segmentMetadata.getColumnMetadataMap()).thenReturn(columnMetadataMap);
    return segmentMetadata;
  }

  private static Schema schema(FieldSpec... fieldSpecs) {
    Schema schema = new Schema();
    for (FieldSpec fieldSpec : fieldSpecs) {
      schema.addField(fieldSpec);
    }
    return schema;
  }

  private static Schema schema(ColumnMetadata... columns) {
    Schema schema = new Schema();
    for (ColumnMetadata column : columns) {
      schema.addField(column.getFieldSpec());
    }
    return schema;
  }

  private static FieldSpec intColumn(String name) {
    return new DimensionFieldSpec(name, FieldSpec.DataType.INT, true);
  }

  private static ColumnMetadataImpl columnMetadata(FieldSpec fieldSpec, @Nullable String parentColumn) {
    ColumnMetadataImpl.Builder builder =
        ColumnMetadataImpl.builder().setFieldSpec(fieldSpec).setTotalDocs(NUM_DOCS).setCardinality(NUM_DOCS)
            .setHasDictionary(false);
    if (parentColumn != null) {
      builder.setParentColumn(parentColumn);
    }
    return builder.build();
  }
}
