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
package org.apache.pinot.segment.local.segment.creator.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.index.datasource.EmptyDataSource;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.datasource.OpenStructDataSource;
import org.apache.pinot.segment.spi.index.IndexCreator;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.creator.ColumnarOpenStructIndexCreator;
import org.apache.pinot.segment.spi.index.creator.OpenStructColumnarSource;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Pins the OPEN_STRUCT dispatch in [SegmentColumnarIndexCreator#indexOpenStructColumn]: when the
/// columnar hand-off is taken versus when the per-document path is. The hand-off writes documents
/// in source order with none filtered out, so taking it under a sorted build or under commit-time
/// compaction would write a segment whose documents are renumbered or that keeps documents the
/// caller asked to drop — a wrong segment rather than a slow one.
public class SegmentColumnarIndexCreatorOpenStructDispatchTest {
  private static final String COLUMN = "metrics";
  private static final int NUM_DOCS = 3;

  @Test
  public void testColumnarPathTakenWhenNothingFiltersOrReorders()
      throws IOException {
    RecordingOpenStructIndexCreator creator = new RecordingOpenStructIndexCreator(true);
    SegmentColumnarIndexCreator.indexOpenStructColumn(COLUMN, newDataSource(NUM_DOCS), NUM_DOCS, null, null,
        List.of(creator));

    assertEquals(creator._columnarCalls, 1, "Expected the columnar hand-off to be used");
    assertTrue(creator._perDocValues.isEmpty(), "The columnar hand-off must not also feed documents one by one");
  }

  @Test
  public void testSortedDocIdsForcesPerDocPath()
      throws IOException {
    RecordingOpenStructIndexCreator creator = new RecordingOpenStructIndexCreator(true);
    // Descending order: the columnar source carries no docId-to-on-disk-position mapping, so taking
    // it here would write the documents in the wrong on-disk order.
    SegmentColumnarIndexCreator.indexOpenStructColumn(COLUMN, newDataSource(NUM_DOCS), NUM_DOCS,
        new int[]{2, 1, 0}, null, List.of(creator));

    assertEquals(creator._columnarCalls, 0, "A sorted build must not take the columnar hand-off");
    assertEquals(creator._perDocValues, List.of(rowFor(2), rowFor(1), rowFor(0)),
        "The per-document path must follow sortedDocIds order");
  }

  @Test
  public void testValidDocIdsForcesPerDocPath()
      throws IOException {
    RecordingOpenStructIndexCreator creator = new RecordingOpenStructIndexCreator(true);
    RoaringBitmap validDocIds = RoaringBitmap.bitmapOf(0, 2);
    SegmentColumnarIndexCreator.indexOpenStructColumn(COLUMN, newDataSource(NUM_DOCS), NUM_DOCS, null, validDocIds,
        List.of(creator));

    assertEquals(creator._columnarCalls, 0, "Commit-time compaction must not take the columnar hand-off");
    assertEquals(creator._perDocValues, List.of(rowFor(0), rowFor(2)),
        "The per-document path must skip documents absent from validDocIds");
  }

  @Test
  public void testNumDocsMismatchFallsBackInsteadOfThrowing()
      throws IOException {
    RecordingOpenStructIndexCreator creator = new RecordingOpenStructIndexCreator(true);
    // The snapshot and the segment metadata are read at different times through different objects,
    // so a disagreement is a consistency concern, not a corrupt-input condition.
    SegmentColumnarIndexCreator.indexOpenStructColumn(COLUMN, newDataSource(NUM_DOCS - 1), NUM_DOCS, null, null,
        List.of(creator));

    assertEquals(creator._columnarCalls, 0, "A numDocs mismatch must not take the columnar hand-off");
    assertEquals(creator._perDocValues, List.of(rowFor(0), rowFor(1), rowFor(2)),
        "The fallback must feed every document the segment claims to hold");
  }

  @Test
  public void testUnsupportedCreatorForcesPerDocPathForEveryCreator()
      throws IOException {
    RecordingOpenStructIndexCreator supporting = new RecordingOpenStructIndexCreator(true);
    RecordingOpenStructIndexCreator refusing = new RecordingOpenStructIndexCreator(false);
    SegmentColumnarIndexCreator.indexOpenStructColumn(COLUMN, newDataSource(NUM_DOCS), NUM_DOCS, null, null,
        List.of(supporting, refusing));

    // A creator that had already accepted the source could not be rewound, so the check has to run
    // before any creator is fed — the supporting creator must be untouched by the columnar path.
    assertEquals(supporting._columnarCalls, 0, "No creator may be partially fed when another one refuses");
    assertEquals(refusing._columnarCalls, 0);
    List<Map<String, Object>> expected = List.of(rowFor(0), rowFor(1), rowFor(2));
    assertEquals(supporting._perDocValues, expected);
    assertEquals(refusing._perDocValues, expected);
  }

  @Test
  public void testNonColumnarCreatorForcesPerDocPath()
      throws IOException {
    RecordingOpenStructIndexCreator columnar = new RecordingOpenStructIndexCreator(true);
    RecordingIndexCreator plain = new RecordingIndexCreator();
    SegmentColumnarIndexCreator.indexOpenStructColumn(COLUMN, newDataSource(NUM_DOCS), NUM_DOCS, null, null,
        List.of(columnar, plain));

    assertEquals(columnar._columnarCalls, 0,
        "A creator that does not implement ColumnarOpenStructIndexCreator must force the per-document path");
    List<Map<String, Object>> expected = List.of(rowFor(0), rowFor(1), rowFor(2));
    assertEquals(columnar._perDocValues, expected);
    assertEquals(plain._perDocValues, expected);
  }

  @Test
  public void testNoCreatorsIsANoOp()
      throws IOException {
    // supportsColumnarAdd() rejects an empty list rather than treating "every creator supports it"
    // as vacuously true; the per-document loop then has nothing to feed.
    SegmentColumnarIndexCreator.indexOpenStructColumn(COLUMN, newDataSource(NUM_DOCS), NUM_DOCS, null, null,
        List.of());
  }

  private static Map<String, Object> rowFor(int docId) {
    Map<String, Object> row = new HashMap<>();
    row.put("k", (long) docId);
    return row;
  }

  private static StubOpenStructDataSource newDataSource(int columnarSourceNumDocs) {
    return new StubOpenStructDataSource(columnarSourceNumDocs);
  }

  /// Minimal [OpenStructDataSource] serving one key per document, plus a columnar snapshot whose
  /// document count is fixed at construction so the mismatch fallback can be driven. Inherits the
  /// zero-row [EmptyDataSource] index/metadata surface: the dispatch under test consults neither.
  /// Not thread-safe; single-threaded test use only.
  private static final class StubOpenStructDataSource extends EmptyDataSource implements OpenStructDataSource {
    private final int _columnarSourceNumDocs;

    private StubOpenStructDataSource(int columnarSourceNumDocs) {
      super(new ComplexFieldSpec(COLUMN, DataType.OPEN_STRUCT, true, Map.of()));
      _columnarSourceNumDocs = columnarSourceNumDocs;
    }

    @Override
    public ComplexFieldSpec getFieldSpec() {
      return (ComplexFieldSpec) getDataSourceMetadata().getFieldSpec();
    }

    @Nullable
    @Override
    public DataSource getDataSource(String key) {
      return null;
    }

    @Override
    public boolean isMaterialized(String key) {
      return false;
    }

    @Override
    public boolean isFullyMaterialized() {
      return false;
    }

    @Override
    public Map<String, DataSource> getDataSources() {
      return Map.of();
    }

    @Nullable
    @Override
    public DataSourceMetadata getDataSourceMetadata(String key) {
      return null;
    }

    @Nullable
    @Override
    public ColumnIndexContainer getIndexContainer(String key) {
      return null;
    }

    @Nullable
    @Override
    public Map<String, Object> getMapValue(int docId) {
      return rowFor(docId);
    }

    @Nullable
    @Override
    public OpenStructColumnarSource getColumnarSource() {
      return new OpenStructColumnarSource() {
        @Override
        public int getNumDocs() {
          return _columnarSourceNumDocs;
        }

        @Override
        public Set<String> getKeys() {
          return Set.of("k");
        }

        @Override
        public DataType getStoredType(String key) {
          return DataType.LONG;
        }

        @Override
        public void forEachPresentValue(String key, PresentValueConsumer consumer) {
          for (int docId = 0; docId < _columnarSourceNumDocs; docId++) {
            consumer.accept(docId, (long) docId);
          }
        }
      };
    }
  }

  /// Records which ingestion path the dispatch chose: one entry per per-document `add`, and a
  /// count of `addColumnar` calls. Not thread-safe; single-threaded test use only.
  private static final class RecordingOpenStructIndexCreator implements ColumnarOpenStructIndexCreator {
    private final boolean _supportsColumnarAdd;
    private final List<Object> _perDocValues = new ArrayList<>();
    private int _columnarCalls;

    private RecordingOpenStructIndexCreator(boolean supportsColumnarAdd) {
      _supportsColumnarAdd = supportsColumnarAdd;
    }

    @Override
    public boolean supportsColumnarAdd() {
      return _supportsColumnarAdd;
    }

    @Override
    public void addColumnar(OpenStructColumnarSource source) {
      _columnarCalls++;
    }

    @Override
    public void add(Map<String, Object> openStructValue, int docId) {
      _perDocValues.add(openStructValue);
    }

    @Override
    public void add(Object value, int dictId) {
      _perDocValues.add(value);
    }

    @Override
    public void add(Object[] values, @Nullable int[] dictIds) {
      throw new UnsupportedOperationException("OPEN_STRUCT index is single-value only");
    }

    @Override
    public void seal() {
    }

    @Override
    public void close() {
    }
  }

  /// A creator that is not a [ColumnarOpenStructIndexCreator] at all, recording the per-document
  /// values it receives. Not thread-safe; single-threaded test use only.
  private static final class RecordingIndexCreator implements IndexCreator {
    private final List<Object> _perDocValues = new ArrayList<>();

    @Override
    public void add(Object value, int dictId) {
      _perDocValues.add(value);
    }

    @Override
    public void add(Object[] values, @Nullable int[] dictIds) {
      throw new UnsupportedOperationException("OPEN_STRUCT index is single-value only");
    }

    @Override
    public void seal() {
    }

    @Override
    public void close() {
    }
  }
}
