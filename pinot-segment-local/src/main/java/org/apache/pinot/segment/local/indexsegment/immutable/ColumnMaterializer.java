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
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.index.column.PhysicalColumnIndexContainer;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.readers.text.MultiColumnLuceneTextIndexReader;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;


/// Creates the [ColumnIndexContainer] of a physical column of an [ImmutableSegmentImpl] on demand.
///
/// [ImmutableSegmentLoader] builds one per segment when lazy column materialization is on, instead of a
/// [PhysicalColumnIndexContainer] per column at load. It retains only what creating a container later needs: the
/// segment reader (held for the segment's lifetime anyway), the forward-index-only flag, the shared multi-column text
/// index reader and, per column, the [FieldIndexConfigs] that were in effect at load.
///
/// [IndexLoadingConfig] replaces its resolved config map on refresh and copies it when adding OPEN_STRUCT child
/// configs. Retaining its unmodifiable map view therefore preserves the settings in effect when this segment loads
/// without copying per-column config entries. A column absent from the map uses [FieldIndexConfigs#EMPTY].
///
/// Thread-safe: immutable after construction, and creating a container mutates nothing here.
class ColumnMaterializer {
  private final SegmentDirectory.Reader _segmentReader;
  private final boolean _forwardIndexOnly;
  private final Map<String, FieldIndexConfigs> _fieldIndexConfigsByColumn;
  @Nullable
  private final MultiColumnLuceneTextIndexReader _multiColumnTextIndex;
  private final Set<String> _multiColumnTextIndexColumns;

  /// @param multiColumnTextIndexColumns the columns covered by `multiColumnTextIndex` (empty when there is none)
  ColumnMaterializer(SegmentDirectory.Reader segmentReader, Map<String, FieldIndexConfigs> fieldIndexConfigsByColumn,
      boolean forwardIndexOnly,
      @Nullable MultiColumnLuceneTextIndexReader multiColumnTextIndex, Set<String> multiColumnTextIndexColumns) {
    _segmentReader = segmentReader;
    _forwardIndexOnly = forwardIndexOnly;
    _fieldIndexConfigsByColumn = fieldIndexConfigsByColumn;
    _multiColumnTextIndex = multiColumnTextIndex;
    _multiColumnTextIndexColumns = multiColumnTextIndexColumns;
  }

  /// Creates the index container of the column, attaching the shared multi-column text index reader when the column is
  /// part of it. Fails with an [UncheckedIOException] when an index cannot be read.
  ColumnIndexContainer createIndexContainer(ColumnMetadata columnMetadata) {
    String column = columnMetadata.getColumnName();
    PhysicalColumnIndexContainer container;
    try {
      container = new PhysicalColumnIndexContainer(_segmentReader, columnMetadata,
          _fieldIndexConfigsByColumn.getOrDefault(column, FieldIndexConfigs.EMPTY), _forwardIndexOnly);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to materialize the indexes of column: " + column, e);
    }
    if (_multiColumnTextIndex != null && _multiColumnTextIndexColumns.contains(column)) {
      container.setMultiColumnTextIndex(_multiColumnTextIndex);
    }
    return container;
  }

}
