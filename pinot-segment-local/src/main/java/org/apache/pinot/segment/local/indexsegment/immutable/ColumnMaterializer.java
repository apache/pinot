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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.index.column.PhysicalColumnIndexContainer;
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
/// The per-column configs are snapshotted at construction because the loading config they come from is mutable and
/// shared: its map is replaced whenever the config is refreshed and mutated in place while OPEN_STRUCT child configs
/// are resolved. The snapshot is compacted so that a wide segment retains close to nothing per column, which is the
/// point of materializing lazily: configs that are equal by value collapse to one instance, the most common one
/// becomes the implicit default, and only the columns that differ from it keep an entry (keyed by the column-name
/// strings the segment metadata already holds). A column absent from the loading config maps to
/// [FieldIndexConfigs#EMPTY], exactly what the eager path hands to the container. Collapsing relies on the value
/// equality of the index configs; a config type that inherits the enabled/disabled-only equality of `IndexConfig`
/// collapses on that alone, which is safe as long as its reader factory ignores the config (true of OPEN_STRUCT, the
/// one such type today).
///
/// Thread-safe: immutable after construction, and creating a container mutates nothing here.
class ColumnMaterializer {
  private final SegmentDirectory.Reader _segmentReader;
  private final boolean _forwardIndexOnly;
  private final FieldIndexConfigs _defaultFieldIndexConfigs;
  private final Map<String, FieldIndexConfigs> _fieldIndexConfigOverrides;
  @Nullable
  private final MultiColumnLuceneTextIndexReader _multiColumnTextIndex;
  private final Set<String> _multiColumnTextIndexColumns;

  /// @param columns the physical columns of the segment; their configs are looked up in `fieldIndexConfigByColumn`
  ///                now, so the map may change afterwards
  /// @param multiColumnTextIndexColumns the columns covered by `multiColumnTextIndex` (empty when there is none)
  ColumnMaterializer(SegmentDirectory.Reader segmentReader, Collection<String> columns,
      Map<String, FieldIndexConfigs> fieldIndexConfigByColumn, boolean forwardIndexOnly,
      @Nullable MultiColumnLuceneTextIndexReader multiColumnTextIndex, Set<String> multiColumnTextIndexColumns) {
    _segmentReader = segmentReader;
    _forwardIndexOnly = forwardIndexOnly;
    _multiColumnTextIndex = multiColumnTextIndex;
    _multiColumnTextIndexColumns = multiColumnTextIndexColumns;

    Map<FieldIndexConfigs, FieldIndexConfigs> canonical = new HashMap<>();
    Map<FieldIndexConfigs, Integer> counts = new HashMap<>();
    Map<String, FieldIndexConfigs> configsByColumn = new HashMap<>();
    for (String column : columns) {
      FieldIndexConfigs configs = canonical.computeIfAbsent(
          fieldIndexConfigByColumn.getOrDefault(column, FieldIndexConfigs.EMPTY), Function.identity());
      counts.merge(configs, 1, Integer::sum);
      configsByColumn.put(column, configs);
    }
    FieldIndexConfigs defaultConfigs = FieldIndexConfigs.EMPTY;
    int maxCount = 0;
    for (Map.Entry<FieldIndexConfigs, Integer> entry : counts.entrySet()) {
      if (entry.getValue() > maxCount) {
        defaultConfigs = entry.getKey();
        maxCount = entry.getValue();
      }
    }
    _defaultFieldIndexConfigs = defaultConfigs;
    configsByColumn.values().removeIf(configs -> configs == _defaultFieldIndexConfigs);
    _fieldIndexConfigOverrides = Map.copyOf(configsByColumn);
  }

  /// Creates the index container of the column, attaching the shared multi-column text index reader when the column is
  /// part of it. Fails with an [UncheckedIOException] when an index cannot be read.
  ColumnIndexContainer createIndexContainer(ColumnMetadata columnMetadata) {
    String column = columnMetadata.getColumnName();
    PhysicalColumnIndexContainer container;
    try {
      container = new PhysicalColumnIndexContainer(_segmentReader, columnMetadata, getFieldIndexConfigs(column),
          _forwardIndexOnly);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to materialize the indexes of column: " + column, e);
    }
    if (_multiColumnTextIndex != null && _multiColumnTextIndexColumns.contains(column)) {
      container.setMultiColumnTextIndex(_multiColumnTextIndex);
    }
    return container;
  }

  @VisibleForTesting
  FieldIndexConfigs getFieldIndexConfigs(String column) {
    return _fieldIndexConfigOverrides.getOrDefault(column, _defaultFieldIndexConfigs);
  }

  @VisibleForTesting
  Map<String, FieldIndexConfigs> getFieldIndexConfigOverrides() {
    return _fieldIndexConfigOverrides;
  }
}
