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
package org.apache.pinot.segment.spi;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.multicolumntext.MultiColumnTextMetadata;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Metadata;
import org.apache.pinot.spi.annotations.InterfaceAudience;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.joda.time.Duration;
import org.joda.time.Interval;


/// The `SegmentMetadata` class holds the segment level management information and data statistics.
@InterfaceAudience.Private
public interface SegmentMetadata {

  /// Returns the raw table name (without the type suffix).
  @Deprecated
  String getTableName();

  String getName();

  String getTimeColumn();

  long getStartTime();

  long getEndTime();

  TimeUnit getTimeUnit();

  Duration getTimeGranularity();

  Interval getTimeInterval();

  String getCrc();

  String getDataCrc();

  SegmentVersion getVersion();

  /// Returns the schema of the segment, one [org.apache.pinot.spi.data.FieldSpec] per column.
  ///
  /// The `Schema` object itself belongs to this segment, but the specs it holds are shared with every other loaded
  /// segment (of this table or any other) whose column parses to an equal spec, and must be treated as immutable: never
  /// call a setter on one; copy it (e.g. through a JSON round-trip) before mutating. Removing a column from this schema
  /// does not affect other segments.
  ///
  /// An implementation may derive the schema on demand rather than hold it per segment, so load- and query-path code
  /// should read column names through [#getAllColumns()] and field specs through [#getColumnMetadataFor(String)]
  /// instead of building a schema for every segment it touches.
  Schema getSchema();

  int getTotalDocs();

  File getIndexDir();

  @Nullable
  String getCreatorName();

  long getIndexCreationTime();

  /// Return the last time a record was indexed in this segment. Applicable for MutableSegments.
  ///
  /// @return time when the last record was indexed
  long getLastIndexedTimestamp();

  /// Return the latest ingestion timestamp associated with the records indexed in this segment.
  /// Applicable for MutableSegments.
  ///
  /// @return latest timestamp associated with indexed records
  ///         `Long.MIN_VALUE` if the stream doesn't provide a timestamp
  long getLatestIngestionTimestamp();

  /// Return the minimum ingestion lag recorded for this segment. Ingestion lag is
  /// the difference between the record ingestion timestamp and current system time.
  /// Applicable for MutableSegments.
  ///
  /// @return minimum ingestion lag recorded for this segment
  long getMinimumIngestionLagMs();

  @Nullable
  List<StarTreeV2Metadata> getStarTreeV2MetadataList();

  @Nullable
  MultiColumnTextMetadata getMultiColumnTextMetadata();

  Map<String, String> getCustomMap();

  String getStartOffset();

  String getEndOffset();

  default NavigableSet<String> getAllColumns() {
    return getSchema().getColumnNames();
  }

  /// Number of columns in [#getAllColumns()].
  ///
  /// A segment that holds no column metadata (a CONSUMING one, built from an explicit schema) still reports its
  /// schema's columns here, so this is not the size of [#getAllColumnMetadata()]: do not pair the two.
  default int getNumColumns() {
    return getColumnMetadataMap().size();
  }

  /// The column metadata of every column that has some, in the natural column-name order of [#getAllColumns()], and
  /// empty for a segment that holds none (a CONSUMING one, which answers [#getColumnMetadataFor(String)] with `null`
  /// for every column of its schema).
  default Collection<ColumnMetadata> getAllColumnMetadata() {
    TreeMap<String, ColumnMetadata> columnMetadataMap = getColumnMetadataMap();
    return columnMetadataMap != null ? columnMetadataMap.values() : List.of();
  }

  /// Applies `action` to every (column name, column metadata) pair, in the natural column-name order of
  /// [#getAllColumns()], and to nothing at all for a segment that holds no column metadata, exactly as
  /// [#getAllColumnMetadata()] is empty for one.
  default void forEachColumn(BiConsumer<String, ColumnMetadata> action) {
    getColumnMetadataMap().forEach(action);
  }

  /// Returns the whole column metadata as a map, for callers that need one.
  ///
  /// An implementation may hold its columns in a form that costs less than a map entry per column and build this map
  /// on demand, so load- and query-path code must not call this: it re-inflates a map entry per column for every
  /// segment it touches, and a server keeps that for the segment's lifetime. Read column names through
  /// [#getAllColumns()], one column through [#getColumnMetadataFor(String)], all of them through
  /// [#getAllColumnMetadata()] or [#forEachColumn(BiConsumer)], and mutate through
  /// [#addColumnMetadata(String, ColumnMetadata)] / [#removeColumn(String)] rather than through the returned map,
  /// whose writes an implementation is free not to see.
  TreeMap<String, ColumnMetadata> getColumnMetadataMap();

  /// Returns the metadata of the given column, or `null` if the segment has no such column.
  @Nullable
  default ColumnMetadata getColumnMetadataFor(String column) {
    return getColumnMetadataMap().get(column);
  }

  /// The names of the physical (non-virtual) columns, i.e. `getSchema().getPhysicalColumnNames()` without building
  /// the schema. Segment load runs this once per segment (the forward-index handler asks which columns exist), and on
  /// a server holding tens of thousands of wide segments a schema built there would be cached for the segment's whole
  /// life: one [Schema] per segment, each with a tree entry and two list slots per column. A segment that holds no
  /// column metadata (a CONSUMING one) still answers from its schema, which it was constructed with.
  ///
  /// Sorted, like the [Schema#getPhysicalColumnNames()] this replaces, and the same set for both segment kinds.
  default SortedSet<String> getPhysicalColumnNames() {
    Collection<ColumnMetadata> columnMetadata = getAllColumnMetadata();
    if (columnMetadata.isEmpty()) {
      return getSchema().getPhysicalColumnNames();
    }
    TreeSet<String> physicalColumnNames = new TreeSet<>();
    for (ColumnMetadata metadata : columnMetadata) {
      FieldSpec fieldSpec = metadata.getFieldSpec();
      if (!fieldSpec.isVirtualColumn()) {
        physicalColumnNames.add(fieldSpec.getName());
      }
    }
    return physicalColumnNames;
  }

  /// Registers the metadata of a column, replacing any metadata already registered under the same name. An
  /// implementation that holds no column metadata (a CONSUMING segment) may reject this.
  default void addColumnMetadata(String column, ColumnMetadata columnMetadata) {
    getColumnMetadataMap().put(column, columnMetadata);
  }

  /// Removes a column from the segment metadata. An implementation that holds no column metadata (a CONSUMING
  /// segment) may reject this.
  void removeColumn(String column);

  /// Converts segment metadata to json.
  /// @param columnFilter list only the columns in the set. Lists all the columns if the parameter value is null.
  /// @return json representation of segment metadata.
  JsonNode toJson(@Nullable Set<String> columnFilter);

  default boolean isMutableSegment() {
    return false;
  }
}
