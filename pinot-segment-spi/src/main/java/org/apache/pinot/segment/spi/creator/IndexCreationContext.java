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
package org.apache.pinot.segment.spi.creator;

import java.io.File;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.ColumnShape;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.FieldSpec;


/// Context of a column for index creation. Inherits the [ColumnShape] accessors and sources shape values from either
/// [ColumnStatistics] (segment-creation path) or [ColumnMetadata] (index-handler path) via [Builder].
public interface IndexCreationContext extends ColumnShape {

  // Identity.

  /// Returns the directory where the index file(s) should be written.
  File getIndexDir();

  /// Returns the fully-qualified table name (e.g. `myTable_OFFLINE`).
  String getTableNameWithType();

  /// Returns the source [ColumnStatistics] on the segment-creation path, or `null` on the index-handler path.
  @Nullable
  ColumnStatistics getColumnStatistics();

  /// Returns the source [ColumnMetadata] on the index-handler path, or `null` on the segment-creation path.
  @Nullable
  ColumnMetadata getColumnMetadata();

  // Overridable column attributes (IndexCreationContext-specific; not on ColumnShape).

  /// Returns `true` when the column has a dictionary.
  boolean hasDictionary();

  // Build-time toggles.

  /// Returns `true` when index buffers should be allocated on-heap, `false` for off-heap (memory-mapped).
  boolean isOnHeap();

  /// Returns `true` when the dictionary-optimization pass selected this column for raw encoding.
  boolean isOptimizeDictionary();

  /// Returns `true` when the text-index creator should commit the underlying Lucene index on close.
  boolean isTextCommitOnClose();

  // Realtime-conversion context.

  /// Returns `true` when this index creation is part of a realtime → offline segment conversion.
  boolean isRealtimeConversion();

  /// Returns the realtime consumer directory; only meaningful when [#isRealtimeConversion] is `true`.
  @Nullable
  File getConsumerDir();

  /// Returns `true` when the source mutable segment was compacted prior to conversion; only meaningful when
  /// [#isRealtimeConversion] is `true`.
  boolean isMutableSegmentCompacted();

  /// Returns the mutable-to-immutable docId mapping generated during realtime segment conversion, or `null` when not
  /// applicable.
  @Nullable
  int[] getMutableToImmutableDocIdMap();

  // Error handling.

  /// Returns `true` when index creation should continue past errors instead of aborting the whole segment.
  boolean isContinueOnError();

  @Deprecated(since = "1.6.0", forRemoval = true)
  default int getLengthOfLongestEntry() {
    return getLengthOfLongestElement();
  }

  @Deprecated(since = "1.6.0", forRemoval = true)
  default int getMaxNumberOfMultiValueElements() {
    return getMaxNumberOfMultiValues();
  }

  @Deprecated(since = "1.6.0", forRemoval = true)
  @Nullable
  default Object getSortedUniqueElementsArray() {
    ColumnStatistics columnStatistics = getColumnStatistics();
    return columnStatistics != null ? columnStatistics.getUniqueValuesSet() : null;
  }

  @SuppressWarnings("UnusedReturnValue")
  final class Builder {
    // Identity. Non-overridable shape accessors delegate to `_columnShape`.
    private final File _indexDir;
    private final String _tableNameWithType;
    private final ColumnShape _columnShape;

    // Overridable column attributes. Source-derived defaults populated in the constructors below;
    // handlers may override via `withXxx` setters.
    private int _lengthOfShortestElement;
    private int _lengthOfLongestElement;
    private boolean _isAscii;
    private int _totalNumberOfEntries;
    private int _maxNumberOfMultiValues;
    private int _maxRowLengthInBytes;
    private boolean _hasDictionary;

    // Build-time toggles.
    private boolean _onHeap;
    private boolean _optimizedDictionary;
    private boolean _textCommitOnClose;

    // Realtime-conversion context (meaningful only when `_realtimeConversion` is true).
    private boolean _realtimeConversion;
    @Nullable
    private File _consumerDir;
    private boolean _mutableSegmentCompacted;
    @Nullable
    private int[] _mutableToImmutableDocIdMap;

    // Error handling.
    private boolean _continueOnError;

    /// Segment-creation path. Shape values come from the freshly-collected [ColumnStatistics]; `hasDictionary` is
    /// supplied separately because it's a driver decision not carried on [ColumnStatistics]. `_tableNameWithType`
    /// and `_continueOnError` are derived from `tableConfig`.
    public Builder(File indexDir, TableConfig tableConfig, ColumnStatistics columnStatistics, boolean hasDictionary) {
      this(indexDir, tableConfig.getTableName(), columnStatistics, hasDictionary, isContinueOnError(tableConfig));
    }

    /// Index-handler path. Shape values (including `hasDictionary`) come from [ColumnMetadata]; `_tableNameWithType`
    /// and `_continueOnError` are derived from `tableConfig`.
    public Builder(File indexDir, TableConfig tableConfig, ColumnMetadata columnMetadata) {
      this(indexDir, tableConfig.getTableName(), columnMetadata, columnMetadata.hasDictionary(),
          isContinueOnError(tableConfig));
    }

    /// Generic constructor for callers that have a [ColumnShape] but not a [TableConfig] — every value the
    /// convenience constructors would derive (`tableNameWithType`, `hasDictionary`, `continueOnError`) must be
    /// supplied explicitly.
    public Builder(File indexDir, String tableNameWithType, ColumnShape columnShape, boolean hasDictionary,
        boolean continueOnError) {
      _indexDir = indexDir;
      _tableNameWithType = tableNameWithType;
      _columnShape = columnShape;
      _lengthOfShortestElement = columnShape.getLengthOfShortestElement();
      _lengthOfLongestElement = columnShape.getLengthOfLongestElement();
      _isAscii = columnShape.isAscii();
      _totalNumberOfEntries = columnShape.getTotalNumberOfEntries();
      _maxNumberOfMultiValues = columnShape.getMaxNumberOfMultiValues();
      _maxRowLengthInBytes = columnShape.getMaxRowLengthInBytes();
      _hasDictionary = hasDictionary;
      _continueOnError = continueOnError;
    }

    private static boolean isContinueOnError(TableConfig tableConfig) {
      IngestionConfig ingestionConfig = tableConfig.getIngestionConfig();
      return ingestionConfig != null && ingestionConfig.isContinueOnError();
    }

    // Overridable column-attribute setters.

    public Builder withLengthOfShortestElement(int lengthOfShortestElement) {
      _lengthOfShortestElement = lengthOfShortestElement;
      return this;
    }

    public Builder withLengthOfLongestElement(int lengthOfLongestElement) {
      _lengthOfLongestElement = lengthOfLongestElement;
      return this;
    }

    public Builder withAscii(boolean isAscii) {
      _isAscii = isAscii;
      return this;
    }

    public Builder withTotalNumberOfEntries(int totalNumberOfEntries) {
      _totalNumberOfEntries = totalNumberOfEntries;
      return this;
    }

    public Builder withMaxNumberOfMultiValues(int maxNumberOfMultiValues) {
      _maxNumberOfMultiValues = maxNumberOfMultiValues;
      return this;
    }

    public Builder withMaxRowLengthInBytes(int maxRowLengthInBytes) {
      _maxRowLengthInBytes = maxRowLengthInBytes;
      return this;
    }

    public Builder withDictionary(boolean hasDictionary) {
      _hasDictionary = hasDictionary;
      return this;
    }

    // Build-time toggle setters.

    public Builder withOnHeap(boolean onHeap) {
      _onHeap = onHeap;
      return this;
    }

    public Builder withOptimizedDictionary(boolean optimized) {
      _optimizedDictionary = optimized;
      return this;
    }

    public Builder withTextCommitOnClose(boolean textCommitOnClose) {
      _textCommitOnClose = textCommitOnClose;
      return this;
    }

    // Realtime-conversion context setters.

    public Builder withRealtimeConversion(boolean realtimeConversion) {
      _realtimeConversion = realtimeConversion;
      return this;
    }

    public Builder withConsumerDir(File consumerDir) {
      _consumerDir = consumerDir;
      return this;
    }

    public Builder withMutableSegmentCompacted(boolean mutableSegmentCompacted) {
      _mutableSegmentCompacted = mutableSegmentCompacted;
      return this;
    }

    public Builder withMutableToImmutableDocIdMap(int[] mutableToImmutableDocIdMap) {
      _mutableToImmutableDocIdMap = mutableToImmutableDocIdMap;
      return this;
    }

    // Error handling setter.

    /// Overrides the `continueOnError` value supplied at construction. Useful when the
    /// [TableConfig]-derived default doesn't match the caller's needs (e.g. a [SegmentGeneratorConfig] override).
    public Builder withContinueOnError(boolean continueOnError) {
      _continueOnError = continueOnError;
      return this;
    }

    public Common build() {
      return new Common(this);
    }
  }

  final class Common implements IndexCreationContext {
    // Identity. Non-overridable shape accessors delegate to `_columnShape`.
    private final String _tableNameWithType;
    private final File _indexDir;
    private final ColumnShape _columnShape;

    // Overridable column attributes.
    private final int _lengthOfShortestElement;
    private final int _lengthOfLongestElement;
    private final boolean _isAscii;
    private final int _totalNumberOfEntries;
    private final int _maxNumberOfMultiValues;
    private final int _maxRowLengthInBytes;
    private final boolean _hasDictionary;

    // Build-time toggles.
    private final boolean _onHeap;
    private final boolean _optimizeDictionary;
    private final boolean _textCommitOnClose;

    // Realtime-conversion context.
    private final boolean _realtimeConversion;
    @Nullable
    private final File _consumerDir;
    private final boolean _mutableSegmentCompacted;
    @Nullable
    private final int[] _mutableToImmutableDocIdMap;

    // Error handling.
    private final boolean _continueOnError;

    private Common(Builder builder) {
      _tableNameWithType = builder._tableNameWithType;
      _indexDir = builder._indexDir;
      _columnShape = builder._columnShape;
      _lengthOfShortestElement = builder._lengthOfShortestElement;
      _lengthOfLongestElement = builder._lengthOfLongestElement;
      _isAscii = builder._isAscii;
      _totalNumberOfEntries = builder._totalNumberOfEntries;
      _maxNumberOfMultiValues = builder._maxNumberOfMultiValues;
      _maxRowLengthInBytes = builder._maxRowLengthInBytes;
      _hasDictionary = builder._hasDictionary;
      _onHeap = builder._onHeap;
      _optimizeDictionary = builder._optimizedDictionary;
      _textCommitOnClose = builder._textCommitOnClose;
      _realtimeConversion = builder._realtimeConversion;
      _consumerDir = builder._consumerDir;
      _mutableSegmentCompacted = builder._mutableSegmentCompacted;
      _mutableToImmutableDocIdMap = builder._mutableToImmutableDocIdMap;
      _continueOnError = builder._continueOnError;
    }

    // Identity accessors.

    @Override
    public String getTableNameWithType() {
      return _tableNameWithType;
    }

    @Override
    public File getIndexDir() {
      return _indexDir;
    }

    @Override
    public FieldSpec getFieldSpec() {
      return _columnShape.getFieldSpec();
    }

    @Override
    @Nullable
    public ColumnStatistics getColumnStatistics() {
      return _columnShape instanceof ColumnStatistics ? (ColumnStatistics) _columnShape : null;
    }

    @Override
    @Nullable
    public ColumnMetadata getColumnMetadata() {
      return _columnShape instanceof ColumnMetadata ? (ColumnMetadata) _columnShape : null;
    }

    // Non-overridable shape accessors delegate to the source [ColumnShape].

    @Override
    public int getTotalDocs() {
      return _columnShape.getTotalDocs();
    }

    @Override
    public int getCardinality() {
      return _columnShape.getCardinality();
    }

    @Override
    public boolean isSorted() {
      return _columnShape.isSorted();
    }

    @Override
    @Nullable
    public Comparable<?> getMinValue() {
      return _columnShape.getMinValue();
    }

    @Override
    @Nullable
    public Comparable<?> getMaxValue() {
      return _columnShape.getMaxValue();
    }

    @Override
    @Nullable
    public PartitionFunction getPartitionFunction() {
      return _columnShape.getPartitionFunction();
    }

    @Override
    @Nullable
    public Set<Integer> getPartitions() {
      return _columnShape.getPartitions();
    }

    // Overridable column-attribute accessors.

    @Override
    public int getLengthOfShortestElement() {
      return _lengthOfShortestElement;
    }

    @Override
    public int getLengthOfLongestElement() {
      return _lengthOfLongestElement;
    }

    @Override
    public boolean isAscii() {
      return _isAscii;
    }

    @Override
    public int getTotalNumberOfEntries() {
      return _totalNumberOfEntries;
    }

    @Override
    public int getMaxNumberOfMultiValues() {
      return _maxNumberOfMultiValues;
    }

    @Override
    public int getMaxRowLengthInBytes() {
      return _maxRowLengthInBytes;
    }

    @Override
    public boolean hasDictionary() {
      return _hasDictionary;
    }

    // Build-time toggles.

    @Override
    public boolean isOnHeap() {
      return _onHeap;
    }

    @Override
    public boolean isOptimizeDictionary() {
      return _optimizeDictionary;
    }

    @Override
    public boolean isTextCommitOnClose() {
      return _textCommitOnClose;
    }

    // Realtime-conversion context.

    @Override
    public boolean isRealtimeConversion() {
      return _realtimeConversion;
    }

    @Override
    @Nullable
    public File getConsumerDir() {
      return _consumerDir;
    }

    @Override
    public boolean isMutableSegmentCompacted() {
      return _mutableSegmentCompacted;
    }

    @Override
    @Nullable
    public int[] getMutableToImmutableDocIdMap() {
      return _mutableToImmutableDocIdMap;
    }

    // Error handling.

    @Override
    public boolean isContinueOnError() {
      return _continueOnError;
    }
  }
}
