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
import java.util.Objects;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * Provides parameters for constructing indexes via
 * {@link IndexType#createIndexCreator(IndexCreationContext, IndexConfig)}.
 * The responsibility for ensuring that the correct parameters for a particular
 * index type lies with the caller.
 */
public interface IndexCreationContext {

  FieldSpec getFieldSpec();

  File getIndexDir();

  boolean isOnHeap();

  int getLengthOfLongestEntry();

  int getMaxNumberOfMultiValueElements();

  int getMaxRowLengthInBytes();

  boolean isSorted();

  int getCardinality();

  long getTotalNumberOfEntries();

  int getTotalDocs();

  boolean hasDictionary();

  Comparable<?> getMinValue();

  Comparable<?> getMaxValue();

  boolean forwardIndexDisabled();

  /**
   * Returns a sorted array with the unique values for the associated column.
   *
   * Primitive types will be stored in an unboxed array (ie if the column contains {@code int}s, this method returns an
   * {@code int[]}).
   *
   * This is an abstraction leak from Text and FST indexes.
   */
  Object getSortedUniqueElementsArray();

  /**
   * This could be set for all metrics in {@link IndexingConfig#isOptimizeDictionary()} or only when the field is a
   * metric with {@link IndexingConfig#isOptimizeDictionaryForMetrics()}, in which case this method will only return
   * true if the column is a metric.
   *
   * Therefore the caller code doesn't need to verify the later condition.
   */
  boolean isOptimizeDictionary();

  boolean isFixedLength();

  /**
   * This is an abstraction leak from TextIndexType.
   * @return
   */
  boolean isTextCommitOnClose();

  ColumnStatistics getColumnStatistics();
  /**
   * This flags whether the index creation is done during realtime segment conversion
   * @return
   */
  boolean isRealtimeConversion();

  /**
   * Used in conjunction with isRealtimeConversion, this returns the location of the consumer directory used
   */
  File getConsumerDir();

  /**
   * This contains immutableToMutableIdMap mapping generated in {@link SegmentIndexCreationDriver}
   *
   * This allows for index creation during realtime segment conversion to take advantage of mutable to immutable
   * docId mapping
   * @return
   */
  int[] getImmutableToMutableIdMap();

  final class Builder {
    private ColumnStatistics _columnStatistics;
    private File _indexDir;
    private int _lengthOfLongestEntry;
    private int _maxNumberOfMultiValueElements;
    private int _maxRowLengthInBytes;
    private boolean _onHeap = false;
    private FieldSpec _fieldSpec;
    private boolean _sorted;
    private int _cardinality;
    private int _totalNumberOfEntries;
    private int _totalDocs;
    private boolean _hasDictionary = true;
    private Comparable<?> _minValue;
    private Comparable<?> _maxValue;
    private boolean _forwardIndexDisabled;
    private Object _sortedUniqueElementsArray;
    private boolean _optimizedDictionary;
    private boolean _fixedLength;
    private boolean _textCommitOnClose;
    private boolean _realtimeConversion = false;
    private File _consumerDir;
    private int[] _immutableToMutableIdMap;

    public Builder withColumnIndexCreationInfo(ColumnIndexCreationInfo columnIndexCreationInfo) {
      return withLengthOfLongestEntry(columnIndexCreationInfo.getLengthOfLongestEntry())
          .withMaxNumberOfMultiValueElements(columnIndexCreationInfo.getMaxNumberOfMultiValueElements())
          .withMaxRowLengthInBytes(columnIndexCreationInfo.getMaxRowLengthInBytes())
          .withMinValue((Comparable<?>) columnIndexCreationInfo.getMin())
          .withMaxValue((Comparable<?>) columnIndexCreationInfo.getMax())
          .withTotalNumberOfEntries(columnIndexCreationInfo.getTotalNumberOfEntries())
          .withSortedUniqueElementsArray(columnIndexCreationInfo.getSortedUniqueElementsArray())
          .withColumnStatistics(columnIndexCreationInfo.getColumnStatistics())
          .withCardinality(columnIndexCreationInfo.getDistinctValueCount())
          .withFixedLength(columnIndexCreationInfo.isFixedLength())
          .sorted(columnIndexCreationInfo.isSorted());
    }

    public Builder withColumnStatistics(ColumnStatistics columnStatistics) {
      _columnStatistics = columnStatistics;
      return this;
    }

    public Builder withIndexDir(File indexDir) {
      _indexDir = indexDir;
      return this;
    }

    public Builder onHeap(boolean onHeap) {
      _onHeap = onHeap;
      return this;
    }

    public Builder withColumnMetadata(ColumnMetadata columnMetadata) {
      return withFieldSpec(columnMetadata.getFieldSpec())
          .sorted(columnMetadata.isSorted())
          .withCardinality(columnMetadata.getCardinality())
          .withTotalNumberOfEntries(columnMetadata.getTotalNumberOfEntries())
          .withTotalDocs(columnMetadata.getTotalDocs())
          .withDictionary(columnMetadata.hasDictionary())
          .withMinValue(columnMetadata.getMinValue())
          .withMaxValue(columnMetadata.getMaxValue())
          .withMaxNumberOfMultiValueElements(columnMetadata.getMaxNumberOfMultiValues());
    }

    public Builder withOptimizedDictionary(boolean optimized) {
      _optimizedDictionary = optimized;
      return this;
    }

    public Builder withFixedLength(boolean fixedLength) {
      _fixedLength = fixedLength;
      return this;
    }

    public Builder withLengthOfLongestEntry(int lengthOfLongestEntry) {
      _lengthOfLongestEntry = lengthOfLongestEntry;
      return this;
    }

    public Builder withMaxNumberOfMultiValueElements(int maxNumberOfMultiValueElements) {
      _maxNumberOfMultiValueElements = maxNumberOfMultiValueElements;
      return this;
    }

    public Builder withMaxRowLengthInBytes(int maxRowLengthInBytes) {
      _maxRowLengthInBytes = maxRowLengthInBytes;
      return this;
    }

    public Builder withFieldSpec(FieldSpec fieldSpec) {
      _fieldSpec = fieldSpec;
      return this;
    }

    public Builder sorted(boolean sorted) {
      _sorted = sorted;
      return this;
    }

    public Builder withCardinality(int cardinality) {
      _cardinality = cardinality;
      return this;
    }

    public Builder withTotalNumberOfEntries(int totalNumberOfEntries) {
      _totalNumberOfEntries = totalNumberOfEntries;
      return this;
    }

    public Builder withTotalDocs(int totalDocs) {
      _totalDocs = totalDocs;
      return this;
    }

    public Builder withDictionary(boolean hasDictionary) {
      _hasDictionary = hasDictionary;
      return this;
    }

    public Builder withMinValue(Comparable<?> minValue) {
      _minValue = minValue;
      return this;
    }

    public Builder withMaxValue(Comparable<?> maxValue) {
      _maxValue = maxValue;
      return this;
    }

    public Builder withForwardIndexDisabled(boolean forwardIndexDisabled) {
      _forwardIndexDisabled = forwardIndexDisabled;
      return this;
    }

    public Builder withTextCommitOnClose(boolean textCommitOnClose) {
      _textCommitOnClose = textCommitOnClose;
      return this;
    }

    public Builder withRealtimeConversion(boolean realtimeConversion) {
      _realtimeConversion = realtimeConversion;
      return this;
    }

    public Builder withConsumerDir(File consumerDir) {
      _consumerDir = consumerDir;
      return this;
    }

    public Builder withImmutableToMutableIdMap(int[] immutableToMutableIdMap) {
      _immutableToMutableIdMap = immutableToMutableIdMap;
      return this;
    }

    public Common build() {
      return new Common(Objects.requireNonNull(_indexDir), _lengthOfLongestEntry, _maxNumberOfMultiValueElements,
          _maxRowLengthInBytes, _onHeap, Objects.requireNonNull(_fieldSpec), _sorted, _cardinality,
          _totalNumberOfEntries, _totalDocs, _hasDictionary, _minValue, _maxValue, _forwardIndexDisabled,
          _sortedUniqueElementsArray, _optimizedDictionary, _fixedLength, _textCommitOnClose, _columnStatistics,
          _realtimeConversion, _consumerDir, _immutableToMutableIdMap);
    }

    public Builder withSortedUniqueElementsArray(Object sortedUniqueElementsArray) {
      _sortedUniqueElementsArray = sortedUniqueElementsArray;
      return this;
    }
  }

  static Builder builder() {
    return new Builder();
  }

  final class Common implements IndexCreationContext {

    private final File _indexDir;
    private final int _lengthOfLongestEntry;
    private final int _maxNumberOfMultiValueElements;
    private final int _maxRowLengthInBytes;
    private final boolean _onHeap;
    private final FieldSpec _fieldSpec;
    private final boolean _sorted;
    private final int _cardinality;
    private final long _totalNumberOfEntries;
    private final int _totalDocs;
    private final boolean _hasDictionary;
    private final Comparable<?> _minValue;
    private final Comparable<?> _maxValue;
    private final boolean _forwardIndexDisabled;
    private final Object _sortedUniqueElementsArray;
    private final boolean _optimizeDictionary;
    private final boolean _fixedLength;
    private final boolean _textCommitOnClose;
    private final ColumnStatistics _columnStatistics;
    private final boolean _realtimeConversion;
    private final File _consumerDir;
    private final int[] _immutableToMutableIdMap;

    public Common(File indexDir, int lengthOfLongestEntry,
        int maxNumberOfMultiValueElements, int maxRowLengthInBytes, boolean onHeap,
        FieldSpec fieldSpec, boolean sorted, int cardinality, int totalNumberOfEntries,
        int totalDocs, boolean hasDictionary, Comparable<?> minValue, Comparable<?> maxValue,
        boolean forwardIndexDisabled, Object sortedUniqueElementsArray, boolean optimizeDictionary, boolean fixedLength,
        boolean textCommitOnClose, ColumnStatistics columnStatistics, boolean realtimeConversion, File consumerDir,
        int[] immutableToMutableIdMap) {
      _indexDir = indexDir;
      _lengthOfLongestEntry = lengthOfLongestEntry;
      _maxNumberOfMultiValueElements = maxNumberOfMultiValueElements;
      _maxRowLengthInBytes = maxRowLengthInBytes;
      _onHeap = onHeap;
      _fieldSpec = fieldSpec;
      _sorted = sorted;
      _cardinality = cardinality;
      _totalNumberOfEntries = totalNumberOfEntries;
      _totalDocs = totalDocs;
      _hasDictionary = hasDictionary;
      _minValue = minValue;
      _maxValue = maxValue;
      _forwardIndexDisabled = forwardIndexDisabled;
      _sortedUniqueElementsArray = sortedUniqueElementsArray;
      _optimizeDictionary = optimizeDictionary;
      _fixedLength = fixedLength;
      _textCommitOnClose = textCommitOnClose;
      _columnStatistics = columnStatistics;
      _realtimeConversion = realtimeConversion;
      _consumerDir = consumerDir;
      _immutableToMutableIdMap = immutableToMutableIdMap;
    }

    public FieldSpec getFieldSpec() {
      return _fieldSpec;
    }

    public File getIndexDir() {
      return _indexDir;
    }

    public boolean isOnHeap() {
      return _onHeap;
    }

    public int getLengthOfLongestEntry() {
      return _lengthOfLongestEntry;
    }

    public int getMaxNumberOfMultiValueElements() {
      return _maxNumberOfMultiValueElements;
    }

    public int getMaxRowLengthInBytes() {
      return _maxRowLengthInBytes;
    }

    public boolean isSorted() {
      return _sorted;
    }

    public int getCardinality() {
      return _cardinality;
    }

    public long getTotalNumberOfEntries() {
      return _totalNumberOfEntries;
    }

    public int getTotalDocs() {
      return _totalDocs;
    }

    public boolean hasDictionary() {
      return _hasDictionary;
    }

    @Override
    public Comparable<?> getMinValue() {
      return _minValue;
    }

    @Override
    public Comparable<?> getMaxValue() {
      return _maxValue;
    }

    @Override
    public boolean forwardIndexDisabled() {
      return _forwardIndexDisabled;
    }

    @Override
    public Object getSortedUniqueElementsArray() {
      return _sortedUniqueElementsArray;
    }

    @Override
    public boolean isOptimizeDictionary() {
      return _optimizeDictionary;
    }

    @Override
    public boolean isFixedLength() {
      return _fixedLength;
    }

    @Override
    public boolean isTextCommitOnClose() {
      return _textCommitOnClose;
    }

    @Override
    public ColumnStatistics getColumnStatistics() {
      return _columnStatistics;
    }

    @Override
    public boolean isRealtimeConversion() {
      return _realtimeConversion;
    }

    @Override
    public File getConsumerDir() {
      return _consumerDir;
    }

    @Override
    public int[] getImmutableToMutableIdMap() {
      return _immutableToMutableIdMap;
    }
  }
}
