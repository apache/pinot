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
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.index.creator.H3IndexConfig;
import org.apache.pinot.spi.config.table.BloomFilterConfig;
import org.apache.pinot.spi.config.table.FSTType;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * Provides parameters for constructing indexes via {@see IndexCreatorProvider}.
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

  int getTotalNumberOfEntries();

  int getTotalDocs();

  boolean hasDictionary();

  final class Builder {
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

    public Builder withColumnIndexCreationInfo(ColumnIndexCreationInfo columnIndexCreationInfo) {
      return withLengthOfLongestEntry(columnIndexCreationInfo.getLengthOfLongestEntry())
          .withMaxNumberOfMultiValueElements(columnIndexCreationInfo.getMaxNumberOfMultiValueElements())
          .withMaxRowLengthInBytes(columnIndexCreationInfo.getMaxRowLengthInBytes());
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
          .withDictionary(columnMetadata.hasDictionary());
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

    public Common build() {
      return new Common(Objects.requireNonNull(_indexDir),
          _lengthOfLongestEntry, _maxNumberOfMultiValueElements, _maxRowLengthInBytes,
          _onHeap, Objects.requireNonNull(_fieldSpec),
          _sorted, _cardinality, _totalNumberOfEntries, _totalDocs, _hasDictionary);
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
    private final int _totalNumberOfEntries;
    private final int _totalDocs;
    private final boolean _hasDictionary;

    public Common(File indexDir, int lengthOfLongestEntry,
        int maxNumberOfMultiValueElements, int maxRowLengthInBytes, boolean onHeap,
        FieldSpec fieldSpec, boolean sorted, int cardinality, int totalNumberOfEntries,
        int totalDocs, boolean hasDictionary) {
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

    public int getTotalNumberOfEntries() {
      return _totalNumberOfEntries;
    }

    public int getTotalDocs() {
      return _totalDocs;
    }

    public boolean hasDictionary() {
      return _hasDictionary;
    }

    public BloomFilter forBloomFilter(BloomFilterConfig bloomFilterConfig) {
      return new BloomFilter(this, bloomFilterConfig);
    }

    public Forward forForwardIndex(ChunkCompressionType chunkCompressionType,
        @Nullable Map<String, Map<String, String>> columnProperties) {
      return new Forward(this, chunkCompressionType, columnProperties);
    }

    public Text forFSTIndex(FSTType fstType, String[] sortedUniqueElementsArray) {
      return new Text(this, fstType, sortedUniqueElementsArray);
    }

    public Geospatial forGeospatialIndex(H3IndexConfig h3IndexConfig) {
      return new Geospatial(this, h3IndexConfig);
    }

    public Inverted forInvertedIndex() {
      return new Inverted(this);
    }

    public Json forJsonIndex() {
      return new Json(this);
    }

    public Range forRangeIndex(int rangeIndexVersion, Comparable<?> min, Comparable<?> max) {
      return new Range(this, rangeIndexVersion, min, max);
    }

    public Text forTextIndex(boolean commitOnClose) {
      return new Text(this, commitOnClose);
    }
  }

  class Wrapper implements IndexCreationContext {

    private final IndexCreationContext _delegate;

    Wrapper(IndexCreationContext delegate) {
      _delegate = delegate;
    }

    @Override
    public FieldSpec getFieldSpec() {
      return _delegate.getFieldSpec();
    }

    @Override
    public File getIndexDir() {
      return _delegate.getIndexDir();
    }

    @Override
    public boolean isOnHeap() {
      return _delegate.isOnHeap();
    }

    @Override
    public int getLengthOfLongestEntry() {
      return _delegate.getLengthOfLongestEntry();
    }

    @Override
    public int getMaxNumberOfMultiValueElements() {
      return _delegate.getMaxNumberOfMultiValueElements();
    }

    @Override
    public int getMaxRowLengthInBytes() {
      return _delegate.getMaxRowLengthInBytes();
    }

    @Override
    public boolean isSorted() {
      return _delegate.isSorted();
    }

    @Override
    public int getCardinality() {
      return _delegate.getCardinality();
    }

    @Override
    public int getTotalNumberOfEntries() {
      return _delegate.getTotalNumberOfEntries();
    }

    @Override
    public int getTotalDocs() {
      return _delegate.getTotalDocs();
    }

    @Override
    public boolean hasDictionary() {
      return _delegate.hasDictionary();
    }
  }

  class BloomFilter extends Wrapper {

    private final BloomFilterConfig _bloomFilterConfig;

    public BloomFilter(IndexCreationContext wrapped, BloomFilterConfig bloomFilterConfig) {
      super(wrapped);
      _bloomFilterConfig = bloomFilterConfig;
    }

    @Nullable
    public BloomFilterConfig getBloomFilterConfig() {
      return _bloomFilterConfig;
    }
  }

  class Forward extends Wrapper {

    private final ChunkCompressionType _chunkCompressionType;
    private final Map<String, Map<String, String>> _columnProperties;

    Forward(IndexCreationContext delegate,
        ChunkCompressionType chunkCompressionType,
        @Nullable Map<String, Map<String, String>> columnProperties) {
      super(delegate);
      _chunkCompressionType = chunkCompressionType;
      _columnProperties = columnProperties;
    }

    public ChunkCompressionType getChunkCompressionType() {
      return _chunkCompressionType;
    }

    @Nullable
    public Map<String, Map<String, String>> getColumnProperties() {
      return _columnProperties;
    }
  }

  class Geospatial extends Wrapper {

    private final H3IndexConfig _h3IndexConfig;

    Geospatial(IndexCreationContext delegate, H3IndexConfig h3IndexConfig) {
      super(delegate);
      _h3IndexConfig = h3IndexConfig;
    }

    public H3IndexConfig getH3IndexConfig() {
      return _h3IndexConfig;
    }
  }

  class Inverted extends Wrapper {

    Inverted(IndexCreationContext delegate) {
      super(delegate);
    }
  }

  class Json extends Wrapper {

    Json(IndexCreationContext delegate) {
      super(delegate);
    }
  }

  class Range extends Wrapper {

    private final Comparable<?> _min;
    private final Comparable<?> _max;
    private final int _rangeIndexVersion;


    Range(IndexCreationContext delegate, int rangeIndexVersion, Comparable<?> min, Comparable<?> max) {
      super(delegate);
      _rangeIndexVersion = rangeIndexVersion;
      _min = min;
      _max = max;
    }

    public Comparable<?> getMin() {
      return _min;
    }

    public Comparable<?> getMax() {
      return _max;
    }

    public int getRangeIndexVersion() {
      return _rangeIndexVersion;
    }
  }

  class Text extends Wrapper {
    private final boolean _commitOnClose;
    private final boolean _isFst;
    private final FSTType _fstType;
    private final String[] _sortedUniqueElementsArray;

    /**
     * For text indexes
     */
    public Text(IndexCreationContext wrapped, boolean commitOnClose) {
      super(wrapped);
      _commitOnClose = commitOnClose;
      _fstType = null;
      _sortedUniqueElementsArray = null;
      _isFst = false;
    }

    /**
     * For FST indexes
     */
    public Text(IndexCreationContext wrapped, FSTType fstType, String[] sortedUniqueElementsArray) {
      super(wrapped);
      _commitOnClose = true;
      _fstType = fstType;
      _sortedUniqueElementsArray = sortedUniqueElementsArray;
      _isFst = true;
    }

    public boolean isCommitOnClose() {
      return _commitOnClose;
    }

    public FSTType getFstType() {
      return _fstType;
    }

    public boolean isFst() {
      return _isFst;
    }

    public String[] getSortedUniqueElementsArray() {
      return _sortedUniqueElementsArray;
    }
  }
}
