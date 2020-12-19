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
package org.apache.pinot.core.segment.index.column;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.pinot.core.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.core.segment.index.metadata.ColumnMetadata;
import org.apache.pinot.core.segment.index.readers.BaseImmutableDictionary;
import org.apache.pinot.core.segment.index.readers.BitmapInvertedIndexReader;
import org.apache.pinot.core.segment.index.readers.BloomFilterReader;
import org.apache.pinot.core.segment.index.readers.BytesDictionary;
import org.apache.pinot.core.segment.index.readers.DoubleDictionary;
import org.apache.pinot.core.segment.index.readers.FloatDictionary;
import org.apache.pinot.core.segment.index.readers.ForwardIndexReader;
import org.apache.pinot.core.segment.index.readers.IntDictionary;
import org.apache.pinot.core.segment.index.readers.InvertedIndexReader;
import org.apache.pinot.core.segment.index.readers.JsonIndexReader;
import org.apache.pinot.core.segment.index.readers.LongDictionary;
import org.apache.pinot.core.segment.index.readers.LuceneFSTIndexReader;
import org.apache.pinot.core.segment.index.readers.NullValueVectorReaderImpl;
import org.apache.pinot.core.segment.index.readers.OnHeapDoubleDictionary;
import org.apache.pinot.core.segment.index.readers.OnHeapFloatDictionary;
import org.apache.pinot.core.segment.index.readers.OnHeapIntDictionary;
import org.apache.pinot.core.segment.index.readers.OnHeapLongDictionary;
import org.apache.pinot.core.segment.index.readers.OnHeapStringDictionary;
import org.apache.pinot.core.segment.index.readers.RangeIndexReader;
import org.apache.pinot.core.segment.index.readers.SortedIndexReader;
import org.apache.pinot.core.segment.index.readers.StringDictionary;
import org.apache.pinot.core.segment.index.readers.TextIndexReader;
import org.apache.pinot.core.segment.index.readers.bloom.BloomFilterReaderFactory;
import org.apache.pinot.core.segment.index.readers.forward.FixedBitMVForwardIndexReader;
import org.apache.pinot.core.segment.index.readers.forward.FixedBitSVForwardIndexReaderV2;
import org.apache.pinot.core.segment.index.readers.forward.FixedByteChunkSVForwardIndexReader;
import org.apache.pinot.core.segment.index.readers.forward.VarByteChunkSVForwardIndexReader;
import org.apache.pinot.core.segment.index.readers.geospatial.H3IndexReader;
import org.apache.pinot.core.segment.index.readers.json.ImmutableJsonIndexReader;
import org.apache.pinot.core.segment.index.readers.sorted.SortedIndexReaderImpl;
import org.apache.pinot.core.segment.index.readers.text.LuceneTextIndexReader;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.apache.pinot.core.segment.store.ColumnIndexType;
import org.apache.pinot.core.segment.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.BloomFilterConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class PhysicalColumnIndexContainer implements ColumnIndexContainer {
  private static final Logger LOGGER = LoggerFactory.getLogger(PhysicalColumnIndexContainer.class);

  private final ForwardIndexReader<?> _forwardIndex;
  private final InvertedIndexReader<?> _invertedIndex;
  private final InvertedIndexReader<?> _rangeIndex;
  private final H3IndexReader _h3Index;
  private final TextIndexReader _textIndex;
  private final TextIndexReader _fstIndex;
  private final JsonIndexReader _jsonIndex;
  private final BaseImmutableDictionary _dictionary;
  private final BloomFilterReader _bloomFilter;
  private final NullValueVectorReaderImpl _nullValueVectorReader;

  public PhysicalColumnIndexContainer(SegmentDirectory.Reader segmentReader, ColumnMetadata metadata,
      IndexLoadingConfig indexLoadingConfig, File segmentIndexDir)
      throws IOException {
    String columnName = metadata.getColumnName();
    boolean loadInvertedIndex = indexLoadingConfig.getInvertedIndexColumns().contains(columnName);
    boolean loadRangeIndex = indexLoadingConfig.getRangeIndexColumns().contains(columnName);
    boolean loadH3Index = indexLoadingConfig.getH3IndexColumns().contains(columnName);
    boolean loadTextIndex = indexLoadingConfig.getTextIndexColumns().contains(columnName);
    boolean loadFSTIndex = indexLoadingConfig.getFSTIndexColumns().contains(columnName);
    boolean loadJsonIndex = indexLoadingConfig.getJsonIndexColumns().contains(columnName);
    boolean loadOnHeapDictionary = indexLoadingConfig.getOnHeapDictionaryColumns().contains(columnName);
    BloomFilterConfig bloomFilterConfig = indexLoadingConfig.getBloomFilterConfigs().get(columnName);

    if (segmentReader.hasIndexFor(columnName, ColumnIndexType.NULLVALUE_VECTOR)) {
      PinotDataBuffer nullValueVectorBuffer = segmentReader.getIndexFor(columnName, ColumnIndexType.NULLVALUE_VECTOR);
      _nullValueVectorReader = new NullValueVectorReaderImpl(nullValueVectorBuffer);
    } else {
      _nullValueVectorReader = null;
    }

    if (loadTextIndex) {
      Preconditions.checkState(segmentReader.hasIndexFor(columnName, ColumnIndexType.TEXT_INDEX));
      Map<String, Map<String, String>> columnProperties = indexLoadingConfig.getColumnProperties();
      _textIndex = new LuceneTextIndexReader(columnName, segmentIndexDir, metadata.getTotalDocs(),
          columnProperties.get(columnName));
    } else {
      _textIndex = null;
    }

    if (loadJsonIndex) {
      Preconditions.checkState(segmentReader.hasIndexFor(columnName, ColumnIndexType.JSON_INDEX));
      PinotDataBuffer jsonIndexBuffer = segmentReader.getIndexFor(columnName, ColumnIndexType.JSON_INDEX);
      _jsonIndex = new ImmutableJsonIndexReader(jsonIndexBuffer, metadata.getTotalDocs());
    } else {
      _jsonIndex = null;
    }

    if (bloomFilterConfig != null) {
      PinotDataBuffer bloomFilterBuffer = segmentReader.getIndexFor(columnName, ColumnIndexType.BLOOM_FILTER);
      _bloomFilter = BloomFilterReaderFactory.getBloomFilterReader(bloomFilterBuffer, bloomFilterConfig.isLoadOnHeap());
    } else {
      _bloomFilter = null;
    }

    if (loadH3Index) {
      _h3Index = new H3IndexReader(segmentReader.getIndexFor(columnName, ColumnIndexType.H3_INDEX));
    } else {
      _h3Index = null;
    }
    PinotDataBuffer fwdIndexBuffer = segmentReader.getIndexFor(columnName, ColumnIndexType.FORWARD_INDEX);
    if (metadata.hasDictionary()) {
      // Dictionary-based index
      _dictionary = loadDictionary(segmentReader.getIndexFor(columnName, ColumnIndexType.DICTIONARY), metadata,
          loadOnHeapDictionary);
      if (metadata.isSingleValue()) {
        // Single-value
        if (metadata.isSorted()) {
          // Sorted
          SortedIndexReader<?> sortedIndexReader = new SortedIndexReaderImpl(fwdIndexBuffer, metadata.getCardinality());
          _forwardIndex = sortedIndexReader;
          _invertedIndex = sortedIndexReader;
          _rangeIndex = null;
          _fstIndex = null;
          return;
        } else {
          // Unsorted
          _forwardIndex =
              new FixedBitSVForwardIndexReaderV2(fwdIndexBuffer, metadata.getTotalDocs(), metadata.getBitsPerElement());
        }
      } else {
        // Multi-value
        _forwardIndex = new FixedBitMVForwardIndexReader(fwdIndexBuffer, metadata.getTotalDocs(),
            metadata.getTotalNumberOfEntries(), metadata.getBitsPerElement());
      }
      if (loadInvertedIndex) {
        _invertedIndex =
            new BitmapInvertedIndexReader(segmentReader.getIndexFor(columnName, ColumnIndexType.INVERTED_INDEX),
                metadata.getCardinality());
      } else {
        _invertedIndex = null;
      }

      if (loadFSTIndex) {
        _fstIndex = new LuceneFSTIndexReader(segmentReader.getIndexFor(columnName, ColumnIndexType.FST_INDEX));
      } else {
        _fstIndex = null;
      }

      if (loadRangeIndex) {
        _rangeIndex = new RangeIndexReader(segmentReader.getIndexFor(columnName, ColumnIndexType.RANGE_INDEX));
      } else {
        _rangeIndex = null;
      }

    } else {
      // Raw index
      _forwardIndex = loadRawForwardIndex(fwdIndexBuffer, metadata.getDataType());
      _dictionary = null;
      _rangeIndex = null;
      _invertedIndex = null;
      _fstIndex = null;
    }

  }

  @Override
  public ForwardIndexReader<?> getForwardIndex() {
    return _forwardIndex;
  }

  @Override
  public InvertedIndexReader<?> getInvertedIndex() {
    return _invertedIndex;
  }

  @Override
  public InvertedIndexReader<?> getRangeIndex() {
    return _rangeIndex;
  }

  @Override
  public H3IndexReader getH3Index() {
    return _h3Index;
  }

  @Override
  public TextIndexReader getTextIndex() {
    return _textIndex;
  }

  @Override
  public JsonIndexReader getJsonIndex() {
    return _jsonIndex;
  }

  @Override
  public BaseImmutableDictionary getDictionary() {
    return _dictionary;
  }

  @Override
  public BloomFilterReader getBloomFilter() {
    return _bloomFilter;
  }

  @Override
  public TextIndexReader getFSTIndex() {
    return _fstIndex;
  }

  @Override
  public NullValueVectorReaderImpl getNullValueVector() {
    return _nullValueVectorReader;
  }

  //TODO: move this to a DictionaryLoader class
  public static BaseImmutableDictionary loadDictionary(PinotDataBuffer dictionaryBuffer, ColumnMetadata metadata,
      boolean loadOnHeap) {
    FieldSpec.DataType dataType = metadata.getDataType();
    if (loadOnHeap) {
      String columnName = metadata.getColumnName();
      LOGGER.info("Loading on-heap dictionary for column: {}", columnName);
    }

    int length = metadata.getCardinality();
    switch (dataType) {
      case INT:
        return (loadOnHeap) ? new OnHeapIntDictionary(dictionaryBuffer, length)
            : new IntDictionary(dictionaryBuffer, length);

      case LONG:
        return (loadOnHeap) ? new OnHeapLongDictionary(dictionaryBuffer, length)
            : new LongDictionary(dictionaryBuffer, length);

      case FLOAT:
        return (loadOnHeap) ? new OnHeapFloatDictionary(dictionaryBuffer, length)
            : new FloatDictionary(dictionaryBuffer, length);

      case DOUBLE:
        return (loadOnHeap) ? new OnHeapDoubleDictionary(dictionaryBuffer, length)
            : new DoubleDictionary(dictionaryBuffer, length);

      case STRING:
        int numBytesPerValue = metadata.getColumnMaxLength();
        byte paddingByte = (byte) metadata.getPaddingCharacter();
        return loadOnHeap ? new OnHeapStringDictionary(dictionaryBuffer, length, numBytesPerValue, paddingByte)
            : new StringDictionary(dictionaryBuffer, length, numBytesPerValue, paddingByte);

      case BYTES:
        numBytesPerValue = metadata.getColumnMaxLength();
        return new BytesDictionary(dictionaryBuffer, length, numBytesPerValue);

      default:
        throw new IllegalStateException("Illegal data type for dictionary: " + dataType);
    }
  }

  private static ForwardIndexReader<?> loadRawForwardIndex(PinotDataBuffer forwardIndexBuffer,
      FieldSpec.DataType dataType) {
    switch (dataType) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        return new FixedByteChunkSVForwardIndexReader(forwardIndexBuffer, dataType);
      case STRING:
      case BYTES:
        return new VarByteChunkSVForwardIndexReader(forwardIndexBuffer, dataType);
      default:
        throw new IllegalStateException("Illegal data type for raw forward index: " + dataType);
    }
  }

  @Override
  public void close()
      throws IOException {
    _forwardIndex.close();
    if (_invertedIndex != null) {
      _invertedIndex.close();
    }
    if (_rangeIndex != null) {
      _rangeIndex.close();
    }
    if (_dictionary != null) {
      _dictionary.close();
    }
    if (_textIndex != null) {
      _textIndex.close();
    }
    if (_fstIndex != null) {
      _fstIndex.close();
    }
    if (_jsonIndex != null) {
      _jsonIndex.close();
    }
    if (_bloomFilter != null) {
      _bloomFilter.close();
    }
  }
}
