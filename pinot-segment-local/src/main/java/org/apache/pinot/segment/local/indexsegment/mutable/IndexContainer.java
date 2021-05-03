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
package org.apache.pinot.segment.local.indexsegment.mutable;

import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.realtime.impl.geospatial.MutableH3Index;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.RealtimeInvertedIndexReader;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.RealtimeLuceneTextIndexReader;
import org.apache.pinot.segment.local.realtime.impl.json.MutableJsonIndex;
import org.apache.pinot.segment.local.realtime.impl.nullvalue.MutableNullValueVector;
import org.apache.pinot.segment.local.segment.index.column.PhysicalColumnIndexContainer;
import org.apache.pinot.segment.local.segment.index.datasource.MutableDataSource;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.reader.BloomFilterReader;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.H3IndexReader;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;
import org.apache.pinot.segment.spi.index.reader.MutableDictionary;
import org.apache.pinot.segment.spi.index.reader.MutableForwardIndex;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.data.FieldSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class IndexContainer implements ColumnIndexContainer {
  final FieldSpec _fieldSpec;
  final PartitionFunction _partitionFunction;
  final Set<Integer> _partitions;
  final MutableSegmentImpl.NumValuesInfo _numValuesInfo;
  final MutableForwardIndex _forwardIndex;
  final MutableDictionary _dictionary;
  final RealtimeInvertedIndexReader _invertedIndex;
  final InvertedIndexReader _rangeIndex;
  final MutableH3Index _h3Index;
  final RealtimeLuceneTextIndexReader _textIndex;
  final boolean _enableFST;
  final MutableJsonIndex _jsonIndex;
  final BloomFilterReader _bloomFilter;
  final MutableNullValueVector _nullValueVector;
  private static final Logger _logger = LoggerFactory.getLogger(PhysicalColumnIndexContainer.class);

  volatile Comparable _minValue;
  volatile Comparable _maxValue;

  // Hold the dictionary id for the latest record
  int _dictId = Integer.MIN_VALUE;
  int[] _dictIds;

  IndexContainer(FieldSpec fieldSpec, @Nullable PartitionFunction partitionFunction,
      @Nullable Set<Integer> partitions, MutableSegmentImpl.NumValuesInfo numValuesInfo, MutableForwardIndex forwardIndex,
      @Nullable MutableDictionary dictionary, @Nullable RealtimeInvertedIndexReader invertedIndex,
      @Nullable InvertedIndexReader rangeIndex, @Nullable RealtimeLuceneTextIndexReader textIndex, boolean enableFST,
      @Nullable MutableJsonIndex jsonIndex, @Nullable MutableH3Index h3Index, @Nullable BloomFilterReader bloomFilter,
      @Nullable MutableNullValueVector nullValueVector) {
    _fieldSpec = fieldSpec;
    _partitionFunction = partitionFunction;
    _partitions = partitions;
    _numValuesInfo = numValuesInfo;
    _forwardIndex = forwardIndex;
    _dictionary = dictionary;
    _invertedIndex = invertedIndex;
    _rangeIndex = rangeIndex;
    _h3Index = h3Index;

    _textIndex = textIndex;
    _enableFST = enableFST;
    _jsonIndex = jsonIndex;
    _bloomFilter = bloomFilter;
    _nullValueVector = nullValueVector;
  }

  DataSource toDataSource(int numDocsIndexed) {
    return new MutableDataSource(_fieldSpec, numDocsIndexed, _numValuesInfo._numValues,
        _numValuesInfo._maxNumValuesPerMVEntry, _partitionFunction, _partitions, _minValue, _maxValue, _forwardIndex,
        _dictionary, _invertedIndex, _rangeIndex, _textIndex, _enableFST, _jsonIndex, _h3Index, _bloomFilter,
        _nullValueVector);
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
  public TextIndexReader getTextIndex() {
    return _textIndex;
  }

  @Override
  public TextIndexReader getFSTIndex() {
    return null;
  }

  @Override
  public JsonIndexReader getJsonIndex() {
    return _jsonIndex;
  }

  @Override
  public H3IndexReader getH3Index() {
    return _h3Index;
  }

  @Override
  public Dictionary getDictionary() {
    return _dictionary;
  }

  @Override
  public BloomFilterReader getBloomFilter() {
    return _bloomFilter;
  }

  @Override
  public NullValueVectorReader getNullValueVector() {
    return _nullValueVector;
  }

  @Override
  public void close() {
    String column = _fieldSpec.getName();
    try {
      _forwardIndex.close();
    } catch (Exception e) {
      _logger.error("Caught exception while closing forward index for column: {}, continuing with error", column, e);
    }
    if (_dictionary != null) {
      try {
        _dictionary.close();
      } catch (Exception e) {
        _logger.error("Caught exception while closing dictionary for column: {}, continuing with error", column, e);
      }
    }
    if (_invertedIndex != null) {
      try {
        _invertedIndex.close();
      } catch (Exception e) {
        _logger
            .error("Caught exception while closing inverted index for column: {}, continuing with error", column, e);
      }
    }
    if (_rangeIndex != null) {
      try {
        _rangeIndex.close();
      } catch (Exception e) {
        _logger.error("Caught exception while closing range index for column: {}, continuing with error", column, e);
      }
    }
    if (_textIndex != null) {
      try {
        _textIndex.close();
      } catch (Exception e) {
        _logger.error("Caught exception while closing text index for column: {}, continuing with error", column, e);
      }
    }
    if (_jsonIndex != null) {
      try {
        _jsonIndex.close();
      } catch (Exception e) {
        _logger.error("Caught exception while closing json index for column: {}, continuing with error", column, e);
      }
    }
    if (_h3Index != null) {
      try {
        _h3Index.close();
      } catch (Exception e) {
        _logger.error("Caught exception while closing H3 index for column: {}, continuing with error", column, e);
      }
    }
    if (_bloomFilter != null) {
      try {
        _bloomFilter.close();
      } catch (Exception e) {
        _logger.error("Caught exception while closing bloom filter for column: {}, continuing with error", column, e);
      }
    }
  }
}