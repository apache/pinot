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
package org.apache.pinot.segment.local.segment.index.column;

import java.io.IOException;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.index.datasource.MutableDataSource;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.reader.BloomFilterReader;
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


public class IntermediateIndexContainer implements ColumnIndexContainer {
  private static final Logger LOGGER = LoggerFactory.getLogger(IntermediateIndexContainer.class);

  final FieldSpec _fieldSpec;
  final PartitionFunction _partitionFunction;
  final Set<Integer> _partitions;
  final NumValuesInfo _numValuesInfo;
  final MutableForwardIndex _forwardIndex;
  final MutableDictionary _dictionary;

  volatile Comparable _minValue;
  volatile Comparable _maxValue;

  // Hold the dictionary id for the latest record
  int _dictId = Integer.MIN_VALUE;
  int[] _dictIds;

  public IntermediateIndexContainer(FieldSpec fieldSpec, @Nullable PartitionFunction partitionFunction,
      @Nullable Set<Integer> partitions, NumValuesInfo numValuesInfo, MutableForwardIndex forwardIndex,
      MutableDictionary dictionary) {
    _fieldSpec = fieldSpec;
    _partitionFunction = partitionFunction;
    _partitions = partitions;
    _numValuesInfo = numValuesInfo;
    _forwardIndex = forwardIndex;
    _dictionary = dictionary;
  }

  public DataSource toDataSource(int numDocsIndexed) {
    return new MutableDataSource(_fieldSpec, numDocsIndexed, _numValuesInfo._numValues,
        _numValuesInfo._maxNumValuesPerMVEntry, _partitionFunction, _partitions, _minValue, _maxValue, _forwardIndex,
        _dictionary, null, null, null, false, null, null, null, null);
  }

  @Override
  public void close()
      throws IOException {
    String column = _fieldSpec.getName();
    try {
      _forwardIndex.close();
    } catch (Exception e) {
      LOGGER.error("Caught exception while closing forward index for column: {}, continuing with error", column, e);
    }
    if (_dictionary != null) {
      try {
        _dictionary.close();
      } catch (Exception e) {
        LOGGER.error("Caught exception while closing dictionary for column: {}, continuing with error", column, e);
      }
    }
  }

  public FieldSpec getFieldSpec() {
    return _fieldSpec;
  }

  public NumValuesInfo getNumValuesInfo() {
    return _numValuesInfo;
  }

  public MutableForwardIndex getForwardIndex() {
    return _forwardIndex;
  }

  @Override
  public InvertedIndexReader<?> getInvertedIndex() {
    return null;
  }

  @Override
  public InvertedIndexReader<?> getRangeIndex() {
    return null;
  }

  @Override
  public TextIndexReader getTextIndex() {
    return null;
  }

  @Override
  public TextIndexReader getFSTIndex() {
    return null;
  }

  @Override
  public JsonIndexReader getJsonIndex() {
    return null;
  }

  @Override
  public H3IndexReader getH3Index() {
    return null;
  }

  public MutableDictionary getDictionary() {
    return _dictionary;
  }

  @Override
  public BloomFilterReader getBloomFilter() {
    return null;
  }

  @Override
  public NullValueVectorReader getNullValueVector() {
    return null;
  }

  public int getDictId() {
    return _dictId;
  }

  public void setDictId(int dictId) {
    _dictId = dictId;
  }

  public int[] getDictIds() {
    return _dictIds;
  }

  public void setDictIds(int[] dictIds) {
    _dictIds = dictIds;
  }

  public Comparable getMinValue() {
    return _minValue;
  }

  public void setMinValue(Comparable minValue) {
    _minValue = minValue;
  }

  public Comparable getMaxValue() {
    return _maxValue;
  }

  public void setMaxValue(Comparable maxValue) {
    _maxValue = maxValue;
  }
}
