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
package org.apache.pinot.segment.local.segment.index.datasource;

import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.reader.BloomFilterReader;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.H3IndexReader;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;


public abstract class BaseDataSource implements DataSource {
  private final DataSourceMetadata _dataSourceMetadata;
  private final ForwardIndexReader<?> _forwardIndex;
  private final Dictionary _dictionary;
  private final InvertedIndexReader<?> _invertedIndex;
  private final InvertedIndexReader<?> _rangeIndex;
  private final TextIndexReader _textIndex;
  private final TextIndexReader _fstIndex;
  private final JsonIndexReader _jsonIndex;
  private final H3IndexReader _h3Index;
  private final BloomFilterReader _bloomFilter;
  private final NullValueVectorReader _nullValueVector;

  public BaseDataSource(DataSourceMetadata dataSourceMetadata, ForwardIndexReader<?> forwardIndex,
      @Nullable Dictionary dictionary, @Nullable InvertedIndexReader<?> invertedIndex,
      @Nullable InvertedIndexReader<?> rangeIndex, @Nullable TextIndexReader textIndex,
      @Nullable TextIndexReader fstIndex, @Nullable JsonIndexReader jsonIndex, @Nullable H3IndexReader h3Index,
      @Nullable BloomFilterReader bloomFilter, @Nullable NullValueVectorReader nullValueVector) {
    _dataSourceMetadata = dataSourceMetadata;
    _forwardIndex = forwardIndex;
    _dictionary = dictionary;
    _invertedIndex = invertedIndex;
    _rangeIndex = rangeIndex;
    _textIndex = textIndex;
    _fstIndex = fstIndex;
    _jsonIndex = jsonIndex;
    _h3Index = h3Index;
    _bloomFilter = bloomFilter;
    _nullValueVector = nullValueVector;
  }

  @Override
  public DataSourceMetadata getDataSourceMetadata() {
    return _dataSourceMetadata;
  }

  @Override
  public ForwardIndexReader<?> getForwardIndex() {
    return _forwardIndex;
  }

  @Nullable
  @Override
  public Dictionary getDictionary() {
    return _dictionary;
  }

  @Nullable
  @Override
  public InvertedIndexReader<?> getInvertedIndex() {
    return _invertedIndex;
  }

  @Nullable
  @Override
  public InvertedIndexReader<?> getRangeIndex() {
    return _rangeIndex;
  }

  @Nullable
  @Override
  public TextIndexReader getTextIndex() {
    return _textIndex;
  }

  @Nullable
  @Override
  public TextIndexReader getFSTIndex() {
    return _fstIndex;
  }

  @Nullable
  @Override
  public JsonIndexReader getJsonIndex() {
    return _jsonIndex;
  }

  @Nullable
  @Override
  public H3IndexReader getH3Index() {
    return _h3Index;
  }

  @Nullable
  @Override
  public BloomFilterReader getBloomFilter() {
    return _bloomFilter;
  }

  @Nullable
  @Override
  public NullValueVectorReader getNullValueVector() {
    return _nullValueVector;
  }
}
