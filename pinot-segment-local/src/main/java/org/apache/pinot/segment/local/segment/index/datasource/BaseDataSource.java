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
import org.apache.pinot.segment.spi.datasource.NullMode;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.reader.BloomFilterReader;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.H3IndexReader;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.segment.spi.index.reader.RangeIndexReader;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;


public abstract class BaseDataSource implements DataSource {
  private final DataSourceMetadata _dataSourceMetadata;
  private final ColumnIndexContainer _indexContainer;

  public BaseDataSource(DataSourceMetadata dataSourceMetadata, ColumnIndexContainer indexContainer) {
    _dataSourceMetadata = dataSourceMetadata;
    _indexContainer = indexContainer;
  }

  @Override
  public DataSourceMetadata getDataSourceMetadata() {
    return _dataSourceMetadata;
  }

  @Override
  public <R extends IndexReader> R getIndex(IndexType<?, R, ?> type) {
    return type.getIndexReader(_indexContainer);
  }

  @Override
  public ForwardIndexReader<?> getForwardIndex() {
    return getIndex(StandardIndexes.forward());
  }

  @Nullable
  @Override
  public Dictionary getDictionary() {
    return getIndex(StandardIndexes.dictionary());
  }

  @Nullable
  @Override
  public InvertedIndexReader<?> getInvertedIndex() {
    return getIndex(StandardIndexes.inverted());
  }

  @Nullable
  @Override
  public RangeIndexReader<?> getRangeIndex() {
    return getIndex(StandardIndexes.range());
  }

  @Nullable
  @Override
  public TextIndexReader getTextIndex() {
    return getIndex(StandardIndexes.text());
  }

  @Nullable
  @Override
  public TextIndexReader getFSTIndex() {
    return getIndex(StandardIndexes.fst());
  }

  @Nullable
  @Override
  public JsonIndexReader getJsonIndex() {
    return getIndex(StandardIndexes.json());
  }

  @Nullable
  @Override
  public H3IndexReader getH3Index() {
    return getIndex(StandardIndexes.h3());
  }

  @Nullable
  @Override
  public BloomFilterReader getBloomFilter() {
    return getIndex(StandardIndexes.bloomFilter());
  }

  @Nullable
  @Override
  public NullValueVectorReader getNullValueVector(NullMode nullMode) {
    switch (nullMode) {
      case NONE_NULLABLE: {
        return null;
      }
      case ALL_NULLABLE: {
        return getIndex(StandardIndexes.nullValueVector());
      }
      case COLUMN_BASED: {
        if (_dataSourceMetadata.getFieldSpec().getNullable()) {
          return getIndex(StandardIndexes.nullValueVector());
        } else {
          return null;
        }
      }
      default: {
        throw new IllegalArgumentException("Mode " + nullMode + " is not recognized");
      }
    }
  }
}
