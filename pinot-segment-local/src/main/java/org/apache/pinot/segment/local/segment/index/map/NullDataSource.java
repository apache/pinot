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

package org.apache.pinot.segment.local.segment.index.map;

import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.reader.BloomFilterReader;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.index.reader.H3IndexReader;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;
import org.apache.pinot.segment.spi.index.reader.MapIndexReader;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.segment.spi.index.reader.RangeIndexReader;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.segment.spi.index.reader.VectorIndexReader;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * If a key does not exist in a Map Column, then the Map Data Source will return this NulLDataSource.
 * The NullDataSource represents an INT column where every document has the Default Null Value.  Semantically,
 * this means that if a key is not in a Map column, then the value will always resolve to "Null".
 */
public class NullDataSource implements DataSource {
  private final NullDataSourceMetadata _md;
  private final ColumnIndexContainer _indexes;

  public NullDataSource(String name) {
    _md = new NullDataSourceMetadata(name);
    _indexes = new ColumnIndexContainer.FromMap(Map.of(StandardIndexes.forward(), new NullForwardIndex()));
  }

  @Override
  public DataSourceMetadata getDataSourceMetadata() {
    return _md;
  }

  @Override
  public ColumnIndexContainer getIndexContainer() {
    return _indexes;
  }

  @Override
  public <R extends IndexReader> R getIndex(IndexType<?, R, ?> type) {
    return type.getIndexReader(_indexes);
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
  public NullValueVectorReader getNullValueVector() {
    return getIndex(StandardIndexes.nullValueVector());
  }

  @Nullable
  @Override
  public VectorIndexReader getVectorIndex() {
    return getIndex(StandardIndexes.vector());
  }

  @Nullable
  @Override
  public MapIndexReader getMapIndex() {
    return getIndex(StandardIndexes.map());
  }

  public static class NullDataSourceMetadata implements DataSourceMetadata {
    String _name;

    NullDataSourceMetadata(String name) {
      _name = name;
    }

    @Override
    public FieldSpec getFieldSpec() {
      return new DimensionFieldSpec(_name, FieldSpec.DataType.INT, true);
    }

    @Override
    public boolean isSorted() {
      return false;
    }

    @Override
    public int getNumDocs() {
      return 0;
    }

    @Override
    public int getNumValues() {
      return 0;
    }

    @Override
    public int getMaxNumValuesPerMVEntry() {
      return 0;
    }

    @Nullable
    @Override
    public Comparable getMinValue() {
      return FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT;
    }

    @Nullable
    @Override
    public Comparable getMaxValue() {
      return FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT;
    }

    @Nullable
    @Override
    public PartitionFunction getPartitionFunction() {
      return null;
    }

    @Nullable
    @Override
    public Set<Integer> getPartitions() {
      return null;
    }

    @Override
    public int getCardinality() {
      return 1;
    }
  }

  public class NullForwardIndex implements ForwardIndexReader<ForwardIndexReaderContext> {
    NullForwardIndex() {
    }

    @Override
    public boolean isDictionaryEncoded() {
      return false;
    }

    @Override
    public boolean isSingleValue() {
      return false;
    }

    @Override
    public FieldSpec.DataType getStoredType() {
      return FieldSpec.DataType.INT;
    }

    @Override
    public int getInt(int docId, ForwardIndexReaderContext context) {
      return FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT;
    }

    @Override
    public long getLong(int docId, ForwardIndexReaderContext context) {
      throw new UnsupportedOperationException();
    }

    @Override
    public float getFloat(int docId, ForwardIndexReaderContext context) {
      throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(int docId, ForwardIndexReaderContext context) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getString(int docId, ForwardIndexReaderContext context) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
    }
  }
}
