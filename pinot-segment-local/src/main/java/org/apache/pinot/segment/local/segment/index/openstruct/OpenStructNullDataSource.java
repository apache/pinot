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
package org.apache.pinot.segment.local.segment.index.openstruct;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.datasource.OpenStructDataSource;
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
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.segment.spi.index.reader.RangeIndexReader;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.segment.spi.index.reader.VectorIndexReader;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/// Typed all-null {@link DataSource} for an OPEN_STRUCT key that is absent from a segment.
///
/// Unlike the MAP column's {@code NullDataSource} (which is hardcoded as INT with numDocs=0),
/// this class uses the correct {@link DataType} from the OPEN_STRUCT child {@link FieldSpec} and
/// reports the segment's actual doc count. Every document is null: the forward index returns the
/// type-correct default null value, and the {@link NullValueVectorReader} returns a full-segment
/// bitmap.
public class OpenStructNullDataSource implements DataSource {
  private final FieldSpec _fieldSpec;
  private final int _numDocs;
  private final ColumnIndexContainer _indexes;
  private final AllNullValueVector _nullVector;

  public OpenStructNullDataSource(FieldSpec fieldSpec, int numDocs) {
    _fieldSpec = fieldSpec;
    _numDocs = numDocs;
    _nullVector = new AllNullValueVector(numDocs);
    _indexes = new ColumnIndexContainer.FromMap(Map.of(
        StandardIndexes.forward(), new TypedNullForwardIndex(fieldSpec.getDataType().getStoredType()),
        StandardIndexes.nullValueVector(), _nullVector));
  }

  /// Creates an all-null DataSource for a key absent from this OPEN_STRUCT segment.
  /// Resolves the key's type from the child FieldSpec, falling back to STRING.
  public static OpenStructNullDataSource forAbsentKey(OpenStructDataSource osDs, String key) {
    FieldSpec childSpec = osDs.getFieldSpec().getChildFieldSpec(key);
    if (childSpec == null) {
      childSpec = new DimensionFieldSpec(key, FieldSpec.DataType.STRING, true);
    }
    int numDocs = osDs.getDataSourceMetadata().getNumDocs();
    return new OpenStructNullDataSource(childSpec, numDocs);
  }

  @Override
  public DataSourceMetadata getDataSourceMetadata() {
    return new AllNullMetadata(_fieldSpec, _numDocs);
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
    return null;
  }

  @Nullable
  @Override
  public InvertedIndexReader<?> getInvertedIndex() {
    return null;
  }

  @Nullable
  @Override
  public RangeIndexReader<?> getRangeIndex() {
    return null;
  }

  @Nullable
  @Override
  public TextIndexReader getTextIndex() {
    return null;
  }

  @Nullable
  @Override
  public TextIndexReader getFSTIndex() {
    return null;
  }

  @Nullable
  @Override
  public TextIndexReader getIFSTIndex() {
    return null;
  }

  @Nullable
  @Override
  public JsonIndexReader getJsonIndex() {
    return null;
  }

  @Nullable
  @Override
  public H3IndexReader getH3Index() {
    return null;
  }

  @Nullable
  @Override
  public BloomFilterReader getBloomFilter() {
    return null;
  }

  @Override
  public NullValueVectorReader getNullValueVector() {
    return _nullVector;
  }

  @Nullable
  @Override
  public VectorIndexReader getVectorIndex() {
    return null;
  }

  /// Forward index that returns the type-correct default null value for every document.
  static class TypedNullForwardIndex implements ForwardIndexReader<ForwardIndexReaderContext> {
    private final DataType _storedType;

    TypedNullForwardIndex(DataType storedType) {
      _storedType = storedType;
    }

    @Override
    public boolean isDictionaryEncoded() {
      return false;
    }

    @Override
    public boolean isSingleValue() {
      return true;
    }

    @Override
    public DataType getStoredType() {
      return _storedType;
    }

    @Override
    public int getInt(int docId, ForwardIndexReaderContext context) {
      return FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT;
    }

    @Override
    public long getLong(int docId, ForwardIndexReaderContext context) {
      return FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG;
    }

    @Override
    public float getFloat(int docId, ForwardIndexReaderContext context) {
      return FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_FLOAT;
    }

    @Override
    public double getDouble(int docId, ForwardIndexReaderContext context) {
      return FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_DOUBLE;
    }

    @Override
    public BigDecimal getBigDecimal(int docId, ForwardIndexReaderContext context) {
      return FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BIG_DECIMAL;
    }

    @Override
    public String getString(int docId, ForwardIndexReaderContext context) {
      return FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING;
    }

    @Override
    public byte[] getBytes(int docId, ForwardIndexReaderContext context) {
      return FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BYTES;
    }

    @Override
    public void close() {
    }
  }

  /// {@link NullValueVectorReader} where every document is null.
  static class AllNullValueVector implements NullValueVectorReader {
    private final ImmutableRoaringBitmap _nullBitmap;

    AllNullValueVector(int numDocs) {
      MutableRoaringBitmap bm = new MutableRoaringBitmap();
      if (numDocs > 0) {
        bm.add(0L, numDocs);
      }
      _nullBitmap = bm.toImmutableRoaringBitmap();
    }

    @Override
    public boolean isNull(int docId) {
      return true;
    }

    @Override
    public ImmutableRoaringBitmap getNullBitmap() {
      return _nullBitmap;
    }
  }

  private static class AllNullMetadata implements DataSourceMetadata {
    private final FieldSpec _fieldSpec;
    private final int _numDocs;

    AllNullMetadata(FieldSpec fieldSpec, int numDocs) {
      _fieldSpec = fieldSpec;
      _numDocs = numDocs;
    }

    @Override
    public FieldSpec getFieldSpec() {
      return _fieldSpec;
    }

    @Override
    public boolean isSorted() {
      return false;
    }

    @Override
    public int getNumDocs() {
      return _numDocs;
    }

    @Override
    public int getNumValues() {
      return _numDocs;
    }

    @Override
    public int getMaxNumValuesPerMVEntry() {
      return 0;
    }

    @Override
    public int getCardinality() {
      return 1;
    }

    @Nullable
    @Override
    public Comparable getMinValue() {
      return null;
    }

    @Nullable
    @Override
    public Comparable getMaxValue() {
      return null;
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
  }
}
