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
package org.apache.pinot.common.systemtable.datasource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.IntFunction;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.multicolumntext.MultiColumnTextMetadata;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
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
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2Metadata;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.joda.time.Duration;
import org.joda.time.Interval;


/**
 * In-memory {@link IndexSegment} implementation intended for system table queries.
 * <p>
 * This segment is backed by per-column value functions (docId -&gt; value) and exposes raw forward indexes (no
 * dictionaries/inverted indexes) so that the standard v1 query engine can operate on it.
 */
public final class InMemorySystemTableSegment implements IndexSegment {
  private final String _segmentName;
  private final Schema _schema;
  private final int _numDocs;
  private final Map<String, IntFunction<Object>> _valueProvidersByColumn;
  private final Map<String, DataSource> _dataSourcesByColumn;
  private final SegmentMetadata _segmentMetadata;

  public InMemorySystemTableSegment(String segmentName, Schema schema, int numDocs,
      Map<String, IntFunction<Object>> valueProvidersByColumn) {
    _segmentName = segmentName;
    _schema = schema;
    _numDocs = numDocs;
    _valueProvidersByColumn = new HashMap<>(valueProvidersByColumn);
    _dataSourcesByColumn = new HashMap<>(schema.getColumnNames().size());
    for (String column : schema.getColumnNames()) {
      FieldSpec fieldSpec = schema.getFieldSpecFor(column);
      if (fieldSpec == null) {
        continue;
      }
      IntFunction<Object> provider = _valueProvidersByColumn.get(column);
      if (provider == null) {
        provider = docId -> defaultValue(fieldSpec.getDataType());
        _valueProvidersByColumn.put(column, provider);
      }
      _dataSourcesByColumn.put(column, new FunctionBasedDataSource(fieldSpec, numDocs, provider));
    }
    _segmentMetadata = new InMemorySegmentMetadata(segmentName, schema, numDocs);
  }

  @Override
  public String getSegmentName() {
    return _segmentName;
  }

  @Override
  public SegmentMetadata getSegmentMetadata() {
    return _segmentMetadata;
  }

  @Override
  public Set<String> getColumnNames() {
    return _schema.getColumnNames();
  }

  @Override
  public Set<String> getPhysicalColumnNames() {
    return _schema.getPhysicalColumnNames();
  }

  @Nullable
  @Override
  public DataSource getDataSourceNullable(String column) {
    return _dataSourcesByColumn.get(column);
  }

  @Override
  public DataSource getDataSource(String column, Schema schema) {
    DataSource dataSource = getDataSourceNullable(column);
    if (dataSource != null) {
      return dataSource;
    }
    throw new IllegalStateException("Failed to find data source for column: " + column);
  }

  @Nullable
  @Override
  public List<StarTreeV2> getStarTrees() {
    return null;
  }

  @Nullable
  @Override
  public TextIndexReader getMultiColumnTextIndex() {
    return null;
  }

  @Nullable
  @Override
  public ThreadSafeMutableRoaringBitmap getValidDocIds() {
    return null;
  }

  @Nullable
  @Override
  public ThreadSafeMutableRoaringBitmap getQueryableDocIds() {
    return null;
  }

  @Override
  public GenericRow getRecord(int docId, GenericRow reuse) {
    GenericRow row = reuse != null ? reuse : new GenericRow();
    row.getFieldToValueMap().clear();
    for (String column : _schema.getColumnNames()) {
      row.putValue(column, getValue(docId, column));
    }
    return row;
  }

  @Override
  public Object getValue(int docId, String column) {
    IntFunction<Object> provider = _valueProvidersByColumn.get(column);
    if (provider == null) {
      return null;
    }
    return provider.apply(docId);
  }

  @Override
  public void offload() {
  }

  @Override
  public void destroy() {
  }

  private static Object defaultValue(FieldSpec.DataType dataType) {
    switch (dataType) {
      case INT:
        return 0;
      case LONG:
        return 0L;
      case FLOAT:
        return 0.0f;
      case DOUBLE:
        return 0.0d;
      case BIG_DECIMAL:
        return BigDecimal.ZERO;
      case BOOLEAN:
        return false;
      case STRING:
        return "";
      case BYTES:
        return new byte[0];
      default:
        return null;
    }
  }

  private static final class InMemorySegmentMetadata implements SegmentMetadata {
    private final String _segmentName;
    private final Schema _schema;
    private final int _totalDocs;
    private final TreeMap<String, ColumnMetadata> _columnMetadataMap;

    private InMemorySegmentMetadata(String segmentName, Schema schema, int totalDocs) {
      _segmentName = segmentName;
      _schema = schema;
      _totalDocs = totalDocs;
      _columnMetadataMap = new TreeMap<>();
      for (String column : schema.getColumnNames()) {
        FieldSpec fieldSpec = schema.getFieldSpecFor(column);
        if (fieldSpec != null) {
          _columnMetadataMap.put(column, new InMemoryColumnMetadata(fieldSpec, totalDocs));
        }
      }
    }

    @Deprecated
    @Override
    public String getTableName() {
      return _schema.getSchemaName();
    }

    @Override
    public String getName() {
      return _segmentName;
    }

    @Override
    public String getTimeColumn() {
      return "";
    }

    @Override
    public long getStartTime() {
      return 0;
    }

    @Override
    public long getEndTime() {
      return 0;
    }

    @Override
    public TimeUnit getTimeUnit() {
      return TimeUnit.MILLISECONDS;
    }

    @Override
    public Duration getTimeGranularity() {
      return Duration.ZERO;
    }

    @Override
    public Interval getTimeInterval() {
      return new Interval(0L, 0L);
    }

    @Override
    public String getCrc() {
      return "";
    }

    @Override
    public String getDataCrc() {
      return "";
    }

    @Override
    public SegmentVersion getVersion() {
      return SegmentVersion.v3;
    }

    @Override
    public Schema getSchema() {
      return _schema;
    }

    @Override
    public int getTotalDocs() {
      return _totalDocs;
    }

    @Override
    public File getIndexDir() {
      return new File("");
    }

    @Nullable
    @Override
    public String getCreatorName() {
      return "systemtable";
    }

    @Override
    public long getIndexCreationTime() {
      return 0;
    }

    @Override
    public long getLastIndexedTimestamp() {
      return 0;
    }

    @Override
    public long getLatestIngestionTimestamp() {
      return Long.MIN_VALUE;
    }

    @Override
    public long getMinimumIngestionLagMs() {
      return Long.MAX_VALUE;
    }

    @Nullable
    @Override
    public List<StarTreeV2Metadata> getStarTreeV2MetadataList() {
      return null;
    }

    @Nullable
    @Override
    public MultiColumnTextMetadata getMultiColumnTextMetadata() {
      return null;
    }

    @Override
    public Map<String, String> getCustomMap() {
      return Collections.emptyMap();
    }

    @Override
    public String getStartOffset() {
      return "";
    }

    @Override
    public String getEndOffset() {
      return "";
    }

    @Override
    public TreeMap<String, ColumnMetadata> getColumnMetadataMap() {
      return _columnMetadataMap;
    }

    @Override
    public void removeColumn(String column) {
      _columnMetadataMap.remove(column);
    }

    @Override
    public JsonNode toJson(@Nullable Set<String> columnFilter) {
      return JsonNodeFactory.instance.objectNode();
    }
  }

  private static final class InMemoryColumnMetadata implements ColumnMetadata {
    private final FieldSpec _fieldSpec;
    private final int _totalDocs;

    private InMemoryColumnMetadata(FieldSpec fieldSpec, int totalDocs) {
      _fieldSpec = fieldSpec;
      _totalDocs = totalDocs;
    }

    @Override
    public FieldSpec getFieldSpec() {
      return _fieldSpec;
    }

    @Override
    public int getTotalDocs() {
      return _totalDocs;
    }

    @Override
    public int getCardinality() {
      return Constants.UNKNOWN_CARDINALITY;
    }

    @Override
    public boolean isSorted() {
      return false;
    }

    @Override
    public Comparable getMinValue() {
      return defaultComparableValue(_fieldSpec.getDataType());
    }

    @Override
    public Comparable getMaxValue() {
      return defaultComparableValue(_fieldSpec.getDataType());
    }

    @Override
    public boolean hasDictionary() {
      return false;
    }

    @Override
    public int getColumnMaxLength() {
      return 0;
    }

    @Override
    public int getBitsPerElement() {
      return 0;
    }

    @Override
    public int getMaxNumberOfMultiValues() {
      return 0;
    }

    @Override
    public int getTotalNumberOfEntries() {
      return _totalDocs;
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
    public boolean isAutoGenerated() {
      return false;
    }

    @Override
    public Map<IndexType<?, ?, ?>, Long> getIndexSizeMap() {
      return Collections.emptyMap();
    }

    private static Comparable defaultComparableValue(FieldSpec.DataType dataType) {
      switch (dataType) {
        case INT:
          return 0;
        case LONG:
          return 0L;
        case FLOAT:
          return 0.0f;
        case DOUBLE:
          return 0.0d;
        case BIG_DECIMAL:
          return BigDecimal.ZERO;
        case BOOLEAN:
          return false;
        case STRING:
          return "";
        default:
          return 0;
      }
    }
  }

  private static final class FunctionBasedDataSource implements DataSource {
    private final DataSourceMetadata _metadata;
    private final ColumnIndexContainer _indexContainer;
    private final ForwardIndexReader<?> _forwardIndex;

    private FunctionBasedDataSource(FieldSpec fieldSpec, int numDocs, IntFunction<Object> valueProvider) {
      _metadata = new FunctionBasedDataSourceMetadata(fieldSpec, numDocs);
      _indexContainer = ColumnIndexContainer.Empty.INSTANCE;
      _forwardIndex = new FunctionBasedForwardIndexReader(fieldSpec.getDataType(), valueProvider);
    }

    @Override
    public DataSourceMetadata getDataSourceMetadata() {
      return _metadata;
    }

    @Override
    public ColumnIndexContainer getIndexContainer() {
      return _indexContainer;
    }

    @Override
    public <R extends IndexReader> R getIndex(IndexType<?, R, ?> type) {
      if (type == null) {
        return null;
      }
      if (StandardIndexes.FORWARD_ID.equals(type.getId())) {
        return (R) _forwardIndex;
      }
      return null;
    }

    @Override
    public ForwardIndexReader<?> getForwardIndex() {
      return _forwardIndex;
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

    @Nullable
    @Override
    public NullValueVectorReader getNullValueVector() {
      return null;
    }

    @Nullable
    @Override
    public VectorIndexReader getVectorIndex() {
      return null;
    }
  }

  private static final class FunctionBasedDataSourceMetadata implements DataSourceMetadata {
    private final FieldSpec _fieldSpec;
    private final int _numDocs;

    private FunctionBasedDataSourceMetadata(FieldSpec fieldSpec, int numDocs) {
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
      return -1;
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

    @Override
    public int getCardinality() {
      return -1;
    }
  }

  private static final class FunctionBasedForwardIndexReader implements ForwardIndexReader<ForwardIndexReaderContext> {
    private final FieldSpec.DataType _storedType;
    private final IntFunction<Object> _valueProvider;

    private FunctionBasedForwardIndexReader(FieldSpec.DataType storedType, IntFunction<Object> valueProvider) {
      _storedType = storedType;
      _valueProvider = valueProvider;
    }

    private static <T> T coerceNumber(Object value, Function<Number, T> numberConverter, Function<String, T> parser) {
      return value instanceof Number ? numberConverter.apply((Number) value) : parser.apply(String.valueOf(value));
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
    public FieldSpec.DataType getStoredType() {
      return _storedType;
    }

    @Override
    public int getInt(int docId, ForwardIndexReaderContext context) {
      Object value = _valueProvider.apply(docId);
      return coerceNumber(value, number -> number.intValue(), Integer::parseInt);
    }

    @Override
    public long getLong(int docId, ForwardIndexReaderContext context) {
      Object value = _valueProvider.apply(docId);
      return coerceNumber(value, number -> number.longValue(), Long::parseLong);
    }

    @Override
    public float getFloat(int docId, ForwardIndexReaderContext context) {
      Object value = _valueProvider.apply(docId);
      return coerceNumber(value, number -> number.floatValue(), Float::parseFloat);
    }

    @Override
    public double getDouble(int docId, ForwardIndexReaderContext context) {
      Object value = _valueProvider.apply(docId);
      return coerceNumber(value, number -> number.doubleValue(), Double::parseDouble);
    }

    @Override
    public BigDecimal getBigDecimal(int docId, ForwardIndexReaderContext context) {
      Object value = _valueProvider.apply(docId);
      if (value instanceof BigDecimal) {
        return (BigDecimal) value;
      }
      if (value instanceof Number) {
        return BigDecimal.valueOf(((Number) value).doubleValue());
      }
      return new BigDecimal(String.valueOf(value));
    }

    @Override
    public String getString(int docId, ForwardIndexReaderContext context) {
      Object value = _valueProvider.apply(docId);
      return value != null ? value.toString() : null;
    }

    @Override
    public byte[] getBytes(int docId, ForwardIndexReaderContext context) {
      Object value = _valueProvider.apply(docId);
      if (value instanceof byte[]) {
        return (byte[]) value;
      }
      return value != null ? value.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8) : null;
    }

    @Override
    public void close()
        throws IOException {
    }
  }
}
