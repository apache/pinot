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

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.index.datasource.BaseDataSource;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


/// Virtual per-key DataSource for a sparse OPEN_STRUCT key. Parses the blob per doc via
/// the shared [OpenStructSparseBlobReader], coerces to the resolved stored type. No dictionary.
/// Null vector built lazily (one blob scan, memoized).
public class SparseKeyDataSource extends BaseDataSource {
  private final FieldSpec _fieldSpec;

  public SparseKeyDataSource(FieldSpec resolvedChildSpec, OpenStructSparseBlobReader blobReader) {
    super(new SparseKeyMetadata(resolvedChildSpec, blobReader.getNumDocs()),
        new ColumnIndexContainer.FromMap(Map.of(
            StandardIndexes.forward(),
            new SparseKeyForwardIndexReader(resolvedChildSpec.getName(),
                resolvedChildSpec.getDataType().getStoredType(), blobReader),
            StandardIndexes.nullValueVector(),
            new LazyPresenceNullVector(resolvedChildSpec.getName(), blobReader))));
    _fieldSpec = resolvedChildSpec;
  }

  public FieldSpec getFieldSpec() {
    return _fieldSpec;
  }

  static class SparseKeyForwardIndexReader implements ForwardIndexReader<ForwardIndexReaderContext> {
    private final String _key;
    private final DataType _storedType;
    private final OpenStructSparseBlobReader _blob;

    SparseKeyForwardIndexReader(String key, DataType storedType, OpenStructSparseBlobReader blob) {
      _key = key;
      _storedType = storedType;
      _blob = blob;
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
    public ForwardIndexReaderContext createContext() {
      return _blob.createBlobContext();
    }

    @Nullable
    private JsonNode valueNode(int docId, ForwardIndexReaderContext context) {
      JsonNode node = _blob.getValue(docId, _key, context);
      return node == null || node.isNull() ? null : node;
    }

    private <T> T orDefault(int docId, ForwardIndexReaderContext context, Function<JsonNode, T> map, T defaultValue) {
      JsonNode node = valueNode(docId, context);
      return node == null ? defaultValue : map.apply(node);
    }

    // TODO: these hardcode the type defaults, so a child with a schema-configured defaultNullValue
    // reads back differently here than on the dense path (OpenStructColumnSplitter uses the spec).
    // Fixing it means threading the spec in and updating MapFilterOperator's sentinel check to match.
    @Override
    public int getInt(int docId, ForwardIndexReaderContext context) {
      return orDefault(docId, context, JsonNode::asInt, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT);
    }

    @Override
    public long getLong(int docId, ForwardIndexReaderContext context) {
      return orDefault(docId, context, JsonNode::asLong, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG);
    }

    @Override
    public float getFloat(int docId, ForwardIndexReaderContext context) {
      return orDefault(docId, context, node -> (float) node.asDouble(),
          FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_FLOAT);
    }

    @Override
    public double getDouble(int docId, ForwardIndexReaderContext context) {
      return orDefault(docId, context, JsonNode::asDouble, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_DOUBLE);
    }

    @Override
    public BigDecimal getBigDecimal(int docId, ForwardIndexReaderContext context) {
      return orDefault(docId, context, node -> new BigDecimal(node.asText()),
          FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BIG_DECIMAL);
    }

    @Override
    public String getString(int docId, ForwardIndexReaderContext context) {
      return orDefault(docId, context, JsonNode::asText, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING);
    }

    @Override
    public byte[] getBytes(int docId, ForwardIndexReaderContext context) {
      // Non-binary nodes yield null, and TextNode base64-decoding throws on malformed input; both fold to the
      // type default, matching the other getters.
      return orDefault(docId, context, node -> {
        try {
          byte[] bytes = node.binaryValue();
          return bytes == null ? FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BYTES : bytes;
        } catch (IOException e) {
          return FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BYTES;
        }
      }, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BYTES);
    }

    @Override
    public void close() {
    }
  }

  static class LazyPresenceNullVector implements NullValueVectorReader {
    private final String _key;
    private final OpenStructSparseBlobReader _blob;
    private volatile ImmutableRoaringBitmap _nullBitmap;

    LazyPresenceNullVector(String key, OpenStructSparseBlobReader blob) {
      _key = key;
      _blob = blob;
    }

    @Override
    public ImmutableRoaringBitmap getNullBitmap() {
      ImmutableRoaringBitmap bm = _nullBitmap;
      if (bm == null) {
        ImmutableRoaringBitmap presence = _blob.computePresence(_key);
        MutableRoaringBitmap nulls = new MutableRoaringBitmap();
        nulls.add(0L, _blob.getNumDocs());
        nulls.andNot(presence);
        bm = nulls.toImmutableRoaringBitmap();
        _nullBitmap = bm;
      }
      return bm;
    }

    @Override
    public boolean isNull(int docId) {
      return getNullBitmap().contains(docId);
    }
  }

  private static class SparseKeyMetadata implements DataSourceMetadata {
    private final FieldSpec _fieldSpec;
    private final int _numDocs;

    SparseKeyMetadata(FieldSpec fieldSpec, int numDocs) {
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
      return Constants.UNKNOWN_CARDINALITY;
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
