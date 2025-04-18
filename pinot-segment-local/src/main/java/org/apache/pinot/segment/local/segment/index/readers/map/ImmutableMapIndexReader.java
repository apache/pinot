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
package org.apache.pinot.segment.local.segment.index.readers.map;

import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.index.readers.forward.VarByteChunkForwardIndexReaderV4;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.MapIndexCreator;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.index.reader.MapIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Reader for map index.
 * The v1 implementation is just bytes.
 *
 */
public class ImmutableMapIndexReader implements MapIndexReader<ForwardIndexReaderContext, IndexReader> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ImmutableMapIndexReader.class);
  // NOTE: Use long type for _numDocs to comply with the RoaringBitmap APIs.
  protected final PinotDataBuffer _dataBuffer;
  private final ForwardIndexReader _forwardIndexReader;
  private final FieldSpec _valueFieldSpec;
  private final ColumnMetadata _columnMetadata;

  public ImmutableMapIndexReader(PinotDataBuffer dataBuffer, ColumnMetadata columnMetadata) {
    int version = dataBuffer.getInt(0);
    Preconditions.checkState(version == MapIndexCreator.VERSION_1,
        "Unsupported map index version: %s.  Valid versions are {}", version, MapIndexCreator.VERSION_1);
    _dataBuffer = dataBuffer;
    _columnMetadata = columnMetadata;
    _forwardIndexReader =
        new VarByteChunkForwardIndexReaderV4(_dataBuffer, FieldSpec.DataType.BYTES, true);
    ComplexFieldSpec complexFieldSpec = (ComplexFieldSpec) columnMetadata.getFieldSpec();
    Preconditions.checkState(
        complexFieldSpec.getChildFieldSpec(ComplexFieldSpec.KEY_FIELD).getDataType() == FieldSpec.DataType.STRING,
        "Only String key is supported in Map");
    _valueFieldSpec = complexFieldSpec.getChildFieldSpec(ComplexFieldSpec.VALUE_FIELD);
  }

  @Override
  public void close() {
    // NOTE: DO NOT close the PinotDataBuffer here because it is tracked by the caller and might be reused later. The
    // caller is responsible of closing the PinotDataBuffer.
  }

  @Override
  public IndexReader getKeyReader(String key, IndexType type) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public Set<String> getKeys() {
    return Set.of();
  }

  @Override
  public Map<IndexType, IndexReader> getKeyIndexes(String key) {
    IndexReader fwdIdx = getKeyReader(key, StandardIndexes.forward());
    if (fwdIdx != null) {
      return Map.of(StandardIndexes.forward(), getKeyReader(key, StandardIndexes.forward()));
    } else {
      return null;
    }
  }

  @Override
  public FieldSpec getKeyFieldSpec(String key) {
    return _valueFieldSpec;
  }

  @Override
  public FieldSpec.DataType getKeyStoredType(String key) {
    return _valueFieldSpec.getDataType();
  }

  @Override
  public ColumnMetadata getKeyMetadata(String key) {
    return new ColumnMetadata() {
      @Override
      public FieldSpec getFieldSpec() {
        return _valueFieldSpec;
      }

      @Override
      public int getTotalDocs() {
        return _columnMetadata.getTotalDocs();
      }

      @Override
      public int getCardinality() {
        return 0;
      }

      @Override
      public boolean isSorted() {
        return false;
      }

      @Override
      public Comparable getMinValue() {
        return null;
      }

      @Override
      public Comparable getMaxValue() {
        return null;
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
        return 0;
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
      public long getIndexSizeFor(IndexType type) {
        return ColumnMetadata.INDEX_NOT_FOUND;
      }

      @Override
      public boolean isAutoGenerated() {
        return false;
      }
    };
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
    return FieldSpec.DataType.MAP;
  }

  @Nullable
  @Override
  public ChunkCompressionType getCompressionType() {
    return ChunkCompressionType.PASS_THROUGH;
  }

  @Override
  public Map<String, Object> getMap(int docId, ForwardIndexReaderContext mapContext) {
    return _forwardIndexReader.getMap(docId, mapContext);
  }

  @Override
  public String getString(int docId, ForwardIndexReaderContext context) {
    return MapUtils.toString(getMap(docId, context));
  }
}
