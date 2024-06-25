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
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.segment.local.segment.creator.impl.map.DenseMapHeader;
import org.apache.pinot.segment.local.segment.creator.impl.map.MapIndexHeader;
import org.apache.pinot.segment.local.segment.index.forward.ForwardIndexReaderFactory;
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
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Reader for map index.
 *
 * The Header for the Map Index
 * - Information that is needed:
 * 1. The number of dense columns in the set
 * 2. The number of sparse columns in the set
 * 3. The offset of each column within the data buffer
 * 4. The type of each column
 * 5. The index type of each column
 * 6. The name of the key for each dense column
 *
 * Header Layout
 * | Version Number
 * | Number of Dense Keys
 * | Offset of Dense Key Metadata
 * | Offset of Sparse Key Metadata
 * | Number of Sparse Indexes
 * | (Offset, Dense Key Name, Type)  Or do we store the metadata for each Keylumn
 * | .... |
 * | (Sparse column offset, sparse column metadata)
 * | .... |
 * | ...Actual Data... |
 *
 * Quesitons:
 * 1. How can I a read a forward index from this data buffer?
 * 2. How do I read index metadata from this buffer?
 */
public class ImmutableMapIndexReader implements MapIndexReader<ForwardIndexReaderContext, IndexReader> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ImmutableMapIndexReader.class);
  // NOTE: Use long type for _numDocs to comply with the RoaringBitmap APIs.
  protected final PinotDataBuffer _dataBuffer;
  private MapIndexHeader _header;
  private final HashMap<String, Pair<ForwardIndexReader<ForwardIndexReaderContext>, ForwardIndexReaderContext>>
      _keyIndexes = new HashMap<>();

  public ImmutableMapIndexReader(PinotDataBuffer dataBuffer) {
    int version = dataBuffer.getInt(0);
    Preconditions.checkState(version == MapIndexCreator.VERSION_1,
        "Unsupported map index version: %s.  Valid versions are {}", version, MapIndexCreator.VERSION_1);
    _dataBuffer = dataBuffer;
    try {
      _header = MapIndexHeader.read(_dataBuffer, 0).getLeft();
    } catch (Exception ex) {
      LOGGER.error("Error while reading header for map index", ex);
      _header = null;
    }
    loadKeyIndexes();
  }

  @Override
  public void close() {
    // NOTE: DO NOT close the PinotDataBuffer here because it is tracked by the caller and might be reused later. The
    // caller is responsible of closing the PinotDataBuffer.
  }

  @Override
  public IndexReader getKeyReader(String key, IndexType type) {
    Preconditions.checkArgument(type.getId().equals(StandardIndexes.forward().getId()),
        "Currently the Map type only supports Forward Indexes");
    // Get the offset for the key index
    DenseMapHeader.DenseKeyMetadata keyMetadata = _header.getMapIndex().getKey(key);
    if (keyMetadata != null) {
      long offset = keyMetadata.getIndexOffset(StandardIndexes.forward());
      long size = keyMetadata.getColumnMetadata().getIndexSizeMap().get(type);
      PinotDataBuffer indexBuffer = _dataBuffer.view(offset, offset + size);
       return ForwardIndexReaderFactory.createIndexReader(indexBuffer, keyMetadata.getColumnMetadata());
    } else {
      return null;
    }
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
  public FieldSpec.DataType getStoredType(String key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ColumnMetadata getKeyMetadata(String key) {
    return _header.getMapIndex().getKey(key).getColumnMetadata();
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
    Map<String, Object> mapValue = new HashMap<>();

    for (DenseMapHeader.DenseKeyMetadata keyMeta : _header.getMapIndex().getKeys()) {
      String key = keyMeta.getName();
      Pair<ForwardIndexReader<ForwardIndexReaderContext>, ForwardIndexReaderContext> keyIndex = _keyIndexes.get(key);

      try {
        switch (keyIndex.getLeft().getStoredType()) {
          case INT: {
            int value = keyIndex.getLeft().getInt(docId, keyIndex.getRight());
            mapValue.put(key, value);
            break;
          }
          case LONG: {
            long value = keyIndex.getLeft().getLong(docId, keyIndex.getRight());
            mapValue.put(key, value);
            break;
          }
          case FLOAT: {
            float value = keyIndex.getLeft().getFloat(docId, keyIndex.getRight());
            mapValue.put(key, value);
            break;
          }
          case DOUBLE: {
            double value = keyIndex.getLeft().getDouble(docId, keyIndex.getRight());
            mapValue.put(key, value);
            break;
          }
          case STRING: {
            String value = keyIndex.getLeft().getString(docId, keyIndex.getRight());
            mapValue.put(key, value);
            break;
          }
          case BIG_DECIMAL:
          case BOOLEAN:
          case TIMESTAMP:
          case JSON:
          case BYTES:
          case STRUCT:
          case LIST:
          case MAP:
          case UNKNOWN:
          default:
            throw new UnsupportedOperationException();
        }
      } catch (Exception ex) {
        LOGGER.error("Exception caught while reading from key '{}'", key, ex);
        throw ex;
      }
    }
    return mapValue;
  }

  @Override
  public String getString(int docId, ForwardIndexReaderContext context) {
    try {
      Map<String, Object> mapValue = getMap(docId, context);
      return JsonUtils.objectToString(mapValue);
    } catch (Exception ex) {
      LOGGER.error("Failed to serialize MAP value to JSON String", ex);
    }

    return "";
  }

  private void loadKeyIndexes() {
    // Iterate over each key in the header and load the index for that key along with the key's context
    for (DenseMapHeader.DenseKeyMetadata keyMeta : _header.getMapIndex().getKeys()) {
      String key = keyMeta.getName();
      ForwardIndexReader<ForwardIndexReaderContext> keyIndex =
          (ForwardIndexReader<ForwardIndexReaderContext>) getKeyReader(key, StandardIndexes.forward());
      ForwardIndexReaderContext keyContext = keyIndex.createContext();

      _keyIndexes.put(key, new ImmutablePair<>(keyIndex, keyContext));
    }
  }
}
