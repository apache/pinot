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
package org.apache.pinot.segment.spi.index.reader;

import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


/**
 * Reader for the ColumnarMap index. Provides O(1) typed key lookup per document via presence bitmap
 * rank operations. Each key has its own presence bitmap and typed forward index, allowing direct
 * typed reads without full map deserialization.
 */
public interface ColumnarMapIndexReader extends IndexReader {

  /**
   * Returns the set of all indexed key names.
   */
  Set<String> getKeys();

  /**
   * Returns the value DataType for the given key, or null if the key is not indexed.
   */
  @Nullable
  DataType getKeyValueType(String key);

  /**
   * Returns the number of documents that have the given key (i.e., the size of the presence bitmap).
   */
  int getNumDocsWithKey(String key);

  /**
   * Returns the presence bitmap for the given key. A document is present in this bitmap if and
   * only if it has a non-null value for the key. This bitmap is the inverted null bitmap.
   */
  ImmutableRoaringBitmap getPresenceBitmap(String key);

  // Typed value access -- O(1) per doc via presence bitmap rank
  // Returns the default value (0, 0L, 0.0f, 0.0, "", new byte[0]) if the doc does not have the key.

  int getInt(int docId, String key);

  long getLong(int docId, String key);

  float getFloat(int docId, String key);

  double getDouble(int docId, String key);

  String getString(int docId, String key);

  byte[] getBytes(int docId, String key);

  /**
   * Returns a bitmap of docIds that have the given key-value pair.
   * Only available if per-key inverted index is enabled (enableInvertedIndexForAll=true or key in invertedIndexKeys).
   * Returns null if inverted index is not available for this key.
   */
  @Nullable
  ImmutableRoaringBitmap getDocsWithKeyValue(String key, Object value);

  /**
   * Returns a DataSource for the given key, suitable for use with ItemTransformFunction and
   * MapFilterOperator. The DataSource provides typed single-value access backed by the per-key
   * forward index.
   */
  DataSource getKeyDataSource(String key);

  /**
   * Reconstructs the full map for a document from per-key data. Used for SELECT sparse_col queries
   * and data table transport to the broker.
   */
  Map<String, Object> getMap(int docId);

  /**
   * Returns whether the given key has an inverted index available.
   */
  default boolean hasInvertedIndex(String key) {
    return false;
  }

  /**
   * Returns the sorted distinct values for the given key from the inverted index, or null if no
   * inverted index is available. Used to build a dictionary for dictionary-based GROUP BY.
   */
  @Nullable
  default String[] getDistinctValuesForKey(String key) {
    return null;
  }
}
