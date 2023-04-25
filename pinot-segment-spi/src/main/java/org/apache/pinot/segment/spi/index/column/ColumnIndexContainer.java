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
package org.apache.pinot.segment.spi.index.column;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.apache.pinot.segment.spi.index.IndexType;


/**
 * A container for all the indexes for a column.
 */
public interface ColumnIndexContainer extends Closeable {

  /**
   * Returns the index reader of the given type or null if the column doesn't have an index of that type.
   * @param indexType the type of the index the caller is interested in.
   * @return The index reader of the given type, or null if the column doesn't have an index of that type.
   * @param <I> The index reader class
   * @param <T> The index type
   */
  @Nullable
  <I extends IndexReader, T extends IndexType<?, I, ?>> I getIndex(T indexType);

  class Empty implements ColumnIndexContainer {
    public static final Empty INSTANCE = new Empty();

    @Nullable
    @Override
    public <I extends IndexReader, T extends IndexType<?, I, ?>> I getIndex(T indexType) {
      return null;
    }

    @Override
    public void close()
        throws IOException {
    }
  }

  class FromMap implements ColumnIndexContainer {
    private final Map<IndexType, ? extends IndexReader> _readersByIndex;

    /**
     * @param readersByIndex it is assumed that each index is associated with a compatible reader, but there is no check
     *                       to verify that. It is recommended to construct instances of this class by using
     *                       {@link FromMap.Builder}
     */
    public FromMap(Map<IndexType, ? extends IndexReader> readersByIndex) {
      _readersByIndex = readersByIndex;
    }

    @Nullable
    @Override
    public <I extends IndexReader, T extends IndexType<?, I, ?>> I getIndex(T indexType) {
      return (I) _readersByIndex.get(indexType);
    }

    @Override
    public void close()
        throws IOException {
    }

    public static class Builder {
      private final Map<IndexType, IndexReader> _readersByIndex = new HashMap<>();

      public Builder withAll(Map<IndexType, ? extends IndexReader> safeMap) {
        _readersByIndex.putAll(safeMap);
        return this;
      }

      public <R extends IndexReader> Builder with(IndexType<?, ? super R, ?> type, R reader) {
        _readersByIndex.put(type, reader);
        return this;
      }

      public FromMap build() {
        return new FromMap(_readersByIndex);
      }
    }
  }
}
