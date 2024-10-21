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
package org.apache.pinot.segment.spi.index.mutable;

import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.index.reader.MapIndexReader;


/**
 * Implementations of this interface can be used to represent indexes that store dynamically typed map values.
 */
public interface MutableMapIndex extends MapIndexReader<ForwardIndexReaderContext, MutableIndex>, MutableForwardIndex {

  @Override
  default void add(@Nonnull Object value, int dictId, int docId) {
    Map<String, Object> mapValue = (Map<String, Object>) value;
    add(mapValue, docId);
  }

  @Override
  default void add(@Nonnull Object[] values, @Nullable int[] dictIds, int docId) {
    throw new UnsupportedOperationException("MultiValues are not yet supported for MAP columns");
  }

  /**
   * Adds the given single value cell to the index.
   *
   * Unlike {@link org.apache.pinot.segment.spi.index.IndexCreator#add(Object, int)}, rows can be added in no
   * particular order, so the docId is required by this method.
   *
   * @param value The nonnull value of the cell. In case the cell was actually null, a default value is received instead
   * @param docId The document id of the given row. A non-negative value.
   */
  void add(Map<String, Object> value, int docId);

  /**
   * Get the Min Value that the given Key has within the segment that this Reader is bound to.
   *
   * @param key A Key within the given Map column.
   * @return The minimum value that is bound to that key within the Segment that this Reader is bound to.
   */
  Comparable<?> getMinValueForKey(String key);

  /**
   * Get the Max Value that the given Key has within the segment that this Reader is bound to.
   *
   * @param key A Key within the given Map column.
   * @return The maximum value that is bound to that key within the Segment that this Reader is bound to.
   */
  Comparable<?> getMaxValueForKey(String key);
}
