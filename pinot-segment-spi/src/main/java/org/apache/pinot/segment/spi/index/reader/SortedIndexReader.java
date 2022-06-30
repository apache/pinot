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

import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.Pairs;


/**
 * Interface for sorted index reader which can be used as both forward index and inverted index.
 */
public interface SortedIndexReader<T extends ForwardIndexReaderContext>
    extends ForwardIndexReader<T>, InvertedIndexReader<Pairs.IntPair> {

  /**
   * NOTE: Sorted index is always dictionary-encoded.
   */
  @Override
  default boolean isDictionaryEncoded() {
    return true;
  }

  /**
   * NOTE: Sorted index can only apply to single-value column.
   */
  @Override
  default boolean isSingleValue() {
    return true;
  }

  @Override
  default DataType getStoredType() {
    return DataType.INT;
  }
}
