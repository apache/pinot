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
package org.apache.pinot.segment.spi;

import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;


public interface ImmutableSegment extends IndexSegment {

  /**
   * Returns the dictionary for the given column.
   *
   * @param column Column name
   * @return Dictionary for the given column, or null if the given column does not have one
   */
  Dictionary getDictionary(String column);

  <I extends IndexReader> I getIndex(String column, IndexType<?, I, ?> type);

  /**
   * Returns the forward index for the given column.
   *
   * @param column Column name
   * @return Forward index for the given column
   */
  ForwardIndexReader getForwardIndex(String column);

  /**
   * Returns the inverted index for the given column.
   *
   * @param column Column name
   * @return Inverted index for the given column, or null if the given column does not have one
   */
  InvertedIndexReader getInvertedIndex(String column);

  /**
   * Returns the total size of the segment in bytes.
   *
   * @return Size of the segment in bytes
   */
  long getSegmentSizeBytes();

  /**
   * Get the storage tier of the immutable segment.
   *
   * @return storage tier, null by default.
   */
  @Nullable
  String getTier();
}
