/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.indexsegment.immutable;

import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.io.reader.DataFileReader;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;
import com.linkedin.pinot.core.segment.index.readers.InvertedIndexReader;


public interface ImmutableSegment extends IndexSegment {

  /**
   * Returns the dictionary for the given column.
   *
   * @param column Column name
   * @return Dictionary for the given column, or null if the given column does not have one
   */
  Dictionary getDictionary(String column);

  /**
   * Returns the forward index for the given column.
   *
   * @param column Column name
   * @return Forward index for the given column
   */
  DataFileReader getForwardIndex(String column);

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
}
