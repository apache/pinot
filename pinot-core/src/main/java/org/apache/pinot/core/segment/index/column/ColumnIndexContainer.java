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
package org.apache.pinot.core.segment.index.column;

import java.io.Closeable;
import org.apache.pinot.core.segment.index.readers.BloomFilterReader;
import org.apache.pinot.core.segment.index.readers.Dictionary;
import org.apache.pinot.core.segment.index.readers.ForwardIndexReader;
import org.apache.pinot.core.segment.index.readers.InvertedIndexReader;
import org.apache.pinot.core.segment.index.readers.JsonIndexReader;
import org.apache.pinot.core.segment.index.readers.NullValueVectorReaderImpl;
import org.apache.pinot.core.segment.index.readers.TextIndexReader;


/**
 * A container for all the indexes for a column.
 */
public interface ColumnIndexContainer extends Closeable {

  /**
   * Returns the forward index for the column.
   */
  ForwardIndexReader<?> getForwardIndex();

  /**
   * Returns the inverted index for the column, or {@code null} if it does not exist.
   */
  InvertedIndexReader<?> getInvertedIndex();

  /**
   * Returns the range index for the column, or {@code null} if it does not exist.
   */
  InvertedIndexReader<?> getRangeIndex();

  /**
   * Returns the text index for the column, or {@code null} if it does not exist.
   */
  TextIndexReader getTextIndex();

  /**
   * Returns the FST index for the column, or {@code null} if it does not exist.
   */
  TextIndexReader getFSTIndex();

  /**
   * Returns the json index for the column, or {@code null} if it does not exist.
   */
  JsonIndexReader getJsonIndex();

  /**
   * Returns the dictionary for the column, or {@code null} if it does not exist.
   */
  Dictionary getDictionary();

  /**
   * Returns the bloom filter for the column, or {@code null} if it does not exist.
   */
  BloomFilterReader getBloomFilter();

  /**
   * Returns the null value vector for the column, or {@code null} if it does not exist.
   */
  NullValueVectorReaderImpl getNullValueVector();
}
