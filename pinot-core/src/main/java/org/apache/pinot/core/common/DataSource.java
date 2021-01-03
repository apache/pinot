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
package org.apache.pinot.core.common;

import javax.annotation.Nullable;
import org.apache.pinot.core.segment.index.readers.BloomFilterReader;
import org.apache.pinot.core.segment.index.readers.Dictionary;
import org.apache.pinot.core.segment.index.readers.ForwardIndexReader;
import org.apache.pinot.core.segment.index.readers.H3IndexReader;
import org.apache.pinot.core.segment.index.readers.InvertedIndexReader;
import org.apache.pinot.core.segment.index.readers.JsonIndexReader;
import org.apache.pinot.core.segment.index.readers.NullValueVectorReader;
import org.apache.pinot.core.segment.index.readers.TextIndexReader;



/**
 * The {@code DataSource} contains all the indexes and metadata for a column for query execution purpose.
 */
public interface DataSource {

  /**
   * Returns the metadata for the column.
   */
  DataSourceMetadata getDataSourceMetadata();

  /**
   * Returns the forward index for the column. The forward index can be either dictionary-encoded or raw.
   */
  ForwardIndexReader<?> getForwardIndex();

  /**
   * Returns the dictionary for the column if it is dictionary-encoded, or {@code null} if not.
   */
  @Nullable
  Dictionary getDictionary();

  /**
   * Returns the inverted index for the column if exists, or {@code null} if not.
   */
  @Nullable
  InvertedIndexReader<?> getInvertedIndex();

  /**
   * Returns the range index for the column if exists, or {@code null} if not.
   * <p>TODO: Have a separate interface for range index.
   */
  @Nullable
  InvertedIndexReader<?> getRangeIndex();

  /**
   * Returns the H3 index for the geospatial column if exists, or {@code null} if not.
   */
  @Nullable
  H3IndexReader getH3Index();

  /**
   * Returns the text index for the column if exists, or {@code null} if not.
   */
  @Nullable
  TextIndexReader getTextIndex();

  /**
   * Returns the FST index for the column if exists, or {@code null} if not.
   */
  @Nullable
  TextIndexReader getFSTIndex();

  /**
   * Returns the json index for the column if exists, or {@code null} if not.
   */
  @Nullable
  JsonIndexReader getJsonIndex();

  /**
   * Returns the bloom filter for the column if exists, or {@code null} if not.
   */
  @Nullable
  BloomFilterReader getBloomFilter();

  /**
   * Returns null value vector for the column if exists, or {@code null} if not.
   */
  @Nullable
  NullValueVectorReader getNullValueVector();
}
