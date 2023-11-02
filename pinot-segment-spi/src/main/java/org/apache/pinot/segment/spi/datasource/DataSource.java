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
package org.apache.pinot.segment.spi.datasource;

import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.reader.BloomFilterReader;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.H3IndexReader;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.segment.spi.index.reader.RangeIndexReader;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * The {@code DataSource} contains all the indexes and metadata for a column for query execution purpose.
 */
public interface DataSource {

  /**
   * Returns the metadata for the column.
   */
  DataSourceMetadata getDataSourceMetadata();

  <R extends IndexReader> R getIndex(IndexType<?, R, ?> type);

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
  RangeIndexReader<?> getRangeIndex();

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
   * Returns the H3 index for the geospatial column if exists, or {@code null} if not.
   */
  @Nullable
  H3IndexReader getH3Index();

  /**
   * Returns the bloom filter for the column if exists, or {@code null} if not.
   */
  @Nullable
  BloomFilterReader getBloomFilter();

  /**
   * Returns null value vector for the column if exists, or {@code null} if not.
   *
   * The result is modified by the given null mode:
   * <ol>
   *   <li>In {@link NullMode#NONE_NULLABLE}, null is always returned.</li>
   *   <li>In {@link NullMode#ALL_NULLABLE}, the null vector of the segment is returned.</li>
   *   <li>In {@link NullMode#COLUMN_BASED}, the null vector of the segment is returned if and only if the field schema
   *   is declared as {@link FieldSpec#getNullable() nullable}.</li>
   * </ol>
   */
  @Nullable
  NullValueVectorReader getNullValueVector(NullMode nullMode);

  /**
   * Like {@link #getNullValueVector(NullMode) getNullValueVector(NullMode.ALL_NULL}.
   *
   * This is the older method that didn't care about the null mode and instead delegated on whether there was a null
   * vector in the segment or not.
   *
   * Ideally older code should be migrated to specify which null mode should be used.
   */
  default NullValueVectorReader getNullValueVector() {
    return getNullValueVector(NullMode.ALL_NULLABLE);
  }
}
