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

import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;
import org.apache.pinot.spi.annotations.InterfaceAudience;
import org.apache.pinot.spi.data.readers.GenericRow;


@InterfaceAudience.Private
public interface IndexSegment {

  /**
   * Returns the name of the segment.
   *
   * @return Segment name
   */
  String getSegmentName();

  /**
   * Returns the {@link SegmentMetadata} of the segment.
   *
   * @return Segment metadata
   */
  SegmentMetadata getSegmentMetadata();

  /**
   * Returns all the columns inside the segment.
   *
   * @return Set of column names
   */
  Set<String> getColumnNames();

  /**
   * Returns all of the columns in the segment that are not provided by a virtual column provider.
   *
   * @return Set of column names
   */
  Set<String> getPhysicalColumnNames();

  /**
   * Returns the {@link DataSource} for the given column.
   *
   * @param columnName Column name
   * @return Data source for the given column
   */
  DataSource getDataSource(String columnName);

  /**
   * Returns a list of star-trees (V2), or null if there is no star-tree (V2) in the segment.
   */
  List<StarTreeV2> getStarTrees();

  /**
   * Returns a bitmap of the valid document ids. Valid document is the document that holds the latest timestamp (or
   * largest comparison value) for the primary key for upsert enabled tables.
   */
  // TODO(upsert): solve the coordination problems of getting validDoc across segments for result consistency
  @Nullable
  ThreadSafeMutableRoaringBitmap getValidDocIds();

  /**
   * Returns a bitmap of the queryable document ids. Queryable document is the document that holds the latest timestamp
   * (or largest comparison value) for the primary key and is not deleted for upsert enabled tables.
   */
  @Nullable
  ThreadSafeMutableRoaringBitmap getQueryableDocIds();

  /**
   * Returns the record for the given document id. Virtual column values are not returned.
   * <p>NOTE: don't use this method for high performance code. Use PinotSegmentRecordReader when reading multiple
   * records from the same segment.
   *
   * @param docId Document id
   * @param reuse Reusable buffer for the record
   * @return Record for the given document id
   */
  GenericRow getRecord(int docId, GenericRow reuse);

  /**
   * Returns the value for the column at the document id. Returns byte[] for BYTES data type.
   * <p>NOTE: don't use this method for high performance code. Use PinotSegmentColumnReader when reading multiple
   * values from the same segment.
   */
  Object getValue(int docId, String column);

  /**
   * Hints the segment to begin prefetching buffers for specified columns.
   * Typically, this should be an async call made before operating on the segment.
   * @param fetchContext context for this segment's fetch
   */
  default void prefetch(FetchContext fetchContext) {
  }

  /**
   * Instructs the segment to fetch buffers for specified columns.
   * When enabled, this should be a blocking call made before operating on the segment.
   * @param fetchContext context for this segment's fetch
   */
  default void acquire(FetchContext fetchContext) {
  }

  /**
   * Instructs the segment to release buffers for specified columns.
   * When enabled, this should be a call made after operating on the segment.
   * It is possible that this called multiple times.
   * @param fetchContext context for this segment's fetch
   */
  default void release(FetchContext fetchContext) {
  }

  /**
   * Destroys segment in memory and closes file handlers if in MMAP mode.
   */
  void destroy();
}
