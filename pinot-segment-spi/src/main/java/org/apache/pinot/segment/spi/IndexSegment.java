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
import org.apache.pinot.segment.spi.index.reader.ValidDocIndexReader;
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

  // TODO(upsert): solve the coordination problems of getting validDoc across segments for result consistency
  @Nullable
  ValidDocIndexReader getValidDocIndex();

  /**
   * Returns the record for the given document Id. Virtual column values are not returned.
   * <p>NOTE: don't use this method for high performance code.
   *
   * @param docId Document Id
   * @param reuse Reusable buffer for the record
   * @return Record for the given document Id
   */
  GenericRow getRecord(int docId, GenericRow reuse);

  /**
   * This is a hint to the the implementation, to prefetch buffers for specified columns
   * @param columns columns to prefetch
   */
  default void prefetch(Set<String> columns) {
  }

  /**
   * Destroys segment in memory and closes file handlers if in MMAP mode.
   */
  void destroy();
}
