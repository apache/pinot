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
package com.linkedin.pinot.core.indexsegment;

import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.startree.StarTree;
import com.linkedin.pinot.core.startreeV2.StarTreeV2Impl;
import java.util.Set;


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
   * Returns the {@link DataSource} for the given column.
   *
   * @param columnName Column name
   * @return Data source for the given column
   */
  DataSource getDataSource(String columnName);

  /**
   * Returns the Star-tree index if it exists, or null if it does not exist.
   *
   * @return Star-tree index
   */
  StarTree getStarTree();

  /**
   * Returns the record for the given document Id.
   * <p>NOTE: don't use this method for high performance code.
   *
   * @param docId Document Id
   * @param reuse Reusable buffer for the record
   * @return Record for the given document Id
   */
  GenericRow getRecord(int docId, GenericRow reuse);

  /**
   * Destroys segment in memory and closes file handlers if in MMAP mode.
   */
  void destroy();

  /**
   * Returns the Star-tree v2 index.
   *
   * @return Star-tree V2 index
   */
  StarTreeV2Impl getStarTree(int starTreeId);
}
