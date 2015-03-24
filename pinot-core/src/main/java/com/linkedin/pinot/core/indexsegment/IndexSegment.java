/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
import com.linkedin.pinot.core.common.Predicate;


/**
 * This is the interface of index segment. The index type of index segment
 * should be one of the supported {@link com.linkedin.pinot.core.indexsegment.IndexType
 * IndexType}.
 *
 * @author Xiang Fu <xiafu@linkedin.com>
 *
 */
public interface IndexSegment {
  /**
   * @return
   */
  public IndexType getIndexType();

  /**
   * @return
   */
  public String getSegmentName();

  /**
   * @return
   */
  public String getAssociatedDirectory();

  /**
   * @return SegmentMetadata
   */
  public SegmentMetadata getSegmentMetadata();

  /**
   *
   * @param columnName
   * @return
   */
  DataSource getDataSource(String columnName);

  /**
   *
   * @param columnName
   * @param p
   * @return
   */
  DataSource getDataSource(String columnName, Predicate p);

  /**
   * @return
   */
  String[] getColumnNames();

  /**
   * Destroy segment in memory and close file handler if in memory mapped mode
   */
  public void destroy();

  /**
   * Total number of records inside the segment.
   * @return
   */
  public int getTotalDocs();
}
