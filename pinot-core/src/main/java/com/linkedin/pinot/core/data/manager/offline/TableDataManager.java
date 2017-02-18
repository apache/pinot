/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.data.manager.offline;

import com.google.common.collect.ImmutableList;
import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.data.manager.config.TableDataManagerConfig;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;


/**
 * TableDataManager interface.
 * Provided interfaces to do operations on segment level.
 *
 *
 */
public interface TableDataManager {
  /**
   * Initialize TableDataManager based on given config.
   *
   * @param tableDataManagerConfig
   * @param serverInstance
   */
  void init(TableDataManagerConfig tableDataManagerConfig, ServerMetrics serverMetrics, String serverInstance);

  void start();

  void shutDown();

  boolean isStarted();

  /**
   * Adding an IndexSegment into the TableDataManager.
   * Used in testing only
   *
   * @param indexSegmentToAdd
   */
  void addSegment(IndexSegment indexSegmentToAdd);

  /**
   * Adding a Segment into the TableDataManager by given SegmentMetadata.
   *
   * @param segmentMetaToAdd
   * @param schema
   * @throws Exception
   */
  void addSegment(SegmentMetadata segmentMetaToAdd, Schema schema) throws Exception;

  /**
   * Adding a Segment into the TableDataManager by given DataTableZKMetadata, InstanceZKMetadata, SegmentZKMetadata.
   * @param propertyStore
   * @param tableConfig
   * @param instanceZKMetadata
   * @param segmentZKMetadata
   * @throws Exception
   */
  void addSegment(ZkHelixPropertyStore<ZNRecord> propertyStore, AbstractTableConfig tableConfig,
      InstanceZKMetadata instanceZKMetadata, SegmentZKMetadata segmentZKMetadata) throws Exception;

  /**
   *
   * Remove an IndexSegment/SegmentMetadata from the partition based on segmentName.
   * @param segmentToRemove
   */
  void removeSegment(String segmentToRemove);

  /**
   *
   * @note This method gets a lock on the segments. It is the caller's responsibility to return the segments
   * using the {@link #releaseSegment(SegmentDataManager) releaseSegment} method
   * @return segments by giving a list of segment names in this TableDataManager.
   */
  @Nonnull
  ImmutableList<SegmentDataManager> acquireAllSegments();

  /**
   *
   * @note This method gets a lock on the segments. It is the caller's responsibility to return the segments
   * using the {@link #releaseSegment(SegmentDataManager) releaseSegment} method
   * @return segments by giving a list of segment names in this TableDataManager.
   */
  List<SegmentDataManager> acquireSegments(List<String> segmentList);

  /**
   *
   * @note This method gets a lock on the segment. It is the caller's responsibility to return the segment
   * using the {@link #releaseSegment(SegmentDataManager) releaseSegment} method.
   * @return a segment by giving the name of this segment in this TableDataManager.
   */
  @Nullable SegmentDataManager acquireSegment(String segmentName);

  /**
  *
  * give back segmentReader, so the segment could be safely deleted.
  */
  void releaseSegment(SegmentDataManager segmentDataManager);

  /**
   * Get the table name managed by this instance
   */
  String getTableName();
}
