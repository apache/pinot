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
package org.apache.pinot.segment.local.upsert;

import java.io.Closeable;
import java.util.List;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * Manages the upsert metadata per partition.
 * <p>For multiple records with the same comparison value (default to timestamp), the manager will preserve the latest
 * record based on the sequence number of the segment. If 2 records with the same comparison value are in the same
 * segment, the one with larger doc id will be preserved. Note that for tables with sorted column, the records will be
 * re-ordered when committing the segment, and we will use the re-ordered doc ids instead of the ingestion doc ids to
 * decide the record to preserve.
 *
 * <p>There will be short term inconsistency when updating the upsert metadata, but should be consistent after the
 * operation is done:
 * <ul>
 *   <li>
 *     When updating a new record, it first removes the doc id from the current location, then update the new location.
 *   </li>
 *   <li>
 *     When adding a new segment, it removes the doc ids from the current locations before the segment being added to
 *     the RealtimeTableDataManager.
 *   </li>
 *   <li>
 *     When replacing an existing segment, after the record location being replaced with the new segment, the following
 *     updates applied to the new segment's valid doc ids won't be reflected to the replaced segment's valid doc ids.
 *   </li>
 * </ul>
 */
@ThreadSafe
public interface PartitionUpsertMetadataManager extends Closeable {

  /**
   * Returns the primary key columns.
   */
  List<String> getPrimaryKeyColumns();

  /**
   * Initializes the upsert metadata for the given immutable segment.
   */
  void addSegment(ImmutableSegment segment);

  /**
   * Preload segments for the table partition. Segments can be added differently during preloading.
   * TODO: Revisit this and see if we can use the same IndexLoadingConfig for all segments. Tier info might be different
   *       for different segments.
   */
  void preloadSegments(IndexLoadingConfig indexLoadingConfig);

  boolean isPreloading();

  /**
   * Different from adding a segment, when preloading a segment, the upsert metadata may be updated more efficiently.
   * Basically the upsert metadata can be directly updated for each primary key, without doing the more costly
   * read-compare-update.
   */
  void preloadSegment(ImmutableSegment segment);

  /**
   * Updates the upsert metadata for a new consumed record in the given consuming segment.
   */
  boolean addRecord(MutableSegment segment, RecordInfo recordInfo);

  /**
   * Replaces the upsert metadata for the old segment with the new immutable segment.
   */
  void replaceSegment(ImmutableSegment segment, IndexSegment oldSegment);

  /**
   * Removes the upsert metadata for the given segment.
   */
  void removeSegment(IndexSegment segment);

  /**
   * Returns the merged record when partial-upsert is enabled.
   */
  GenericRow updateRecord(GenericRow record, RecordInfo recordInfo);

  /**
   * Takes snapshot for all the tracked immutable segments when snapshot is enabled. This method should be invoked
   * before a new consuming segment starts consuming.
   */
  void takeSnapshot();

  /**
   * Remove the expired primary keys from the metadata when TTL is enabled.
   */
  void removeExpiredPrimaryKeys();

  /**
   * Stops the metadata manager. After invoking this method, no access to the metadata will be accepted.
   */
  void stop();

  /**
   * Track segments for upsert view, and this method must be called before registering segment to the table manager,
   * so that the segment is included in the upsert view before it becomes visible to the query.
   */
  void trackSegmentForUpsertView(IndexSegment segment);

  /**
   * Untrack segments for upsert view, and this method must be called when segment is to be destroyed, when the
   * segment is not used by any queries. Untrack segment after unregistering the segment is not safe, as there may be
   * ongoing queries that are still accessing the segment.
   */
  void untrackSegmentForUpsertView(IndexSegment segment);

  /**
   * Track newly added segments to get them included in the list of selected segments for queries to get a more
   * complete upsert data view, e.g. the newly created consuming segment or newly uploaded immutable segments. Such
   * segments can be processed by the server even before they get included in the broker's routing table.
   */
  void trackNewlyAddedSegment(String segmentName);
}
