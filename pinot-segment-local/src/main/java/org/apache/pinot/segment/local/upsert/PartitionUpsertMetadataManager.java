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
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.MutableSegment;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.PrimaryKey;


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
   * Different from adding a segment, when preloading a segment, the upsert metadata may be updated more efficiently.
   * Basically the upsert metadata can be directly updated for each primary key, without doing the more costly
   * read-compare-update.
   */
  void preloadSegment(ImmutableSegment segment);

  /**
   * Updates the upsert metadata for a new consumed record in the given consuming segment.
   */
  void addRecord(MutableSegment segment, RecordInfo recordInfo);

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
   * Returns the record location for the given primary key.
   */
  GenericRow getRecordLocation(PrimaryKey primaryKey);

  /**
   * Stops the metadata manager. After invoking this method, no access to the metadata will be accepted.
   */
  void stop();
}
