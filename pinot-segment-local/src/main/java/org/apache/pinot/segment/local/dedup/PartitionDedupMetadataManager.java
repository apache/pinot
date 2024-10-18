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
package org.apache.pinot.segment.local.dedup;

import java.io.Closeable;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.data.readers.PrimaryKey;


public interface PartitionDedupMetadataManager extends Closeable {
  /**
   * Initializes the dedup metadata for the given immutable segment.
   */
  void addSegment(IndexSegment segment);

  /**
   * Replaces the dedup metadata for the given old segment with the new segment.
   */
  default void replaceSegment(IndexSegment oldSegment, IndexSegment newSegment) {
    // since this is a newly added method, by default, add the new segment to keep backward compatibility
    addSegment(newSegment);
  }

  /**
   * Preload segments for the table partition. Segments can be added differently during preloading.
   * TODO: As commented in PartitionUpsertMetadataManager, revisit this method and see if we can use the same
   *       IndexLoadingConfig for all segments. Tier info might be different for different segments.
   */
  void preloadSegments(IndexLoadingConfig indexLoadingConfig);

  boolean isPreloading();

  /**
   * Different from adding a segment, when preloading a segment, the dedup metadata may be updated more efficiently.
   * Basically the dedup metadata can be directly updated for each primary key, without doing the more costly
   * read-compare-update.
   */
  void preloadSegment(ImmutableSegment immutableSegment);

  /**
   * Removes the dedup metadata for the given segment.
   */
  void removeSegment(IndexSegment segment);

  /**
   * Remove the expired primary keys from the metadata when TTL is enabled.
   */
  void removeExpiredPrimaryKeys();

  /**
   * Add the primary key to the given segment to the dedup matadata if it is absent.
   * Returns true if the key was already present, i.e., the new record associated with the given {@link PrimaryKey}
   * is a duplicate and should be skipped/dropped.
   */
  @Deprecated
  boolean checkRecordPresentOrUpdate(PrimaryKey pk, IndexSegment indexSegment);

  /**
   * Add the primary key to the given segment to the dedup matadata if it is absent and with in the retention time.
   * Returns true if the key was already present, i.e., the new record associated with the given {@link DedupRecordInfo}
   * is a duplicate and should be skipped/dropped.
   * @param dedupRecordInfo  The primary key and the dedup time.
   * @param indexSegment  The segment to which the record belongs.
   * @return true if the key was already present, i.e., the new record associated with the given {@link DedupRecordInfo}
   * is a duplicate and should be skipped/dropped.
   */
  default boolean checkRecordPresentOrUpdate(DedupRecordInfo dedupRecordInfo, IndexSegment indexSegment) {
    return checkRecordPresentOrUpdate(dedupRecordInfo.getPrimaryKey(), indexSegment);
  }

  /**
   * Stops the metadata manager. After invoking this method, no access to the metadata will be accepted.
   */
  void stop();
}
