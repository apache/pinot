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

import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.data.readers.PrimaryKey;


public interface PartitionDedupMetadataManager {
  /**
   * Initializes the dedup metadata for the given immutable segment.
   */
  public void addSegment(IndexSegment segment);

  /**
   * Removes the dedup metadata for the given segment.
   */
  public void removeSegment(IndexSegment segment);

  /**
   * Add the primary key to the given segment to the dedup matadata if it was absent.
   * Returns true if the key was already present.
   */
  boolean checkRecordPresentOrUpdate(PrimaryKey pk, IndexSegment indexSegment);
}
