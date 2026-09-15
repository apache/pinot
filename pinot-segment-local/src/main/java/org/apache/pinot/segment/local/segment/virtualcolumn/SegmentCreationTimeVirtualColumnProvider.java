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
package org.apache.pinot.segment.local.segment.virtualcolumn;

import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.SegmentMetadata;


/// Virtual column provider for `$creationTime`, the time the segment was created.
///
/// For a CONSUMING segment this is the time the consuming segment was created, not the time it was committed.
public class SegmentCreationTimeVirtualColumnProvider extends BaseSegmentMetadataVirtualColumnProvider {

  @Nullable
  @Override
  protected Object extractValue(SegmentMetadata segmentMetadata) {
    // An unset creation time is represented as Long.MIN_VALUE in the segment metadata and as -1 in the segment ZK
    // metadata a CONSUMING segment is created from, so treat any non-positive value as unavailable.
    long creationTime = segmentMetadata.getIndexCreationTime();
    return creationTime > 0 ? creationTime : null;
  }
}
