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


/// Virtual column provider for `$crc`, the CRC of the segment.
///
/// The CRC is exposed as a LONG, matching how it is stored everywhere else in Pinot ([SegmentMetadata#getCrc()]
/// renders the same `long` as a String). It reads as NULL for CONSUMING segments, which have no CRC until they are
/// committed. Grouping by `$segmentName` and `$crc` is a convenient way to detect replicas of a segment that have
/// diverged.
public class SegmentCrcVirtualColumnProvider extends BaseSegmentMetadataVirtualColumnProvider {
  @Nullable
  @Override
  protected Object extractValue(SegmentMetadata segmentMetadata) {
    String crc = segmentMetadata.getCrc();
    if (crc == null) {
      return null;
    }
    long crcValue;
    try {
      crcValue = Long.parseLong(crc);
    } catch (NumberFormatException e) {
      // The CRC is always rendered from a long, but never fail a segment load over an unreadable one
      return null;
    }
    // SegmentMetadataImpl renders an unset CRC as Long.MIN_VALUE
    return crcValue != Long.MIN_VALUE ? crcValue : null;
  }
}
