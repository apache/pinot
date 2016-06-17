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
package com.linkedin.pinot.core.realtime;

import com.linkedin.pinot.common.segment.SegmentMetadata;

/**
 * Allows one to listen to segment metadata changes
 *
 *
 */
public interface SegmentMetadataListener {
  /**
   * This method is invoked in three scenarios <br>
   * when the listener is initialized <br>
   * when the segment metadata is changed on ZK <br>
   * when the listener is removed <br>
   * Dont see the need to have different methods for each type of call back, its
   * better to have the logic idempotent across the three scenarios. If needed
   * we can add the change type in the changecontext
   *
   * @param segmentMetadata
   */
  public void onChange(SegmentMetadataChangeContext changeContext,
      SegmentMetadata segmentMetadata);

}
