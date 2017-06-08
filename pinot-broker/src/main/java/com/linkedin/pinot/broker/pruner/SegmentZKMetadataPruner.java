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
package com.linkedin.pinot.broker.pruner;

import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadata;


/**
 * Interface for SegmentZKMetadata based pruner.
 */
public interface SegmentZKMetadataPruner {

  /**
   * Prunes the segment based on the segment's ZK metadata.
   *
   * @param segmentZKMetadata ZK metadata of the segment.
   * @param prunerContext Context for the pruner.
   *
   * @return True if the segment can be pruned out, false otherwise.
   */
  boolean prune(SegmentZKMetadata segmentZKMetadata, SegmentPrunerContext prunerContext);
}
