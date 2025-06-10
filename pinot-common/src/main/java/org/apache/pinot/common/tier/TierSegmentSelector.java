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
package org.apache.pinot.common.tier;

import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;


/**
 * Interface for the segment selection strategy of a tier
 */
public interface TierSegmentSelector {

  /// Returns the type of the segment selector (e.g. TIME).
  String getType();

  /// Returns `true` if the segment is eligible for the tier.
  boolean selectSegment(String tableNameWithType, SegmentZKMetadata segmentZKMetadata);
}
