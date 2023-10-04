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
package org.apache.pinot.core.data.manager.realtime;

import org.apache.pinot.common.protocols.SegmentCompletionProtocol;


/**
 * For committing realtime segments.
 */
public interface SegmentCommitter {
  /**
   * Commits a realtime segment to persistent store
   * @param segmentBuildDescriptor object that describes segment to be committed
   * @return
   */
  SegmentCompletionProtocol.Response commit(RealtimeSegmentDataManager.SegmentBuildDescriptor segmentBuildDescriptor);
}
