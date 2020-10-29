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
package org.apache.pinot.broker.routing.segmentpreselector;

import java.util.Set;


/**
 * The segment pre-selector filters the unnecessary online segments for the query.
 * <p>Segment pre-selector examples:
 * <ul>
 *   <li>
 *     For table with segment merge/rollup enabled, select the merged segments over the original segments with the same
 *     data
 *   </li>
 * </ul>
 */
public interface SegmentPreSelector {

  /**
   * Pre-selects the online segments to filter out the unnecessary segments. This method might modify the online segment
   * set passed in.
   */
  Set<String> preSelect(Set<String> onlineSegments);
}
