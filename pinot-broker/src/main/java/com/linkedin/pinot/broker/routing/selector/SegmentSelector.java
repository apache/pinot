/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.broker.routing.selector;

import com.linkedin.pinot.broker.routing.RoutingTableLookupRequest;
import com.linkedin.pinot.common.config.TableConfig;
import java.util.Set;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;


/**
 * Interface for segment selector
 */
public interface SegmentSelector {

  /**
   * Initiate the segment selector
   */
  void init(TableConfig tableConfig, ZkHelixPropertyStore<ZNRecord> propertyStore);

  /**
   * Compute and update the information required for a selector
   * <p>Should be called whenever there is an external view change
   */
  void computeOnExternalViewChange();

  /**
   * Compute selection algorithm.
   * <p>Should NOT modify input list of segments to query. Should copy it if any modification is required.
   */
  Set<String> selectSegments(RoutingTableLookupRequest request, Set<String> segmentsToQuery);
}
