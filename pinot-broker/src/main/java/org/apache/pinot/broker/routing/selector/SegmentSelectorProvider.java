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
package org.apache.pinot.broker.routing.selector;

import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.config.TableConfig;


/**
 * Factory for segment selector
 */
public class SegmentSelectorProvider {

  private ZkHelixPropertyStore<ZNRecord> _propertyStore;

  public SegmentSelectorProvider(ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _propertyStore = propertyStore;
  }

  /**
   * Get a segment selector based on table config. This method returns null when no segment selector is required
   *
   * @param tableConfig a table config
   * @return a segment selector or returns null if no selector is required
   */
  public SegmentSelector getSegmentSelector(TableConfig tableConfig) {
    // TODO: add the support for merged segment selector once merge config is updated.
    return null;
  }
}
