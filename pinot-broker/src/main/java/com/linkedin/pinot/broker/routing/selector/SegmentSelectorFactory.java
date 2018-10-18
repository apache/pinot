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

import com.linkedin.pinot.common.config.TableConfig;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;


/**
 * Factory for segment selector
 */
public class SegmentSelectorFactory {

  private ZkHelixPropertyStore<ZNRecord> _propertyStore;

  public SegmentSelectorFactory(ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _propertyStore = propertyStore;
  }

  // TODO: update this once we have more types of selectors (e.g. merged segment selector)
  public SegmentSelector createSegmentSelector(TableConfig tableConfig) {
    SegmentSelector segmentSelector = new DefaultSegmentSelector();
    segmentSelector.init(tableConfig, _propertyStore);
    return segmentSelector;
  }
}
