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
import org.apache.pinot.broker.routing.tablesampler.TableSampler;


/**
 * A {@link SegmentPreSelector} that applies an underlying pre-selector (e.g. segment lineage) and then applies a
 * {@link TableSampler} to deterministically choose a smaller subset.
 */
public class TableSamplerSegmentPreSelector implements SegmentPreSelector {
  private final SegmentPreSelector _delegate;
  private final TableSampler _tableSampler;

  public TableSamplerSegmentPreSelector(SegmentPreSelector delegate, TableSampler tableSampler) {
    _delegate = delegate;
    _tableSampler = tableSampler;
  }

  @Override
  public Set<String> preSelect(Set<String> onlineSegments) {
    Set<String> preSelected = _delegate.preSelect(onlineSegments);
    return _tableSampler.selectSegments(preSelected);
  }
}
