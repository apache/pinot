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
package org.apache.pinot.broker.routing.tablesampler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections4.MapUtils;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.spi.annotations.tablesampler.TableSamplerProvider;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.sampler.TableSamplerConfig;


/**
 * Selects the first N segments after sorting segment names lexicographically.
 *
 * <p>Config:
 * <ul>
 *   <li>{@code properties.numSegments}: positive integer</li>
 * </ul>
 */
@TableSamplerProvider(name = FirstNSegmentsTableSampler.TYPE)
public class FirstNSegmentsTableSampler implements TableSampler {
  public static final String TYPE = "firstN";
  public static final String PROP_NUM_SEGMENTS = "numSegments";

  private int _numSegments;

  @Override
  public void init(TableConfig tableConfig, TableSamplerConfig samplerConfig,
      ZkHelixPropertyStore<ZNRecord> propertyStore) {
    Map<String, String> props = samplerConfig.getProperties();
    if (MapUtils.isEmpty(props) || !props.containsKey(PROP_NUM_SEGMENTS)) {
      throw new IllegalArgumentException(
          "Missing required property '" + PROP_NUM_SEGMENTS + "' for table sampler type '" + TYPE + "'");
    }
    _numSegments = Integer.parseInt(props.get(PROP_NUM_SEGMENTS));
    if (_numSegments <= 0) {
      throw new IllegalArgumentException("'" + PROP_NUM_SEGMENTS + "' must be positive");
    }
  }

  @Override
  public Set<String> sampleSegments(Set<String> onlineSegments) {
    if (onlineSegments.isEmpty()) {
      return Collections.emptySet();
    }
    if (onlineSegments.size() <= _numSegments) {
      return onlineSegments;
    }
    List<String> sorted = new ArrayList<>(onlineSegments);
    Collections.sort(sorted);
    return new HashSet<>(sorted.subList(0, _numSegments));
  }
}
