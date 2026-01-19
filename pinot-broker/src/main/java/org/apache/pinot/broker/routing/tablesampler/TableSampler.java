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

import java.util.Set;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.sampler.TableSamplerConfig;


/**
 * A {@code TableSampler} deterministically selects a subset of segments from the set of online segments for a table.
 *
 * <p>Selection is performed during routing table build/update so there is no additional per-query overhead beyond
 * selecting the pre-built routing entry.
 */
public interface TableSampler {

  /**
   * Initializes the sampler for a specific table and sampler config.
   */
  void init(TableConfig tableConfig, TableSamplerConfig samplerConfig, ZkHelixPropertyStore<ZNRecord> propertyStore);

  /**
   * Selects a subset of segments from the provided online segments.
   *
   * <p>Implementations must not mutate the input set because the same pre-selected segment set can be reused by
   * multiple samplers.
   */
  Set<String> sampleSegments(Set<String> onlineSegments);
}
