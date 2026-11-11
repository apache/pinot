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
package org.apache.pinot.broker.routing.tablesampler.external;

import java.util.Set;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.routing.tablesampler.TableSampler;
import org.apache.pinot.spi.annotations.tablesampler.TableSamplerProvider;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.sampler.TableSamplerConfig;

@TableSamplerProvider(name = ExternalAnnotatedSampler.TYPE)
public class ExternalAnnotatedSampler implements TableSampler {
  public static final String TYPE = "externalAnnotatedSampler";

  @Override
  public void init(TableConfig tableConfig, TableSamplerConfig samplerConfig,
      ZkHelixPropertyStore<ZNRecord> propertyStore) {
  }

  @Override
  public Set<String> sampleSegments(Set<String> onlineSegments) {
    return onlineSegments;
  }
}
