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
package org.apache.pinot.spi.stream.v2;

import java.util.Map;
import org.apache.pinot.spi.stream.StreamConfig;


public interface StreamConsumerFactoryV2 {
  void init(StreamConfig streamConfig);

  // takes the current state of partition groups (groupings of shards, the state of the consumption) and creates the new state
  PartitionGroupMetadataMap getPartitionGroupsMetadata(PartitionGroupMetadataMap currentPartitionGroupsMetadata);

  // creates a name generator which generates segment name for a partition group
  SegmentNameGenerator getSegmentNameGenerator();

  // creates a consumer which consumes from a partition group
  ConsumerV2 createConsumer(PartitionGroupMetadata metadata);

}
