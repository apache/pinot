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
package org.apache.pinot.plugin.stream.kinesis;

import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.v2.ConsumerV2;
import org.apache.pinot.spi.stream.v2.PartitionGroupMetadata;
import org.apache.pinot.spi.stream.v2.PartitionGroupMetadataMap;
import org.apache.pinot.spi.stream.v2.SegmentNameGenerator;
import org.apache.pinot.spi.stream.v2.StreamConsumerFactoryV2;


public class KinesisConsumerFactory implements StreamConsumerFactoryV2 {
  private KinesisConfig _kinesisConfig;

  @Override
  public void init(StreamConfig streamConfig) {
    _kinesisConfig = new KinesisConfig(streamConfig);
  }

  @Override
  public PartitionGroupMetadataMap getPartitionGroupsMetadata(
      PartitionGroupMetadataMap currentPartitionGroupsMetadata) {
    return new KinesisPartitionGroupMetadataMap(_kinesisConfig.getStream(), _kinesisConfig.getAwsRegion(),
        currentPartitionGroupsMetadata);
  }

  @Override
  public SegmentNameGenerator getSegmentNameGenerator() {
    return null;
  }

  @Override
  public ConsumerV2 createConsumer(PartitionGroupMetadata metadata) {
    return new KinesisConsumer(_kinesisConfig, metadata);
  }
}
