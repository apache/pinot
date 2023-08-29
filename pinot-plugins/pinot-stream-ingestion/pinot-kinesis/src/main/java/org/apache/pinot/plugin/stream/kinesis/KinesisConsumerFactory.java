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

import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.PartitionGroupConsumptionStatus;
import org.apache.pinot.spi.stream.PartitionLevelConsumer;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffsetFactory;


/**
 * {@link StreamConsumerFactory} implementation for the Kinesis stream
 */
public class KinesisConsumerFactory extends StreamConsumerFactory {

  @Override
  public PartitionLevelConsumer createPartitionLevelConsumer(String clientId, int partition) {
    throw new UnsupportedOperationException();
  }
  @Override
  public StreamMetadataProvider createPartitionMetadataProvider(String clientId, int partition) {
    return new KinesisStreamMetadataProvider(clientId, _streamConfig);
  }

  @Override
  public StreamMetadataProvider createStreamMetadataProvider(String clientId) {
    return new KinesisStreamMetadataProvider(clientId, _streamConfig);
  }

  @Override
  public PartitionGroupConsumer createPartitionGroupConsumer(String clientId,
      PartitionGroupConsumptionStatus partitionGroupConsumptionStatus) {
    return new KinesisConsumer(new KinesisConfig(_streamConfig));
  }

  @Override
  public StreamPartitionMsgOffsetFactory createStreamMsgOffsetFactory() {
    return new KinesisMsgOffsetFactory();
  }
}
