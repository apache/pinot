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
package org.apache.pinot.plugin.stream.push;

import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.PartitionGroupConsumptionStatus;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamMetadataProvider;


/**
 * {@link StreamConsumerFactory} implementation for the Kinesis stream
 */
public class PushBasedIngestionConsumerFactory extends StreamConsumerFactory {

  private static volatile PushApiApplication _pushApiApplication;

  @Override
  public StreamMetadataProvider createPartitionMetadataProvider(String clientId, int partition) {
    return new PushBasedIngestionMetadataProvider(clientId, _streamConfig);
  }

  @Override
  public StreamMetadataProvider createStreamMetadataProvider(String clientId) {
    return new PushBasedIngestionMetadataProvider(clientId, _streamConfig);
  }

  @Override
  public PartitionGroupConsumer createPartitionGroupConsumer(String clientId,
      PartitionGroupConsumptionStatus partitionGroupConsumptionStatus) {
    // TODO this can be moved to pinot-server initialization
    // launch the application the first time
    if (_pushApiApplication == null) {
      synchronized (PushBasedIngestionConsumer.class) {
        if (_pushApiApplication == null) {
          _pushApiApplication = new PushApiApplication(new PushBasedIngestionBufferManager());
          // TODO make port configurable
          _pushApiApplication.start(8989);
        }
      }
    }
    return new PushBasedIngestionConsumer(new PushBasedIngestionConfig(_streamConfig),
        _pushApiApplication.getBufferManager());
  }
}
