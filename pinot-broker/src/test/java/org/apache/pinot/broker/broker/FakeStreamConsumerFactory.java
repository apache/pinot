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
package org.apache.pinot.broker.broker;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pinot.spi.stream.PartitionGroupConsumptionStatus;
import org.apache.pinot.spi.stream.PartitionGroupMetadata;
import org.apache.pinot.spi.stream.PartitionLevelConsumer;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamLevelConsumer;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;


public class FakeStreamConsumerFactory extends StreamConsumerFactory {
  @Override
  public PartitionLevelConsumer createPartitionLevelConsumer(String clientId, int partition) {
    return new FakePartitionLevelConsumer();
  }

  @Override
  public StreamLevelConsumer createStreamLevelConsumer(String clientId, String tableName, Set<String> fieldsToRead,
      String groupId) {
    return null;
  }

  @Override
  public StreamMetadataProvider createPartitionMetadataProvider(String clientId, int partition) {
    return new FakesStreamMetadataProvider();
  }

  @Override
  public StreamMetadataProvider createStreamMetadataProvider(String clientId) {
    return new FakesStreamMetadataProvider();
  }

  public class FakePartitionLevelConsumer implements PartitionLevelConsumer {

    @Override
    public MessageBatch fetchMessages(long startOffset, long endOffset, int timeoutMillis)
        throws TimeoutException {
      return null;
    }

    @Override
    public void close()
        throws IOException {
    }
  }

  public class FakesStreamMetadataProvider implements StreamMetadataProvider {

    @Override
    public List<PartitionGroupMetadata> computePartitionGroupMetadata(String clientId, StreamConfig streamConfig,
        List<PartitionGroupConsumptionStatus> partitionGroupConsumptionStatuses, int timeoutMillis)
        throws IOException, TimeoutException {
      return Collections.singletonList(new PartitionGroupMetadata(0, new LongMsgOffset(0)));
    }

    @Override
    public int fetchPartitionCount(long timeoutMillis) {
      return 1;
    }

    @Override
    public StreamPartitionMsgOffset fetchStreamPartitionOffset(OffsetCriteria offsetCriteria, long timeoutMillis)
        throws TimeoutException {
      return new LongMsgOffset(0);
    }

    @Override
    public void close()
        throws IOException {
    }
  }
}
