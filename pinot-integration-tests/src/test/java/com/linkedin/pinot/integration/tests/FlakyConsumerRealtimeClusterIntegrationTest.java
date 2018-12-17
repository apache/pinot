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
package com.linkedin.pinot.integration.tests;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaStreamLevelConsumer;
import com.linkedin.pinot.core.realtime.stream.PartitionLevelConsumer;
import com.linkedin.pinot.core.realtime.stream.StreamConfig;
import com.linkedin.pinot.core.realtime.stream.StreamConsumerFactory;
import com.linkedin.pinot.core.realtime.stream.StreamLevelConsumer;
import com.linkedin.pinot.core.realtime.stream.StreamMetadataProvider;
import java.util.Random;
import org.testng.annotations.BeforeClass;


/**
 * Integration test that simulates a flaky Kafka consumer.
 */
public class FlakyConsumerRealtimeClusterIntegrationTest extends RealtimeClusterIntegrationTest {

  @BeforeClass
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  protected String getStreamConsumerFactoryClassName() {
    return FlakyStreamFactory.class.getName();
  }

  public static class FlakyStreamLevelConsumer implements StreamLevelConsumer {
    private StreamLevelConsumer _streamLevelConsumer;
    private Random _random = new Random();

    public FlakyStreamLevelConsumer(String clientId, String tableName, StreamConfig streamConfig, Schema schema,
        InstanceZKMetadata instanceZKMetadata, ServerMetrics serverMetrics) {
      _streamLevelConsumer =
          new KafkaStreamLevelConsumer(clientId, tableName, streamConfig, schema, instanceZKMetadata, serverMetrics);
    }

    @Override
    public void start() throws Exception {
      _streamLevelConsumer.start();
    }

    @Override
    public GenericRow next(GenericRow destination) {
      // Return a null row every ~1/1000 rows and an exception every ~1/1000 rows
      int randomValue = _random.nextInt(1000);

      if (randomValue == 0) {
        return null;
      } else if (randomValue == 1) {
        throw new RuntimeException("Flaky stream level consumer exception");
      } else {
        return _streamLevelConsumer.next(destination);
      }
    }

    @Override
    public void commit() {
      // Fail to commit 50% of the time
      boolean failToCommit = _random.nextBoolean();

      if (failToCommit) {
        throw new RuntimeException("Flaky stream level consumer exception");
      } else {
        _streamLevelConsumer.commit();
      }
    }

    @Override
    public void shutdown() throws Exception {
      _streamLevelConsumer.shutdown();
    }
  }

  public static class FlakyStreamFactory extends StreamConsumerFactory {
    @Override
    public PartitionLevelConsumer createPartitionLevelConsumer(String clientId, int partition) {
      throw new UnsupportedOperationException();
    }

    @Override
    public StreamLevelConsumer createStreamLevelConsumer(String clientId, String tableName, Schema schema,
        InstanceZKMetadata instanceZKMetadata, ServerMetrics serverMetrics) {
      return new FlakyStreamLevelConsumer(clientId, tableName, _streamConfig, schema, instanceZKMetadata,
          serverMetrics);
    }

    @Override
    public StreamMetadataProvider createPartitionMetadataProvider(String clientId, int partition) {
      throw new UnsupportedOperationException();
    }

    @Override
    public StreamMetadataProvider createStreamMetadataProvider(String clientId) {
      throw new UnsupportedOperationException();
    }
  }
}
