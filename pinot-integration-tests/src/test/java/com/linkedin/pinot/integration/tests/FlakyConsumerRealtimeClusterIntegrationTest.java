/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.realtime.StreamProvider;
import com.linkedin.pinot.core.realtime.StreamProviderConfig;
import com.linkedin.pinot.core.realtime.StreamProviderFactory;
import java.util.Random;
import org.testng.annotations.BeforeClass;


/**
 * Integration test that simulates a flaky Kafka consumer.
 */
public class FlakyConsumerRealtimeClusterIntegrationTest extends RealtimeClusterIntegrationTest {
  private static final Class<? extends StreamProvider> ORIGINAL_STREAM_PROVIDER =
      StreamProviderFactory.getStreamProviderClass();

  @BeforeClass
  @Override
  public void setUp()
      throws Exception {
    StreamProviderFactory.setStreamProviderClass(FlakyStreamProvider.class);
    super.setUp();
  }

  public static class FlakyStreamProvider implements StreamProvider {
    private StreamProvider _streamProvider;
    private Random _random = new Random();

    public FlakyStreamProvider()
        throws Exception {
      _streamProvider = ORIGINAL_STREAM_PROVIDER.newInstance();
    }

    @Override
    public void init(StreamProviderConfig streamProviderConfig, String tableName, ServerMetrics serverMetrics)
        throws Exception {
      _streamProvider.init(streamProviderConfig, tableName, serverMetrics);
    }

    @Override
    public void start()
        throws Exception {
      _streamProvider.start();
    }

    @Override
    public void setOffset(long offset) {
      _streamProvider.setOffset(offset);
    }

    @Override
    public GenericRow next(GenericRow destination) {
      // Return a null row every ~1/1000 rows and an exception every ~1/1000 rows
      int randomValue = _random.nextInt(1000);

      if (randomValue == 0) {
        return null;
      } else if (randomValue == 1) {
        throw new RuntimeException("Flaky stream provider exception");
      } else {
        return _streamProvider.next(destination);
      }
    }

    @Override
    public GenericRow next(long offset) {
      return _streamProvider.next(offset);
    }

    @Override
    public long currentOffset() {
      return _streamProvider.currentOffset();
    }

    @Override
    public void commit() {
      // Fail to commit 50% of the time
      boolean failToCommit = _random.nextBoolean();

      if (failToCommit) {
        throw new RuntimeException("Flaky stream provider exception");
      } else {
        _streamProvider.commit();
      }
    }

    @Override
    public void commit(long offset) {
      _streamProvider.commit(offset);
    }

    @Override
    public void shutdown()
        throws Exception {
      _streamProvider.shutdown();
    }
  }
}
