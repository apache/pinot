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
package org.apache.pinot.core.data.manager.realtime;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConsumerFactory;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.PartitionGroupConsumptionStatus;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.utils.retry.RetryPolicy;

/// Test [org.apache.pinot.spi.stream.StreamConsumerFactory] that records which create overload was used
/// and can fail create to simulate exhausted stream-consumer init.
public class RecordingStreamConsumerFactory extends FakeStreamConsumerFactory {
  static final AtomicInteger CREATE_WITHOUT_POLICY_COUNT = new AtomicInteger();
  static final AtomicInteger CREATE_WITH_POLICY_COUNT = new AtomicInteger();
  static final AtomicReference<RetryPolicy> LAST_RETRY_POLICY = new AtomicReference<>();
  static final AtomicBoolean FAIL_CREATE = new AtomicBoolean();
  static final AtomicReference<Runnable> BEFORE_CREATE_WITH_POLICY = new AtomicReference<>();
  static final AtomicInteger CLOSE_COUNT = new AtomicInteger();

  static void reset() {
    CREATE_WITHOUT_POLICY_COUNT.set(0);
    CREATE_WITH_POLICY_COUNT.set(0);
    LAST_RETRY_POLICY.set(null);
    FAIL_CREATE.set(false);
    BEFORE_CREATE_WITH_POLICY.set(null);
    CLOSE_COUNT.set(0);
  }

  @Override
  public PartitionGroupConsumer createPartitionGroupConsumer(String clientId,
      PartitionGroupConsumptionStatus partitionGroupConsumptionStatus) {
    CREATE_WITHOUT_POLICY_COUNT.incrementAndGet();
    if (FAIL_CREATE.get()) {
      throw new RuntimeException("stream consumer create failed");
    }
    return super.createPartitionGroupConsumer(clientId, partitionGroupConsumptionStatus);
  }

  @Override
  public PartitionGroupConsumer createPartitionGroupConsumer(String clientId,
      PartitionGroupConsumptionStatus partitionGroupConsumptionStatus, RetryPolicy retryPolicy) {
    CREATE_WITH_POLICY_COUNT.incrementAndGet();
    LAST_RETRY_POLICY.set(retryPolicy);
    Runnable beforeCreate = BEFORE_CREATE_WITH_POLICY.get();
    if (beforeCreate != null) {
      beforeCreate.run();
    }
    if (FAIL_CREATE.get()) {
      throw new RuntimeException("stream consumer create exhausted");
    }
    PartitionGroupConsumer delegate =
        super.createPartitionGroupConsumer(clientId, partitionGroupConsumptionStatus);
    return new CloseCountingPartitionGroupConsumer(delegate);
  }

  private static final class CloseCountingPartitionGroupConsumer implements PartitionGroupConsumer {
    private final PartitionGroupConsumer _delegate;

    CloseCountingPartitionGroupConsumer(PartitionGroupConsumer delegate) {
      _delegate = delegate;
    }

    @Override
    public void start(StreamPartitionMsgOffset startOffset) {
      _delegate.start(startOffset);
    }

    @Override
    public MessageBatch fetchMessages(StreamPartitionMsgOffset startOffset, int timeoutMs)
        throws TimeoutException {
      return _delegate.fetchMessages(startOffset, timeoutMs);
    }

    @Override
    public StreamPartitionMsgOffset checkpoint(StreamPartitionMsgOffset lastOffset) {
      return _delegate.checkpoint(lastOffset);
    }

    @Override
    public void close()
        throws IOException {
      CLOSE_COUNT.incrementAndGet();
      _delegate.close();
    }
  }
}
