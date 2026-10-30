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
package org.apache.pinot.core.query.scheduler;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.LongAccumulator;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.scheduler.resources.UnboundedResourceManager;
import org.apache.pinot.segment.spi.memory.CompoundDataBuffer;
import org.apache.pinot.segment.spi.memory.DataBuffer;
import org.apache.pinot.spi.accounting.ThreadAccountantUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


/// Tests the bridge between the deprecated `byte[]` API of [QueryScheduler] and the [DataBuffer] API, in both
/// directions.
public class QuerySchedulerResponseApiTest {
  private static final byte[] RESPONSE_BYTES = {1, 2, 3, 4, 5, 6, 7};
  private static final PinotConfiguration CONFIG = new PinotConfiguration();

  /// A scheduler written against the old API, like the ones outside this repository.
  @Test
  public void testSubmitResponseWrapsDeprecatedSubmit()
      throws Exception {
    QueryScheduler scheduler = new TestScheduler() {
      @Override
      @SuppressWarnings("deprecation")
      public ListenableFuture<byte[]> submit(ServerQueryRequest queryRequest) {
        return Futures.immediateFuture(RESPONSE_BYTES);
      }
    };
    DataBuffer response = scheduler.submitResponse(mock(ServerQueryRequest.class)).get();
    byte[] bytes = new byte[Math.toIntExact(response.size())];
    response.copyTo(0, bytes);
    assertEquals(bytes, RESPONSE_BYTES);
  }

  /// A scheduler written against the new API, called through the deprecated method.
  @Test
  @SuppressWarnings("deprecation")
  public void testDeprecatedSubmitCopiesSubmitResponse()
      throws Exception {
    QueryScheduler scheduler = new TestScheduler() {
      @Override
      public ListenableFuture<DataBuffer> submitResponse(ServerQueryRequest queryRequest) {
        // Split the response into three buffers.
        CompoundDataBuffer.Builder builder = new CompoundDataBuffer.Builder();
        builder.addBuffer(ByteBuffer.wrap(RESPONSE_BYTES, 0, 2).slice());
        builder.addBuffer(ByteBuffer.wrap(RESPONSE_BYTES, 2, 3).slice());
        builder.addBuffer(ByteBuffer.wrap(RESPONSE_BYTES, 5, 2).slice());
        return Futures.immediateFuture(builder.build());
      }
    };
    assertEquals(scheduler.submit(mock(ServerQueryRequest.class)).get(), RESPONSE_BYTES);
  }

  @Test
  public void testSchedulerMustOverrideOneSubmitMethod() {
    assertThrows(IllegalStateException.class, TestScheduler::new);
  }

  /// Overrides neither submit method, so subclasses choose which one to override.
  private static class TestScheduler extends QueryScheduler {
    TestScheduler() {
      super(CONFIG, "serverId", mock(QueryExecutor.class), ThreadAccountantUtils.getNoOpAccountant(),
          new LongAccumulator(Long::max, 0), new UnboundedResourceManager(CONFIG));
    }

    @Override
    public String name() {
      return "test";
    }
  }
}
