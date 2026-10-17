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
package org.apache.pinot.query.runtime.operator.utils;

import java.util.ArrayList;
import java.util.List;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


public class BlockingMultiStreamConsumerTest {
  @Test(timeOut = 10_000)
  public void shouldReturnStreamCompletionWithoutWaitingForUnrelatedData() {
    @SuppressWarnings("unchecked")
    AsyncStream<String> finishedStream = mock(AsyncStream.class);
    @SuppressWarnings("unchecked")
    AsyncStream<String> waitingStream = mock(AsyncStream.class);
    when(finishedStream.poll()).thenReturn("success");
    when(waitingStream.poll()).thenReturn(null);

    try (TestConsumer consumer = new TestConsumer(new ArrayList<>(List.of(finishedStream, waitingStream)))) {
      assertNull(consumer.readBlockOrStreamCompletionBlocking());
      assertEquals(consumer.getFinishedStreamsLastRead(), List.of(finishedStream));
      assertEquals(consumer.getLiveStreamsSnapshot(), List.of(waitingStream));
    }
  }

  @Test
  public void shouldPollDataCompletionAndNoProgressWithoutRetainingPriorState() {
    @SuppressWarnings("unchecked")
    AsyncStream<String> dataStream = mock(AsyncStream.class);
    @SuppressWarnings("unchecked")
    AsyncStream<String> finishedStream = mock(AsyncStream.class);
    when(dataStream.poll()).thenReturn("data").thenReturn(null);
    when(finishedStream.poll()).thenReturn("success");

    try (TestConsumer consumer = new TestConsumer(new ArrayList<>(List.of(finishedStream, dataStream)))) {
      assertEquals(consumer.pollBlockOrStreamCompletion(), "data");
      assertSame(consumer.getLastReadStream(), dataStream);
      assertEquals(consumer.getFinishedStreamsLastRead(), List.of(finishedStream));
      assertEquals(consumer.getLiveStreamsSnapshot(), List.of(dataStream));

      assertNull(consumer.pollBlockOrStreamCompletion());
      assertNull(consumer.getLastReadStream());
      assertTrue(consumer.getFinishedStreamsLastRead().isEmpty());
    }
  }

  private static class TestConsumer extends BlockingMultiStreamConsumer<String> {
    TestConsumer(List<AsyncStream<String>> streams) {
      super("test", Long.MAX_VALUE, streams);
    }

    @Override
    protected boolean isError(String element) {
      return "error".equals(element);
    }

    @Override
    protected boolean isSuccess(String element) {
      return "success".equals(element);
    }

    @Override
    protected void onMailboxSuccess(String element) {
    }

    @Override
    protected String onTimeout() {
      return "timeout";
    }

    @Override
    protected String onException(Exception e) {
      return "exception";
    }

    @Override
    protected String onSuccess() {
      return "success";
    }

    @Override
    protected void onError(String element) {
    }
  }
}
