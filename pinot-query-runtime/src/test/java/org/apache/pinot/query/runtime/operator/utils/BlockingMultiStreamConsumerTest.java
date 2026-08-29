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
import java.util.concurrent.atomic.AtomicReference;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


public class BlockingMultiStreamConsumerTest {
  @Test
  public void shouldTrackTheStreamThatActuallyProducedTheReturnedElement() {
    @SuppressWarnings("unchecked")
    AsyncStream<String> stream = mock(AsyncStream.class);
    AtomicReference<AsyncStream.OnNewData> onNewData = new AtomicReference<>();
    doAnswer(invocation -> {
      onNewData.set(invocation.getArgument(0));
      return null;
    }).when(stream).addOnNewDataListener(any());
    when(stream.poll()).thenReturn("data", null).thenThrow(new IllegalStateException("test exception"));
    try (TestConsumer consumer = new TestConsumer(new ArrayList<>(List.of(stream)))) {
      assertEquals(consumer.readBlockBlocking(), "data");
      assertSame(consumer.getLastReadStream(), stream);

      // Make the next read enter its notified (and exception-wrapping) path after the optimistic poll finds no data.
      onNewData.get().newDataAvailable();
      assertEquals(consumer.readBlockBlocking(), "exception");
      assertNull(consumer.getLastReadStream());
    }
  }

  @Test
  public void shouldReportOnlyStreamsWhoseEosWasConsumedDuringTheLastRead() {
    @SuppressWarnings("unchecked")
    AsyncStream<String> finishedStream = mock(AsyncStream.class);
    @SuppressWarnings("unchecked")
    AsyncStream<String> dataStream = mock(AsyncStream.class);
    when(finishedStream.poll()).thenReturn("success");
    when(dataStream.poll()).thenReturn("data");
    try (TestConsumer consumer = new TestConsumer(new ArrayList<>(List.of(finishedStream, dataStream)))) {
      assertEquals(consumer.readBlockBlocking(), "data");
      assertSame(consumer.getLastReadStream(), dataStream);
      assertEquals(consumer.getFinishedStreamsLastRead(), List.of(finishedStream));
      assertFalse(consumer.isStreamLive(finishedStream));
      assertTrue(consumer.isStreamLive(dataStream));
    }
  }

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
      assertFalse(consumer.isStreamLive(finishedStream));
      assertTrue(consumer.isStreamLive(waitingStream));
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

    try (TestConsumer consumer = new TestConsumer(new ArrayList<>(List.of(dataStream, finishedStream)))) {
      assertEquals(consumer.pollBlockOrStreamCompletion(), "data");
      assertSame(consumer.getLastReadStream(), dataStream);
      assertTrue(consumer.getFinishedStreamsLastRead().isEmpty());

      assertNull(consumer.pollBlockOrStreamCompletion());
      assertNull(consumer.getLastReadStream());
      assertEquals(consumer.getFinishedStreamsLastRead(), List.of(finishedStream));

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
