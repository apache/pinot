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
package org.apache.pinot.query.mailbox.channel;

import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.common.proto.Mailbox;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Tests backpressure and terminal cleanup for materialized partition server streaming.
public class GrpcMailboxServerMaterializedStreamingTest {

  @Test
  public void testPumpStopsWhenStreamBecomesNotReady() {
    AtomicBoolean ready = new AtomicBoolean();
    AtomicReference<Runnable> onReady = new AtomicReference<>();
    ServerCallStreamObserver<Mailbox.MaterializedPartitionContent> observer = observer(ready, onReady, null);
    doAnswer(invocation -> {
      ready.set(false);
      return null;
    }).when(observer).onNext(any());

    GrpcMailboxServer.MaterializedPartitionPump pump =
        new GrpcMailboxServer.MaterializedPartitionPump(List.of(new byte[]{1, 2, 3, 4}).iterator(), observer, 2);
    pump.start();
    verify(observer, never()).onNext(any());

    ready.set(true);
    onReady.get().run();
    verify(observer, times(1)).onNext(any());
    verify(observer, never()).onCompleted();

    ready.set(true);
    onReady.get().run();
    verify(observer, times(2)).onNext(any());
    verify(observer, never()).onCompleted();

    ready.set(true);
    onReady.get().run();
    verify(observer, times(1)).onCompleted();
  }

  @Test
  public void testCancellationClosesReaderWithoutSignalingCompletion() {
    AtomicBoolean ready = new AtomicBoolean();
    AtomicReference<Runnable> onReady = new AtomicReference<>();
    AtomicReference<Runnable> onCancel = new AtomicReference<>();
    ServerCallStreamObserver<Mailbox.MaterializedPartitionContent> observer = observer(ready, onReady, onCancel);
    CloseableIterator records = new CloseableIterator(List.of(new byte[]{1}).iterator());

    new GrpcMailboxServer.MaterializedPartitionPump(records, observer, 1).start();
    onCancel.get().run();
    onCancel.get().run();
    onReady.get().run();

    assertTrue(records._closed);
    assertEquals(records._closeCount, 1);
    verify(observer, never()).onNext(any());
    verify(observer, never()).onCompleted();
    verify(observer, never()).onError(any());
  }

  @Test
  public void testReadFailureClosesReaderAndSignalsErrorOnce() {
    AtomicBoolean ready = new AtomicBoolean(true);
    AtomicReference<Runnable> onReady = new AtomicReference<>();
    ServerCallStreamObserver<Mailbox.MaterializedPartitionContent> observer = observer(ready, onReady, null);
    CloseableIterator records = new CloseableIterator(List.of(new byte[]{1}).iterator());
    records._fail = true;

    new GrpcMailboxServer.MaterializedPartitionPump(records, observer, 1).start();
    onReady.get().run();

    assertTrue(records._closed);
    assertEquals(records._closeCount, 1);
    ArgumentCaptor<Throwable> error = ArgumentCaptor.forClass(Throwable.class);
    verify(observer, times(1)).onError(error.capture());
    assertEquals(Status.fromThrowable(error.getValue()).getCode(), Status.Code.INTERNAL);
    verify(observer, never()).onCompleted();
  }

  @Test
  public void testSuccessfulReadDoesNotAbortReader() {
    AtomicBoolean ready = new AtomicBoolean(true);
    AtomicReference<Runnable> onReady = new AtomicReference<>();
    ServerCallStreamObserver<Mailbox.MaterializedPartitionContent> observer =
        observer(ready, onReady, null);
    CloseableIterator records = new CloseableIterator(List.of(new byte[]{1}).iterator());

    new GrpcMailboxServer.MaterializedPartitionPump(records, observer, 1).start();
    onReady.get().run();

    assertFalse(records._closed);
    verify(observer, times(1)).onCompleted();
    verify(observer, never()).onError(any());
  }

  @SuppressWarnings("unchecked")
  private static ServerCallStreamObserver<Mailbox.MaterializedPartitionContent> observer(AtomicBoolean ready,
      AtomicReference<Runnable> onReady, AtomicReference<Runnable> onCancel) {
    ServerCallStreamObserver<Mailbox.MaterializedPartitionContent> observer = mock(ServerCallStreamObserver.class);
    when(observer.isReady()).thenAnswer(invocation -> ready.get());
    when(observer.isCancelled()).thenReturn(false);
    doAnswer(invocation -> {
      onReady.set(invocation.getArgument(0));
      return null;
    }).when(observer).setOnReadyHandler(any());
    if (onCancel != null) {
      doAnswer(invocation -> {
        onCancel.set(invocation.getArgument(0));
        return null;
      }).when(observer).setOnCancelHandler(any());
    }
    return observer;
  }

  private static final class CloseableIterator implements Iterator<byte[]>, AutoCloseable {
    private final Iterator<byte[]> _delegate;
    private boolean _closed;
    private boolean _fail;
    private int _closeCount;

    private CloseableIterator(Iterator<byte[]> delegate) {
      _delegate = delegate;
    }

    @Override
    public boolean hasNext() {
      if (_fail) {
        throw new IllegalStateException("read failed");
      }
      return _delegate.hasNext();
    }

    @Override
    public byte[] next() {
      return _delegate.next();
    }

    @Override
    public void close() {
      _closed = true;
      _closeCount++;
    }
  }
}
