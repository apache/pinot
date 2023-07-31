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

import com.google.common.base.Preconditions;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;


public class BlockingToAsyncStream<E> implements AsyncStream<E> {
  private final BlockingStream<E> _blockingStream;
  private final Executor _executor;
  /**
   * A completable future that contains the next element to return.
   *
   * Only the thread that calls {@link #poll()} is allowed to modify or read this attribute.
   */
  @Nullable
  private CompletableFuture<E> _blockToRead;
  /**
   * The callback used to indicate that there is more data.
   *
   * It is mandatory to register a callback here before calling {@link #poll()}.
   */
  private final AtomicReference<OnNewData> _onNewData = new AtomicReference<>();

  public BlockingToAsyncStream(Executor executor, BlockingStream<E> blockingStream) {
    _executor = executor;
    _blockingStream = blockingStream;
  }

  @Override
  public Object getId() {
    return _blockingStream.getId();
  }

  @Nullable
  @Override
  public E poll() {
    Preconditions.checkState(_onNewData.get() != null, "Reading while no new data callback is added may imply data "
        + "loss");
    CompletableFuture<E> blockToRead = _blockToRead;
    if (blockToRead == null) {
      _blockToRead = CompletableFuture.supplyAsync(this::askForNewBlock, _executor);
      return null;
    } else if (blockToRead.isDone()) {
      E block = blockToRead.getNow(null);
      assert block != null;

      _blockToRead = CompletableFuture.supplyAsync(this::askForNewBlock, _executor);
      return block;
    } else {
      return null;
    }
  }

  private E askForNewBlock() {
    E block = _blockingStream.get();
    OnNewData onNewData = _onNewData.get();
    if (onNewData != null) {
      onNewData.newDataAvailable();
    }
    return block;
  }

  @Override
  public void addOnNewDataListener(OnNewData onNewData) {
    boolean success = _onNewData.compareAndSet(null, onNewData);
    if (!success) {
      throw new IllegalArgumentException("Another listener has been added");
    }
  }

  @Override
  public void cancel() {
    try {
      _blockingStream.cancel();
    } finally {
      if (_blockToRead != null) {
        _blockToRead.cancel(true);
      }
    }
  }
}
