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
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nullable;


public class BlockingToAsyncStream<E> implements AsyncStream<E> {
  private final BlockingStream<E> _blockingStream;
  private final ReentrantLock _lock = new ReentrantLock();
  private final Executor _executor;
  @Nullable
  private CompletableFuture<E> _blockToRead;
  private OnNewData _onNewData;

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
    _lock.lock();
    try {
      if (_blockToRead == null) {
        _blockToRead = CompletableFuture.supplyAsync(this::askForNewBlock, _executor);
        return null;
      } else if (_blockToRead.isDone()) {
        E block = _blockToRead.getNow(null);
        assert block != null;

        _blockToRead = CompletableFuture.supplyAsync(this::askForNewBlock, _executor);
        return block;
      } else {
        return null;
      }
    } finally {
      _lock.unlock();
    }
  }

  private E askForNewBlock() {
    E block = _blockingStream.get();
    _lock.lock();
    try {
      if (_onNewData != null) {
        _onNewData.newDataAvailable();
      }
      return block;
    } finally {
      _lock.unlock();
    }
  }

  @Override
  public void addOnNewDataListener(OnNewData onNewData) {
    _lock.lock();
    try {
      Preconditions.checkState(_onNewData == null, "Another listener has been added");
      _onNewData = onNewData;
    } finally {
      _lock.unlock();
    }
  }

  @Override
  public void cancel() {
    _blockingStream.cancel();
    _lock.lock();
    try {
      if (_blockToRead != null) {
        _blockToRead.cancel(true);
      }
    } finally {
      _lock.unlock();
    }
  }
}
