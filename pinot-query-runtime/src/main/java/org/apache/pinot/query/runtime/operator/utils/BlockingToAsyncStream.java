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
