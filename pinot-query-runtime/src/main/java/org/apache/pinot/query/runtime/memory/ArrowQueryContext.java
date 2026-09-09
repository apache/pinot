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
package org.apache.pinot.query.runtime.memory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.pinot.common.datablock.ArrowDataBlock;
import org.apache.pinot.query.runtime.blocks.ArrowBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Owns an attempt's allocator and strongly tracks every constructed block until its terminal release.
 *
 * <p>Construction and cleanup are serialized; ordinary releases may occur concurrently. {@link #close()} requires
 * all operators, callbacks and consumers to be quiescent. Blocks must not escape into a longer-lived mailbox scope.
 */
public final class ArrowQueryContext implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ArrowQueryContext.class);

  private final BufferAllocator _allocator;
  private final Runnable _onClosed;
  private final Set<ArrowBlock> _liveBlocks = ConcurrentHashMap.newKeySet();
  private boolean _closed;

  /**
   * Takes ownership of a standalone allocator. Production attempts use {@link ArrowBuffers#newQueryContext(String)}.
   */
  public ArrowQueryContext(BufferAllocator allocator) {
    this(allocator, () -> {
    });
  }

  ArrowQueryContext(BufferAllocator allocator, Runnable onClosed) {
    _allocator = allocator;
    _onClosed = onClosed;
  }

  public synchronized BufferAllocator getAllocator() {
    checkOpen();
    return _allocator;
  }

  /**
   * Constructs and registers a block atomically. Ownership of data allocated by this context transfers on success.
   */
  public synchronized ArrowBlock createBlock(ArrowDataBlock dataBlock) {
    checkOpen();
    return new ArrowBlock(dataBlock, this);
  }

  /** Construction hook for {@link ArrowBlock}; callers should use {@link #createBlock(ArrowDataBlock)}. */
  public synchronized void registerBlock(ArrowBlock block) {
    checkOpen();
    Preconditions.checkState(_liveBlocks.add(block), "Arrow block is already registered");
  }

  /** Terminal-release hook; removes the block before its vectors are freed. */
  public void deregisterBlock(ArrowBlock block) {
    _liveBlocks.remove(block);
  }

  @VisibleForTesting
  public int getLiveBlockCount() {
    return _liveBlocks.size();
  }

  /**
   * Sweeps missed releases before closing the allocator. Call only after quiescence, never directly on cancellation.
   */
  @Override
  public synchronized void close() {
    if (_closed) {
      return;
    }
    _closed = true;
    try (BufferAllocator ignored = _allocator) {
      IllegalStateException failure = null;
      for (ArrowBlock block : _liveBlocks) {
        LOGGER.warn("Leaked Arrow block in allocator {}: {}", _allocator.getName(), block);
        try {
          block.forceRelease();
        } catch (IllegalStateException e) {
          if (failure == null) {
            failure = e;
          } else if (failure != e) {
            failure.addSuppressed(e);
          }
        }
      }
      if (failure != null) {
        throw failure;
      }
    } finally {
      _onClosed.run();
    }
  }

  private void checkOpen() {
    Preconditions.checkState(!_closed, "Arrow query context is closed");
  }
}
