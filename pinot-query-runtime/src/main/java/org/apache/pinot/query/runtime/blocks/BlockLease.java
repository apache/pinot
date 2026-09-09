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
package org.apache.pinot.query.runtime.blocks;

import com.google.common.base.Preconditions;
import javax.annotation.Nullable;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;


/**
 * Single-threaded scope releasing transient inputs while preserving output ownership. Does not retain blocks.
 */
public final class BlockLease implements AutoCloseable {
  private final HeldBlocks _inputs = new HeldBlocks();
  @Nullable
  private ArrowBlock _output;
  private boolean _closed;

  private BlockLease() {
  }

  public static BlockLease open(OpChainExecutionContext context) {
    Preconditions.checkState(context.isArrowEnabled(), "Arrow is not enabled for this op-chain");
    return new BlockLease();
  }

  /** Takes the caller's reference and releases it at scope exit unless returned as output. */
  public ArrowBlock consume(ArrowBlock block) {
    return _inputs.holdTransferred(block);
  }

  /** Transfers a getNextBlock() return value, not a mailbox enqueue that has yet to succeed. */
  public <T extends MseBlock> T returnOutput(T block) {
    Preconditions.checkState(!_closed, "Arrow block lease is closed");
    if (block instanceof ArrowBlock) {
      Preconditions.checkState(_output == null, "Arrow block lease already has an output");
      _inputs.transferOutput((ArrowBlock) block);
      _output = (ArrowBlock) block;
    }
    return block;
  }

  @Override
  public void close() {
    if (_closed) {
      return;
    }
    _closed = true;
    boolean released = false;
    try {
      _inputs.releaseAll();
      released = true;
    } finally {
      ArrowBlock output = _output;
      _output = null;
      // A failing scope close prevents the Java return from transferring ownership to the caller.
      if (!released && output != null) {
        output.release();
      }
    }
  }
}
