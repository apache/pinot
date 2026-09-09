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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


/**
 * Single-threaded owner of transferred block references. Operator state is released from close(), not cancel().
 */
public final class HeldBlocks {
  private final List<ArrowBlock> _blocks = new ArrayList<>();
  private boolean _released;

  /** Accepts the caller's existing reference without retaining it. */
  public ArrowBlock holdTransferred(ArrowBlock block) {
    checkOpen();
    _blocks.add(block);
    return block;
  }

  public List<ArrowBlock> blocks() {
    return Collections.unmodifiableList(_blocks);
  }

  void transferOutput(ArrowBlock block) {
    checkOpen();
    _blocks.remove(block);
  }

  /** Releases each owned reference once, including when another release fails. Repeated calls are harmless. */
  public void releaseAll() {
    if (_released) {
      return;
    }
    _released = true;
    IllegalStateException failure = null;
    for (ArrowBlock block : _blocks) {
      try {
        block.release();
      } catch (IllegalStateException e) {
        if (failure == null) {
          failure = e;
        } else if (failure != e) {
          failure.addSuppressed(e);
        }
      }
    }
    _blocks.clear();
    if (failure != null) {
      throw failure;
    }
  }

  private void checkOpen() {
    Preconditions.checkState(!_released, "Held Arrow blocks have been released");
  }
}
