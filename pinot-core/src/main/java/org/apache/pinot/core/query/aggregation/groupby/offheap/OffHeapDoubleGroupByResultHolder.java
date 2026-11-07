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
package org.apache.pinot.core.query.aggregation.groupby.offheap;

import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;


/// Off-heap implementation of [GroupByResultHolder] for double results, backed by a single direct-memory
/// [PinotDataBuffer]. Drop-in replacement for
/// [org.apache.pinot.core.query.aggregation.groupby.DoubleGroupByResultHolder] with identical semantics
/// observed through the interface: each group key indexes a fixed-width 8-byte slot, un-initialized slots return
/// the default value, and [GroupKeyGenerator#INVALID_ID] returns the default value on get and is silently
/// ignored on set.
///
/// An initial capacity of 0 is legal (a zero-size buffer is allocated); in that state no getter or setter may
/// be called until [#ensureCapacity(int)] grows the holder.
///
/// [#close()] releases the direct memory and is idempotent; the behavior of all other methods after
/// close is undefined.
///
/// This class is single-threaded and not thread-safe.
@NotThreadSafe
public class OffHeapDoubleGroupByResultHolder implements GroupByResultHolder, AutoCloseable {
  private static final long BYTES_PER_VALUE = Double.BYTES;
  private static final int VALUE_SHIFT = 3;
  private static final String BUFFER_DESCRIPTION = "OffHeapDoubleGroupByResultHolder";

  private final int _maxCapacity;
  private final double _defaultValue;

  private int _resultHolderCapacity;
  private PinotDataBuffer _dataBuffer;
  // Absolute-indexed direct view of _dataBuffer for the per-row hot path (monomorphic, intrinsified
  // ByteBuffer access instead of the PinotDataBuffer wrapper); null when empty or beyond the 2GB view limit
  private ByteBuffer _view;
  private boolean _closed;

  /// Constructor for the class.
  ///
  /// @param initialCapacity Initial capacity of the result holder
  /// @param maxCapacity Maximum capacity of the result holder
  /// @param defaultValue Default value of un-initialized results
  public OffHeapDoubleGroupByResultHolder(int initialCapacity, int maxCapacity, double defaultValue) {
    _maxCapacity = maxCapacity;
    _defaultValue = defaultValue;

    _resultHolderCapacity = initialCapacity;
    _dataBuffer = OffHeapGroupByBufferPool.acquire(initialCapacity * BYTES_PER_VALUE, BUFFER_DESCRIPTION);
    _view = createView(_dataBuffer, initialCapacity);
    // Direct buffer contents are undefined, so always fill the allocated region with the default value
    fillWithDefaultValue(0, initialCapacity);
  }

  @Override
  public void ensureCapacity(int capacity) {
    Preconditions.checkArgument(capacity <= _maxCapacity);

    if (capacity > _resultHolderCapacity) {
      int copyLength = _resultHolderCapacity;
      // Cap the growth to maximum possible number of group keys. NOTE: _resultHolderCapacity (the bounds-guard
      // reference) is updated only after the new buffer is successfully acquired.
      int newCapacity = Math.min(Math.max(_resultHolderCapacity * 2, capacity), _maxCapacity);

      PinotDataBuffer current = _dataBuffer;
      _dataBuffer = OffHeapGroupByBufferPool.acquire(newCapacity * BYTES_PER_VALUE, BUFFER_DESCRIPTION);
      _view = createView(_dataBuffer, newCapacity);
      _resultHolderCapacity = newCapacity;
      if (copyLength > 0) {
        current.copyTo(0, _dataBuffer, 0, copyLength * BYTES_PER_VALUE);
      }
      // Fill the newly extended region with the default value (direct buffer contents are undefined)
      fillWithDefaultValue(copyLength, newCapacity);
      closeBuffer(current);
    }
  }

  @Override
  public double getDoubleResult(int groupKey) {
    if (groupKey == GroupKeyGenerator.INVALID_ID) {
      return _defaultValue;
    } else {
      // PinotDataBuffer access is unchecked: an out-of-range key would read arbitrary memory instead of the
      // on-heap twin's ArrayIndexOutOfBoundsException, so guard the sizing contract with an assert
      assert groupKey >= 0 && groupKey < _resultHolderCapacity : "groupKey " + groupKey + " out of bounds";
      ByteBuffer view = _view;
      if (view != null) {
        return view.getDouble(groupKey << VALUE_SHIFT);
      }
      return _dataBuffer.getDouble(groupKey * BYTES_PER_VALUE);
    }
  }

  @Override
  public int getIntResult(int groupKey) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLongResult(int groupKey) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T getResult(int groupKey) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setValueForKey(int groupKey, double newValue) {
    if (groupKey != GroupKeyGenerator.INVALID_ID) {
      // See getDoubleResult: unchecked buffer access means an out-of-range key would corrupt memory, not throw
      assert groupKey >= 0 && groupKey < _resultHolderCapacity : "groupKey " + groupKey + " out of bounds";
      ByteBuffer view = _view;
      if (view != null) {
        view.putDouble(groupKey << VALUE_SHIFT, newValue);
      } else {
        _dataBuffer.putDouble(groupKey * BYTES_PER_VALUE, newValue);
      }
    }
  }

  @Override
  public void setValueForKey(int groupKey, int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setValueForKey(int groupKey, long value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setValueForKey(int groupKey, Object newValue) {
    throw new UnsupportedOperationException();
  }

  public double getDefaultValue() {
    return _defaultValue;
  }

  @Override
  public void close() {
    if (_closed) {
      return;
    }
    _closed = true;
    closeBuffer(_dataBuffer);
    // Null the buffer and view so any use-after-close (or a second release of a pooled buffer) fails loudly with
    // an NPE instead of silently aliasing memory that the pool may have handed to another query
    _dataBuffer = null;
    _view = null;
  }

  private void fillWithDefaultValue(int fromIndex, int toIndex) {
    ByteBuffer view = _view;
    if (view != null) {
      // Intrinsified view puts are several times cheaper than the PinotDataBuffer wrapper accessors
      for (int i = fromIndex; i < toIndex; i++) {
        view.putDouble(i << VALUE_SHIFT, _defaultValue);
      }
    } else {
      for (int i = fromIndex; i < toIndex; i++) {
        _dataBuffer.putDouble(i * BYTES_PER_VALUE, _defaultValue);
      }
    }
  }

  private static ByteBuffer createView(PinotDataBuffer dataBuffer, int capacity) {
    return OffHeapGroupByUtils.createView(dataBuffer, capacity * BYTES_PER_VALUE);
  }

  private static void closeBuffer(PinotDataBuffer buffer) {
    OffHeapGroupByBufferPool.release(buffer);
  }
}
