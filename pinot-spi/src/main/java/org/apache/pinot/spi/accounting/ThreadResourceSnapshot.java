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
package org.apache.pinot.spi.accounting;

/**
 * ThreadResourceSnapshot is a utility class that helps to track the CPU time and memory allocated.
 * {@link ThreadResourceUsageProvider} provides cumulative CPU time and memory allocated for the current thread.
 * This class uses that provider to snapshot start & end values for a task executed by that thread.
 * It also implements {@link AutoCloseable} to allow usage in try-with-resources blocks,
 */
public class ThreadResourceSnapshot implements AutoCloseable {
  private long _startCpuTime;
  private long _startAllocatedBytes;

  private long _endCpuTime;
  private long _endAllocatedBytes;
  private boolean _closed = false;

  /**
   * Creates a new tracker and takes initial snapshots.
   */
  public ThreadResourceSnapshot() {
    reset();
  }

  public void reset() {
    _startCpuTime = ThreadResourceUsageProvider.getCurrentThreadCpuTime();
    _startAllocatedBytes = ThreadResourceUsageProvider.getCurrentThreadAllocatedBytes();
    _endCpuTime = _startCpuTime;
    _endAllocatedBytes = _startAllocatedBytes;
  }

  /**
   * Gets the CPU time used so far in nanoseconds.
   * Takes a current snapshot if not yet closed.
   */
  public long getCpuTimeNs() {
    updateCurrentSnapshot();
    return _endCpuTime - _startCpuTime;
  }

  /**
   * Gets the memory allocated so far in bytes.
   * Takes a current snapshot if not yet closed.
   */
  public long getAllocatedBytes() {
    updateCurrentSnapshot();
    return _endAllocatedBytes - _startAllocatedBytes;
  }

  /**
   * Updates the current snapshot if not already closed.
   */
  private void updateCurrentSnapshot() {
    if (!_closed) {
      _endCpuTime = ThreadResourceUsageProvider.getCurrentThreadCpuTime();
      _endAllocatedBytes = ThreadResourceUsageProvider.getCurrentThreadAllocatedBytes();
    }
  }

  /**
   * Takes final snapshots and marks the tracker as closed.
   * This is automatically called when used in try-with-resources.
   */
  @Override
  public void close() {
    if (!_closed) {
      updateCurrentSnapshot();
      _closed = true;
    }
  }

  /**
   * Returns true if this tracker has been closed.
   */
  public boolean isClosed() {
    return _closed;
  }

  @Override
  public String toString() {
    return "ThreadResourceSnapshot{" + "cpuTime=" + (_endCpuTime - _startCpuTime) + ", allocatedBytes="
        + (_endAllocatedBytes - _startAllocatedBytes) + ", closed=" + _closed + '}';
  }
}
