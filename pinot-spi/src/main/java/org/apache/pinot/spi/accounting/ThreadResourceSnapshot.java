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
 * This class uses that provider to snapshot start for a task executed by that thread.
 */
public class ThreadResourceSnapshot {
  private long _startCpuTime;
  private long _startAllocatedBytes;

  /**
   * Creates a new tracker and takes initial snapshots.
   */
  public ThreadResourceSnapshot() {
    reset();
  }

  public void reset() {
    _startCpuTime = ThreadResourceUsageProvider.getCurrentThreadCpuTime();
    _startAllocatedBytes = ThreadResourceUsageProvider.getCurrentThreadAllocatedBytes();
  }

  /**
   * Gets the CPU time used so far in nanoseconds.
   * This is the difference between the current CPU time and the start CPU time.
   */
  public long getCpuTimeNs() {
    return ThreadResourceUsageProvider.getCurrentThreadCpuTime() - _startCpuTime;
  }

  /**
   * Gets the memory allocated so far in bytes.
   * This is the difference between the current allocated bytes and the start allocated bytes.
   */
  public long getAllocatedBytes() {
    return ThreadResourceUsageProvider.getCurrentThreadAllocatedBytes() - _startAllocatedBytes;
  }

  @Override
  public String toString() {
    return "ThreadResourceSnapshot{" + "cpuTime=" + (getCpuTimeNs()) + ", allocatedBytes="
        + (getAllocatedBytes()) + '}';
  }
}
