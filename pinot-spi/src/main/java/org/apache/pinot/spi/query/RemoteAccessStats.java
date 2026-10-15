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
package org.apache.pinot.spi.query;

import java.util.concurrent.atomic.LongAdder;


/// Thread-safe accumulator for the remote accesses (local cache misses) performed to read segment data from a remote
/// storage tier, and the bytes they read. The direct remote accesses, i.e. the reads that went to the remote storage
/// tier directly instead of through the buffers fetched for the query, are counted both in the totals and on their
/// own.
///
/// The counts are read independently, so a read concurrent with a record can observe one without the others.
public final class RemoteAccessStats {
  private final LongAdder _numRemoteAccesses = new LongAdder();
  private final LongAdder _remoteAccessBytes = new LongAdder();
  private final LongAdder _numDirectRemoteAccesses = new LongAdder();
  private final LongAdder _directRemoteAccessBytes = new LongAdder();

  /// Records remote accesses and the bytes they read.
  public void addRemoteAccesses(long numRemoteAccesses, long remoteAccessBytes) {
    _numRemoteAccesses.add(numRemoteAccesses);
    _remoteAccessBytes.add(remoteAccessBytes);
  }

  /// Records direct remote accesses and the bytes they read, in the totals and on their own.
  public void addDirectRemoteAccesses(long numDirectRemoteAccesses, long directRemoteAccessBytes) {
    addRemoteAccesses(numDirectRemoteAccesses, directRemoteAccessBytes);
    _numDirectRemoteAccesses.add(numDirectRemoteAccesses);
    _directRemoteAccessBytes.add(directRemoteAccessBytes);
  }

  /// Merges all the counts of the given stats into these.
  public void merge(RemoteAccessStats remoteAccessStats) {
    addRemoteAccesses(remoteAccessStats.getNumRemoteAccesses(), remoteAccessStats.getRemoteAccessBytes());
    _numDirectRemoteAccesses.add(remoteAccessStats.getNumDirectRemoteAccesses());
    _directRemoteAccessBytes.add(remoteAccessStats.getDirectRemoteAccessBytes());
  }

  /// Returns the number of remote accesses recorded, the direct ones included.
  public long getNumRemoteAccesses() {
    return _numRemoteAccesses.sum();
  }

  /// Returns the number of bytes read by the remote accesses recorded, the direct ones included.
  public long getRemoteAccessBytes() {
    return _remoteAccessBytes.sum();
  }

  /// Returns the number of direct remote accesses recorded.
  public long getNumDirectRemoteAccesses() {
    return _numDirectRemoteAccesses.sum();
  }

  /// Returns the number of bytes read by the direct remote accesses recorded.
  public long getDirectRemoteAccessBytes() {
    return _directRemoteAccessBytes.sum();
  }
}
