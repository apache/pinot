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
package org.apache.pinot.common.lease;

import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import org.apache.helix.HelixManager;


/// A held lease on one operation, backed by an EPHEMERAL ZNode in the Helix property store.
///
/// The ZNode lives as long as the ZooKeeper session that created it, which is the session of the [HelixManager] the
/// lease was acquired through. The lease counts as held only while that same session is still the manager's current
/// session, so a session expiry ends the lease on both sides: ZooKeeper deletes the ZNode, and [#isHeld()] turns false.
///
/// The lease is advisory. It saves duplicate work but cannot fence writes made by a holder that stalls past its session
/// timeout; operations that must never publish a mixed result need their own commit step.
///
/// Instances are created by [PinotOperationLeaseUtils#tryAcquire]. This class is thread-safe. [#release()] is
/// idempotent and never deletes a ZNode created by another session.
public final class PinotOperationLease implements AutoCloseable {
  private final HelixManager _helixManager;
  private final String _path;
  @Nullable
  private final String _key;
  private final String _owner;
  private final long _sessionId;
  private final long _acquireTimeMs;
  private final AtomicBoolean _released = new AtomicBoolean();

  PinotOperationLease(HelixManager helixManager, String path, @Nullable String key, String owner, long sessionId,
      long acquireTimeMs) {
    _helixManager = helixManager;
    _path = path;
    _key = key;
    _owner = owner;
    _sessionId = sessionId;
    _acquireTimeMs = acquireTimeMs;
  }

  /// Returns whether this lease is still held. Local check: makes no ZooKeeper request and never blocks.
  public boolean isHeld() {
    return !_released.get() && PinotOperationLeaseUtils.isCurrentSession(_helixManager, _sessionId);
  }

  /// Throws if this lease is no longer held. Call it between the steps of the leased operation.
  public void checkOwnership()
      throws LeaseLostException {
    if (!isHeld()) {
      throw new LeaseLostException(_path, _owner, _sessionId);
    }
  }

  /// Releases the lease. Idempotent. Returns `true` when the ZNode is gone or belongs to another session afterwards.
  public boolean release() {
    if (!_released.compareAndSet(false, true)) {
      return true;
    }
    return PinotOperationLeaseUtils.releaseIfOwner(_helixManager, _path, _sessionId);
  }

  @Override
  public void close() {
    release();
  }

  /// Property store path of the lease ZNode.
  public String getPath() {
    return _path;
  }

  /// Key the lease was acquired on, or `null` when it was acquired by path.
  @Nullable
  public String getKey() {
    return _key;
  }

  /// Instance id of the holder.
  public String getOwner() {
    return _owner;
  }

  /// ZooKeeper session that owns the lease ZNode.
  public long getSessionId() {
    return _sessionId;
  }

  public long getAcquireTimeMs() {
    return _acquireTimeMs;
  }

  public long getAgeMs() {
    return System.currentTimeMillis() - _acquireTimeMs;
  }

  @Override
  public String toString() {
    return "PinotOperationLease{path='" + _path + "', key='" + _key + "', owner='" + _owner + "', sessionId=0x"
        + Long.toHexString(_sessionId) + ", ageMs=" + getAgeMs() + ", released=" + _released.get() + "}";
  }
}
