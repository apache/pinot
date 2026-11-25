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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixManager;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Operation leases: exclusive, advisory rights to run one operation, held as EPHEMERAL ZNodes in the Helix property
/// store. See [PinotOperationLease] for the guarantees.
///
/// The framework provides the lease and manages its lifecycle, nothing more. What to do when a lease is held (wait,
/// skip, defer) and how to tell that an operation is done are up to the caller.
///
/// A lease is named by a table and a key. The key is any string the caller builds; the framework never parses it. It
/// becomes a single ZNode under the table's lease directory, so it must be a valid ZNode name (see [#getLeasePath]).
/// Every method also has an overload that takes a full property store path, for a caller that keeps an existing
/// ZNode. Missing parent ZNodes are created as PERSISTENT.
///
/// ZooKeeper errors are thrown rather than swallowed, so each caller decides whether to fail open (do the work without
/// a lease) or fail closed (skip it).
public final class PinotOperationLeaseUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotOperationLeaseUtils.class);

  /// Longest key, in UTF-8 bytes, used as the ZNode name as is. A longer key is replaced by its hash, and the full key
  /// is kept in the ZNode's data.
  public static final int MAX_KEY_LENGTH_BYTES = 1024;
  /// Prefix of the ZNode name of a hashed key. Keys must not start with it, so they never collide with a hash.
  public static final String HASHED_KEY_PREFIX = "sha256-";

  static final String KEY_KEY = "key";
  static final String OWNER_KEY = "owner";
  static final String ACQUIRE_TIME_MS_KEY = "acquireTimeMs";
  private static final int MAX_RELEASE_ATTEMPTS = 3;
  private static final long BASE_RELEASE_RETRY_DELAY_MS = 100;

  private PinotOperationLeaseUtils() {
  }

  /// Returns the property store path of the lease on `key` for a table: `/OPERATION_LEASES/<table>/<key>`.
  ///
  /// @throws IllegalArgumentException if `key` is not a valid ZNode name: empty, `.`, `..`, containing `/` or a
  ///         character ZooKeeper rejects, or starting with [#HASHED_KEY_PREFIX]
  public static String getLeasePath(String tableNameWithType, String key) {
    return ZKMetadataProvider.constructPropertyStorePathForOperationLease(tableNameWithType, toZNodeName(key));
  }

  /// Tries to acquire the lease on `key` for a table without blocking.
  ///
  /// @return the lease, or `null` when another session holds it
  /// @throws IllegalArgumentException if `key` is not valid (see [#getLeasePath])
  /// @throws IllegalStateException when ZooKeeper is unreachable or the ZNode cannot be created
  @Nullable
  public static PinotOperationLease tryAcquire(HelixManager helixManager, String tableNameWithType, String key,
      String owner) {
    return doTryAcquire(helixManager, getLeasePath(tableNameWithType, key), key, owner);
  }

  /// Tries to acquire the lease at a property store `path` without blocking.
  ///
  /// @return the lease, or `null` when another session holds it
  /// @throws IllegalStateException when ZooKeeper is unreachable or the ZNode cannot be created
  @Nullable
  public static PinotOperationLease tryAcquire(HelixManager helixManager, String path, String owner) {
    return doTryAcquire(helixManager, path, null, owner);
  }

  /// Waits until nobody holds the lease on `key` for a table, using a ZooKeeper watch rather than polling.
  ///
  /// @return `true` if the lease is free, `false` if `maxWait` elapsed first
  public static boolean awaitRelease(HelixManager helixManager, String tableNameWithType, String key,
      Duration maxWait)
      throws InterruptedException {
    return awaitRelease(helixManager, getLeasePath(tableNameWithType, key), maxWait);
  }

  /// Waits until nobody holds the lease at a property store `path`, using a ZooKeeper watch rather than polling.
  ///
  /// @return `true` if the lease is free, `false` if `maxWait` elapsed first
  public static boolean awaitRelease(HelixManager helixManager, String path, Duration maxWait)
      throws InterruptedException {
    ZkHelixPropertyStore<ZNRecord> propertyStore = helixManager.getHelixPropertyStore();
    CountDownLatch released = new CountDownLatch(1);
    // Runs on the ZkClient event thread shared with Helix callbacks, so it only counts down.
    IZkDataListener listener = new IZkDataListener() {
      @Override
      public void handleDataChange(String dataPath, Object data) {
      }

      @Override
      public void handleDataDeleted(String dataPath) {
        released.countDown();
      }
    };
    propertyStore.subscribeDataChanges(path, listener);
    try {
      // Check after subscribing so a release between the caller's failed acquire and the subscribe is not missed.
      if (!propertyStore.exists(path, AccessOption.EPHEMERAL)) {
        return true;
      }
      return released.await(maxWait.toMillis(), TimeUnit.MILLISECONDS);
    } finally {
      propertyStore.unsubscribeDataChanges(path, listener);
    }
  }

  /// Returns who holds the lease on `key` for a table, or `null` when nobody does.
  @Nullable
  public static LeaseInfo getLeaseInfo(HelixManager helixManager, String tableNameWithType, String key) {
    return getLeaseInfo(helixManager, getLeasePath(tableNameWithType, key));
  }

  /// Returns who holds the lease at a property store `path`, or `null` when nobody does.
  @Nullable
  public static LeaseInfo getLeaseInfo(HelixManager helixManager, String path) {
    Stat stat = new Stat();
    ZNRecord record = helixManager.getHelixPropertyStore().get(path, stat, AccessOption.EPHEMERAL);
    if (record == null) {
      return null;
    }
    long acquireTimeMs = stat.getCtime();
    String acquireTimeMsString = record.getSimpleField(ACQUIRE_TIME_MS_KEY);
    if (acquireTimeMsString != null) {
      try {
        acquireTimeMs = Long.parseLong(acquireTimeMsString);
      } catch (NumberFormatException e) {
        LOGGER.warn("Invalid acquire time: {} in operation lease: {}, using the ZNode creation time",
            acquireTimeMsString, path);
      }
    }
    return new LeaseInfo(record.getSimpleField(KEY_KEY), record.getSimpleField(OWNER_KEY), acquireTimeMs,
        stat.getEphemeralOwner());
  }

  /// Deletes the lease on `key` for a table, whoever holds it. For operators only (see
  /// [#forceRelease(HelixManager, String)]).
  ///
  /// @return `true` if a ZNode was deleted, `false` if there was none
  public static boolean forceRelease(HelixManager helixManager, String tableNameWithType, String key) {
    return forceRelease(helixManager, getLeasePath(tableNameWithType, key));
  }

  /// Deletes the lease ZNode at `path` whoever holds it. For operators only: the holder keeps working until its next
  /// ownership check, and does not learn of the release unless its session changes.
  ///
  /// @return `true` if a ZNode was deleted, `false` if there was none
  public static boolean forceRelease(HelixManager helixManager, String path) {
    ZkHelixPropertyStore<ZNRecord> propertyStore = helixManager.getHelixPropertyStore();
    if (!propertyStore.exists(path, AccessOption.EPHEMERAL)) {
      return false;
    }
    if (!propertyStore.remove(path, AccessOption.EPHEMERAL)) {
      throw new IllegalStateException("Failed to force release operation lease: " + path);
    }
    LOGGER.info("Force released operation lease: {}", path);
    return true;
  }

  @Nullable
  private static PinotOperationLease doTryAcquire(HelixManager helixManager, String path, @Nullable String key,
      String owner) {
    if (!helixManager.isConnected()) {
      throw new IllegalStateException("Cannot acquire operation lease: " + path + ", Helix manager is not connected");
    }
    ZkHelixPropertyStore<ZNRecord> propertyStore = helixManager.getHelixPropertyStore();
    long acquireTimeMs = System.currentTimeMillis();
    ZNRecord record = new ZNRecord(owner);
    if (key != null) {
      record.setSimpleField(KEY_KEY, key);
    }
    record.setSimpleField(OWNER_KEY, owner);
    record.setSimpleField(ACQUIRE_TIME_MS_KEY, Long.toString(acquireTimeMs));

    // The property store returns false both when the ZNode exists and on other ZooKeeper errors, so tell them apart.
    if (!propertyStore.create(path, record, AccessOption.EPHEMERAL)) {
      if (propertyStore.exists(path, AccessOption.EPHEMERAL)) {
        return null;
      }
      throw new IllegalStateException("Failed to create operation lease ZNode: " + path);
    }
    // Pin the lease to the session that owns the ZNode. If our session expired right after the create, the ZNode is
    // gone or belongs to another session: report the lease as held by someone else, and never delete that ZNode.
    Stat stat = propertyStore.getStat(path, AccessOption.EPHEMERAL);
    if (stat == null || !isCurrentSession(helixManager, stat.getEphemeralOwner())) {
      LOGGER.warn("Lost operation lease: {} right after acquiring it", path);
      return null;
    }
    return new PinotOperationLease(helixManager, path, key, owner, stat.getEphemeralOwner(), acquireTimeMs);
  }

  /// Validates `key` and returns the ZNode name for it: the key itself, or its hash when longer than
  /// [#MAX_KEY_LENGTH_BYTES].
  @VisibleForTesting
  static String toZNodeName(String key) {
    Preconditions.checkArgument(!key.isEmpty() && !key.equals(".") && !key.equals(".."),
        "Invalid operation lease key: '%s'", key);
    Preconditions.checkArgument(!key.startsWith(HASHED_KEY_PREFIX), "Operation lease key must not start with: %s",
        HASHED_KEY_PREFIX);
    for (int i = 0; i < key.length(); i++) {
      char c = key.charAt(i);
      if (c == '/' || isRejectedByZooKeeper(c)) {
        throw new IllegalArgumentException(
            String.format("Operation lease key contains an invalid character: \\u%04x at index: %d", (int) c, i));
      }
    }
    if (key.getBytes(StandardCharsets.UTF_8).length <= MAX_KEY_LENGTH_BYTES) {
      return key;
    }
    return HASHED_KEY_PREFIX + DigestUtils.sha256Hex(key);
  }

  /// Characters ZooKeeper rejects in a path (see `org.apache.zookeeper.common.PathUtils#validatePath`). Surrogates are
  /// among them, so characters outside the Basic Multilingual Plane are rejected too.
  private static boolean isRejectedByZooKeeper(char c) {
    return c <= '\u001f' || (c >= '\u007f' && c <= '\u009f') || (c >= '\ud800' && c <= '') || c >= '￰';
  }

  /// Deletes the lease ZNode only if the given session still owns it.
  ///
  /// A small race remains: if the session expires between the ownership check and the delete, and another session
  /// acquires the lease in that gap, the retried delete removes the new holder's ZNode. ZooKeeper offers no
  /// conditional delete on the ephemeral owner, and every new ZNode starts at version 0, so a version check cannot
  /// tell the two apart.
  static boolean releaseIfOwner(HelixManager helixManager, String path, long sessionId) {
    ZkHelixPropertyStore<ZNRecord> propertyStore = helixManager.getHelixPropertyStore();
    for (int attempt = 1; attempt <= MAX_RELEASE_ATTEMPTS; attempt++) {
      try {
        Stat stat = propertyStore.getStat(path, AccessOption.EPHEMERAL);
        if (stat == null) {
          return true;
        }
        if (stat.getEphemeralOwner() != sessionId) {
          LOGGER.warn("Operation lease: {} is now held by session: 0x{}, not ours: 0x{}; leaving it", path,
              Long.toHexString(stat.getEphemeralOwner()), Long.toHexString(sessionId));
          return true;
        }
        if (!isCurrentSession(helixManager, sessionId)) {
          // Our session ended, so ZooKeeper drops the ZNode on its own.
          return true;
        }
        if (propertyStore.remove(path, AccessOption.EPHEMERAL)) {
          return true;
        }
        LOGGER.warn("Failed to release operation lease: {} on attempt: {}/{}", path, attempt, MAX_RELEASE_ATTEMPTS);
      } catch (Exception e) {
        LOGGER.warn("Caught exception releasing operation lease: {} on attempt: {}/{}", path, attempt,
            MAX_RELEASE_ATTEMPTS, e);
      }
      if (attempt < MAX_RELEASE_ATTEMPTS) {
        try {
          Thread.sleep(BASE_RELEASE_RETRY_DELAY_MS << (attempt - 1));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return false;
        }
      }
    }
    LOGGER.error("Failed to release operation lease: {} after {} attempts", path, MAX_RELEASE_ATTEMPTS);
    return false;
  }

  /// Returns whether `sessionId` is the Helix manager's current, connected ZooKeeper session. Never blocks:
  /// [HelixManager#getSessionId()] waits for a connection when disconnected, so connectivity is checked first.
  static boolean isCurrentSession(HelixManager helixManager, long sessionId) {
    if (!helixManager.isConnected()) {
      return false;
    }
    try {
      // Helix keeps the session id as a hex string.
      return Long.parseUnsignedLong(helixManager.getSessionId(), 16) == sessionId;
    } catch (Exception e) {
      // Disconnected between the two calls.
      return false;
    }
  }

  /// Holder of a lease as read from ZooKeeper.
  public static final class LeaseInfo {
    @Nullable
    private final String _key;
    @Nullable
    private final String _owner;
    private final long _acquireTimeMs;
    private final long _sessionId;

    public LeaseInfo(@Nullable String key, @Nullable String owner, long acquireTimeMs, long sessionId) {
      _key = key;
      _owner = owner;
      _acquireTimeMs = acquireTimeMs;
      _sessionId = sessionId;
    }

    /// The full key, or `null` for a lease acquired by path.
    @Nullable
    public String getKey() {
      return _key;
    }

    @Nullable
    public String getOwner() {
      return _owner;
    }

    public long getAcquireTimeMs() {
      return _acquireTimeMs;
    }

    public long getSessionId() {
      return _sessionId;
    }

    @Override
    public String toString() {
      return "LeaseInfo{key='" + _key + "', owner='" + _owner + "', acquireTimeMs=" + _acquireTimeMs
          + ", sessionId=0x" + Long.toHexString(_sessionId) + "}";
    }
  }
}
