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
package org.apache.pinot.common.utils.helix;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.helix.zookeeper.zkclient.exception.ZkException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs;


/**
 * Fluent builder for an atomic ZooKeeper {@code multi()} transaction over Helix property-store
 * paths. Accumulates ops via {@link #set}/{@link #create}/{@link #delete}/{@link #check} and
 * submits them as a single all-or-nothing batch on {@link #execute()}.
 *
 * <p>Paths passed to op methods are property-store-relative — i.e. the same path you would pass to
 * {@code ZkHelixPropertyStore.set(...)}, e.g. {@code /SEGMENTS/{table}/{segment}}. The builder
 * prepends the configured property-store root (e.g. {@code /{cluster}/PROPERTYSTORE}) before
 * submitting to ZK. Multi-path writes outside the property store are intentionally not supported.
 *
 * <p>On atomic rollback (e.g. version mismatch, node missing, node already exists), {@link #execute()}
 * throws the underlying {@link KeeperException} subtype ({@code BadVersionException},
 * {@code NoNodeException}, {@code NodeExistsException}, ...). Callers branch on the subtype to
 * distinguish retryable concurrent-state changes from hard errors. Per-op offender info is reachable
 * via {@link KeeperException#getResults()}.
 *
 * <p>Connectivity / session failures (timeout, interrupt, session expiry) are not atomic outcomes
 * and propagate as the original {@link ZkException}.
 *
 * <p>Single-use: each instance can be executed at most once. Obtain a fresh builder per transaction
 * (typically via {@code PinotHelixResourceManager.multiWriteZK()}).
 *
 * <p>Not thread-safe: instance state ({@code _ops}, {@code _executed}) is mutated by every fluent
 * call. A single builder must not be shared across threads; use a fresh builder per thread.
 */
public final class ZkMultiWriteBuilder {

  /** Pass as {@code expectedVersion} to skip the version check on a {@link #set}/{@link #delete}. */
  public static final int ANY_VERSION = -1;

  private final ZkClient _zkClient;
  private final String _propertyStoreRoot;
  private final List<Op> _ops = new ArrayList<>();
  private boolean _executed;

  /**
   * @param zkClient ZK client used to serialize records and submit the transaction.
   * @param propertyStoreRoot absolute ZK path that all op paths are prefixed with (typically
   *     {@code /{clusterName}/PROPERTYSTORE}). Must start with {@code /} and not end with {@code /}.
   *     Pass {@code ""} to operate on raw absolute paths (test-only).
   */
  public ZkMultiWriteBuilder(ZkClient zkClient, String propertyStoreRoot) {
    _zkClient = Preconditions.checkNotNull(zkClient, "zkClient");
    Preconditions.checkNotNull(propertyStoreRoot, "propertyStoreRoot");
    Preconditions.checkArgument(
        propertyStoreRoot.isEmpty() || (propertyStoreRoot.startsWith("/") && !propertyStoreRoot.endsWith("/")),
        "propertyStoreRoot must be empty or start with '/' and not end with '/': %s", propertyStoreRoot);
    _propertyStoreRoot = propertyStoreRoot;
  }

  /**
   * Set (overwrite) the znode at {@code path} (property-store-relative) to {@code record}, with a
   * CAS check on {@code expectedVersion}. Pass {@link #ANY_VERSION} to skip the check.
   */
  public ZkMultiWriteBuilder set(String path, ZNRecord record, int expectedVersion) {
    checkNotExecuted();
    Preconditions.checkNotNull(record, "record");
    String fullPath = resolve(path);
    _ops.add(Op.setData(fullPath, _zkClient.serialize(record, fullPath), expectedVersion));
    return this;
  }

  /** Set without a version check; equivalent to {@code set(path, record, ANY_VERSION)}. */
  public ZkMultiWriteBuilder set(String path, ZNRecord record) {
    return set(path, record, ANY_VERSION);
  }

  /**
   * Create a persistent znode at {@code path} (property-store-relative) with {@code record} as its
   * data.
   */
  public ZkMultiWriteBuilder create(String path, ZNRecord record) {
    checkNotExecuted();
    Preconditions.checkNotNull(record, "record");
    String fullPath = resolve(path);
    _ops.add(
        Op.create(fullPath, _zkClient.serialize(record, fullPath), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
    return this;
  }

  /**
   * Delete the znode at {@code path} (property-store-relative), with a CAS check on
   * {@code expectedVersion}. Pass {@link #ANY_VERSION} to skip the check.
   */
  public ZkMultiWriteBuilder delete(String path, int expectedVersion) {
    checkNotExecuted();
    _ops.add(Op.delete(resolve(path), expectedVersion));
    return this;
  }

  /** Delete without a version check; equivalent to {@code delete(path, ANY_VERSION)}. */
  public ZkMultiWriteBuilder delete(String path) {
    return delete(path, ANY_VERSION);
  }

  /**
   * Assert the version of the znode at {@code path} (property-store-relative). No mutation; gates
   * other ops in the batch atomically — the whole transaction fails with
   * {@link KeeperException.BadVersionException} if the version no longer matches.
   */
  public ZkMultiWriteBuilder check(String path, int expectedVersion) {
    checkNotExecuted();
    _ops.add(Op.check(resolve(path), expectedVersion));
    return this;
  }

  /**
   * Submit the accumulated ops as a single atomic ZK {@code multi()} transaction. Throws
   * {@link KeeperException} on atomic rollback (subtype identifies the cause). Throws
   * {@link IllegalStateException} if called more than once or if no ops have been added.
   * Connectivity / session failures propagate as the original {@link ZkException}.
   */
  public void execute()
      throws KeeperException {
    checkNotExecuted();
    _executed = true;
    Preconditions.checkState(!_ops.isEmpty(), "no ops to execute");
    try {
      _zkClient.multi(_ops);
    } catch (ZkException ze) {
      Throwable cause = ze.getCause();
      if (cause instanceof KeeperException) {
        throw (KeeperException) cause;
      }
      throw ze;
    }
  }

  private void checkNotExecuted() {
    Preconditions.checkState(!_executed, "ZkMultiWriteBuilder already executed");
  }

  private String resolve(String path) {
    Preconditions.checkNotNull(path, "path");
    Preconditions.checkArgument(path.startsWith("/"), "path must start with '/': %s", path);
    return _propertyStoreRoot + path;
  }
}
