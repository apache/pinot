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
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.zkclient.exception.ZkException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs;


/**
 * Fluent builder for an atomic ZooKeeper {@code multi()} transaction. Accumulates ops via
 * {@link #set}/{@link #create}/{@link #delete}/{@link #check} and submits them as a single
 * all-or-nothing batch on {@link #execute()}.
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
 */
public final class ZkMultiWriteBuilder {

  /** Pass as {@code expectedVersion} to skip the version check on a {@link #set}/{@link #delete}. */
  public static final int ANY_VERSION = -1;

  // ZNRecordSerializer is documented thread-safe by Helix (no mutable state beyond per-call buffers),
  // so a single static instance is fine across concurrent builders.
  private static final ZNRecordSerializer SERIALIZER = new ZNRecordSerializer();

  private final RealmAwareZkClient _zkClient;
  private final List<Op> _ops = new ArrayList<>();
  private boolean _executed;

  public ZkMultiWriteBuilder(RealmAwareZkClient zkClient) {
    _zkClient = Preconditions.checkNotNull(zkClient, "zkClient");
  }

  /**
   * Set (overwrite) the znode at {@code path} to {@code record}, with a CAS check on
   * {@code expectedVersion}. Pass {@link #ANY_VERSION} to skip the check.
   */
  public ZkMultiWriteBuilder set(String path, ZNRecord record, int expectedVersion) {
    checkNotExecuted();
    Preconditions.checkNotNull(path, "path");
    Preconditions.checkNotNull(record, "record");
    _ops.add(Op.setData(path, SERIALIZER.serialize(record), expectedVersion));
    return this;
  }

  /** Set without a version check; equivalent to {@code set(path, record, ANY_VERSION)}. */
  public ZkMultiWriteBuilder set(String path, ZNRecord record) {
    return set(path, record, ANY_VERSION);
  }

  /** Create a persistent znode at {@code path} with {@code record} as its data. */
  public ZkMultiWriteBuilder create(String path, ZNRecord record) {
    checkNotExecuted();
    Preconditions.checkNotNull(path, "path");
    Preconditions.checkNotNull(record, "record");
    _ops.add(Op.create(path, SERIALIZER.serialize(record), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
    return this;
  }

  /**
   * Delete the znode at {@code path}, with a CAS check on {@code expectedVersion}. Pass
   * {@link #ANY_VERSION} to skip the check.
   */
  public ZkMultiWriteBuilder delete(String path, int expectedVersion) {
    checkNotExecuted();
    Preconditions.checkNotNull(path, "path");
    _ops.add(Op.delete(path, expectedVersion));
    return this;
  }

  /** Delete without a version check; equivalent to {@code delete(path, ANY_VERSION)}. */
  public ZkMultiWriteBuilder delete(String path) {
    return delete(path, ANY_VERSION);
  }

  /**
   * Assert the version of the znode at {@code path}. No mutation; gates other ops in the batch
   * atomically — the whole transaction fails with {@link KeeperException.BadVersionException} if the
   * version no longer matches.
   */
  public ZkMultiWriteBuilder check(String path, int expectedVersion) {
    checkNotExecuted();
    Preconditions.checkNotNull(path, "path");
    _ops.add(Op.check(path, expectedVersion));
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
}
