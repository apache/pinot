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
import javax.annotation.Nullable;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs;


/**
 * One operation inside a transactional ZK multi-write submitted through
 * {@link ZkMultiWriter#multi(org.apache.helix.zookeeper.impl.client.ZkClient, java.util.List)}.
 * <p>Supported kinds mirror ZooKeeper's {@code multi()} primitives:
 * <ul>
 *   <li>{@link #set} &mdash; update an existing znode, optionally version-checked (CAS).</li>
 *   <li>{@link #create} &mdash; create a new persistent znode with a {@link ZNRecord} payload.</li>
 *   <li>{@link #delete} &mdash; delete a znode, optionally version-checked.</li>
 *   <li>{@link #check} &mdash; assert a znode's version without mutating; gates other ops in the same batch.</li>
 * </ul>
 * <p>Instances are immutable. Payload bytes are lazily produced via {@link #toZkOp(ZNRecordSerializer)}
 * when the batch is submitted.
 * <p>Not thread-safe for concurrent mutation (irrelevant &mdash; instances are immutable).
 */
public final class PinotZkOp {

  /** Pass as {@code expectedVersion} to skip the version check. */
  public static final int ANY_VERSION = -1;

  enum Kind {
    SET, CREATE, DELETE, CHECK
  }

  private final Kind _kind;
  private final String _path;
  @Nullable
  private final ZNRecord _record;
  private final int _expectedVersion;

  private PinotZkOp(Kind kind, String path, @Nullable ZNRecord record, int expectedVersion) {
    _kind = kind;
    _path = path;
    _record = record;
    _expectedVersion = expectedVersion;
  }

  /**
   * Set (overwrite) the znode at {@code path} to {@code record}. Pass {@link #ANY_VERSION} for
   * {@code expectedVersion} to skip the CAS check.
   */
  public static PinotZkOp set(String path, ZNRecord record, int expectedVersion) {
    Preconditions.checkNotNull(path, "path");
    Preconditions.checkNotNull(record, "record");
    return new PinotZkOp(Kind.SET, path, record, expectedVersion);
  }

  /** Create a persistent znode at {@code path} with {@code record} as its data. */
  public static PinotZkOp create(String path, ZNRecord record) {
    Preconditions.checkNotNull(path, "path");
    Preconditions.checkNotNull(record, "record");
    return new PinotZkOp(Kind.CREATE, path, record, ANY_VERSION);
  }

  /** Delete the znode at {@code path}. Pass {@link #ANY_VERSION} to skip the version check. */
  public static PinotZkOp delete(String path, int expectedVersion) {
    Preconditions.checkNotNull(path, "path");
    return new PinotZkOp(Kind.DELETE, path, null, expectedVersion);
  }

  /**
   * Assert the version of the znode at {@code path}. No mutation; used to gate other ops in the
   * batch atomically &mdash; the whole {@code multi()} fails if the version no longer matches.
   */
  public static PinotZkOp check(String path, int expectedVersion) {
    Preconditions.checkNotNull(path, "path");
    return new PinotZkOp(Kind.CHECK, path, null, expectedVersion);
  }

  public Kind getKind() {
    return _kind;
  }

  public String getPath() {
    return _path;
  }

  public int getExpectedVersion() {
    return _expectedVersion;
  }

  Op toZkOp(ZNRecordSerializer serializer) {
    switch (_kind) {
      case SET:
        return Op.setData(_path, serializer.serialize(_record), _expectedVersion);
      case CREATE:
        return Op.create(_path, serializer.serialize(_record), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      case DELETE:
        return Op.delete(_path, _expectedVersion);
      case CHECK:
        return Op.check(_path, _expectedVersion);
      default:
        throw new IllegalStateException("Unknown PinotZkOp kind: " + _kind);
    }
  }

  @Override
  public String toString() {
    return "PinotZkOp{" + _kind + " path=" + _path + " expectedVersion=" + _expectedVersion + "}";
  }
}
