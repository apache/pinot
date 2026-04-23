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

import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;


/**
 * Outcome of a transactional ZK multi-write submitted through
 * {@link ZkMultiWriter#multi(org.apache.helix.zookeeper.impl.client.ZkClient, java.util.List)}.
 * <p>Immutable. ZK multi is all-or-nothing: either every op committed (success) or none of them
 * did (atomic rollback). On rollback, {@link #getFailedOpIndex()} identifies the op that caused
 * the rejection and {@link #getFailureCode()} is its ZK error code. Non-atomic failures
 * (connectivity / session loss) are signaled by a thrown exception, not this result.
 */
public final class PinotZkMultiResult {

  private final boolean _success;
  private final List<OpOutcome> _outcomes;
  private final int _failedOpIndex;
  @Nullable
  private final KeeperException.Code _failureCode;

  private PinotZkMultiResult(boolean success, List<OpOutcome> outcomes, int failedOpIndex,
      @Nullable KeeperException.Code failureCode) {
    _success = success;
    _outcomes = Collections.unmodifiableList(outcomes);
    _failedOpIndex = failedOpIndex;
    _failureCode = failureCode;
  }

  static PinotZkMultiResult success(List<OpOutcome> outcomes) {
    return new PinotZkMultiResult(true, outcomes, -1, null);
  }

  static PinotZkMultiResult failure(List<OpOutcome> outcomes, int failedOpIndex, KeeperException.Code code) {
    return new PinotZkMultiResult(false, outcomes, failedOpIndex, code);
  }

  public boolean isSuccess() {
    return _success;
  }

  /** One entry per submitted op, in submission order. */
  public List<OpOutcome> getOutcomes() {
    return _outcomes;
  }

  /** Index into the submitted op list that caused the rollback, or -1 on success. */
  public int getFailedOpIndex() {
    return _failedOpIndex;
  }

  /** ZK error code that triggered the rollback, or {@code null} on success. */
  @Nullable
  public KeeperException.Code getFailureCode() {
    return _failureCode;
  }

  @Override
  public String toString() {
    if (_success) {
      return "PinotZkMultiResult{success ops=" + _outcomes.size() + "}";
    }
    return "PinotZkMultiResult{rollback failedOpIndex=" + _failedOpIndex + " code=" + _failureCode + "}";
  }

  /**
   * Per-op result. On success, {@link #getStat()} carries the post-commit {@link Stat} for
   * create/set ops (null for delete/check). On atomic rollback, {@link #getCode()} is non-null
   * on the offending op and may be {@link KeeperException.Code#RUNTIMEINCONSISTENCY} or
   * {@link KeeperException.Code#OK} on the other ops (per ZK multi semantics).
   */
  public static final class OpOutcome {
    private final String _path;
    @Nullable
    private final Stat _stat;
    @Nullable
    private final KeeperException.Code _code;

    OpOutcome(String path, @Nullable Stat stat, @Nullable KeeperException.Code code) {
      _path = path;
      _stat = stat;
      _code = code;
    }

    public String getPath() {
      return _path;
    }

    @Nullable
    public Stat getStat() {
      return _stat;
    }

    @Nullable
    public KeeperException.Code getCode() {
      return _code;
    }

    @Override
    public String toString() {
      return "OpOutcome{path=" + _path + " stat=" + _stat + " code=" + _code + "}";
    }
  }
}
