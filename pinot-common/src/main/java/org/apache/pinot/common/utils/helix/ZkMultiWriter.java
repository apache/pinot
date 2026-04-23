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
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.zkclient.exception.ZkException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;


/**
 * Stateless helper that submits a list of {@link PinotZkOp}s as a ZooKeeper atomic {@code multi()}
 * transaction. The {@link RealmAwareZkClient} is owned by the caller; this class only
 * serializes ops and translates the result or exception into a {@link PinotZkMultiResult}.
 * <p>Atomic rollback (e.g. {@code BADVERSION}, {@code NONODE}, {@code NODEEXISTS}) is returned as a
 * failed {@link PinotZkMultiResult} with the offending op index and ZK error code. Connectivity
 * and session failures ({@code ZkTimeoutException}, {@code ZkInterruptedException}, etc.) are
 * allowed to propagate.
 * <p>Typed against the {@code RealmAwareZkClient} interface (not the concrete
 * {@code org.apache.helix.zookeeper.zkclient.ZkClient}) so callers can pass in either a dedicated
 * client or the Helix-managed one if/when that becomes reachable.
 */
public final class ZkMultiWriter {

  // ZNRecordSerializer is documented thread-safe by Helix (no mutable state beyond reusable buffers
  // inside serialize/deserialize), so a single static instance is fine across concurrent callers.
  private static final ZNRecordSerializer SERIALIZER = new ZNRecordSerializer();

  private ZkMultiWriter() {
  }

  /**
   * Submit {@code ops} as a single atomic ZK {@code multi()} transaction against {@code zkClient}.
   * Returns a {@link PinotZkMultiResult} describing per-op outcomes. Throws only on non-atomic
   * failures (connection loss, session expiry, interrupt, timeout).
   */
  public static PinotZkMultiResult multi(RealmAwareZkClient zkClient, List<PinotZkOp> ops) {
    Preconditions.checkNotNull(zkClient, "zkClient");
    Preconditions.checkNotNull(ops, "ops");
    Preconditions.checkArgument(!ops.isEmpty(), "ops must not be empty");

    List<Op> zkOps = new ArrayList<>(ops.size());
    for (int i = 0; i < ops.size(); i++) {
      PinotZkOp op = ops.get(i);
      Preconditions.checkNotNull(op, "ops[%s] is null", i);
      zkOps.add(op.toZkOp(SERIALIZER));
    }

    try {
      List<OpResult> results = zkClient.multi(zkOps);
      return buildSuccess(ops, results);
    } catch (ZkException ze) {
      Throwable cause = ze.getCause();
      if (cause instanceof KeeperException) {
        return buildFailure(ops, (KeeperException) cause);
      }
      // Non-atomic failure (timeout, interrupt, etc.) or unknown wrapping — propagate.
      throw ze;
    }
  }

  private static PinotZkMultiResult buildSuccess(List<PinotZkOp> ops, List<OpResult> results) {
    List<PinotZkMultiResult.OpOutcome> outcomes = new ArrayList<>(ops.size());
    for (int i = 0; i < ops.size(); i++) {
      PinotZkOp op = ops.get(i);
      OpResult r = results.get(i);
      outcomes.add(new PinotZkMultiResult.OpOutcome(op.getPath(), statFromOpResult(r), null));
    }
    return PinotZkMultiResult.success(outcomes);
  }

  private static PinotZkMultiResult buildFailure(List<PinotZkOp> ops, KeeperException ke) {
    List<OpResult> results = ke.getResults();
    List<PinotZkMultiResult.OpOutcome> outcomes = new ArrayList<>(ops.size());
    int failedIndex = -1;
    KeeperException.Code failedCode = ke.code();

    if (results == null) {
      // Multi failed before per-op results were populated — treat offender as unlocalizable.
      for (PinotZkOp op : ops) {
        outcomes.add(new PinotZkMultiResult.OpOutcome(op.getPath(), null, failedCode));
      }
      return PinotZkMultiResult.failure(outcomes, -1, failedCode);
    }

    for (int i = 0; i < ops.size(); i++) {
      PinotZkOp op = ops.get(i);
      OpResult r = i < results.size() ? results.get(i) : null;
      KeeperException.Code code = null;
      if (r instanceof OpResult.ErrorResult) {
        int err = ((OpResult.ErrorResult) r).getErr();
        code = KeeperException.Code.get(err);
        // The first op whose code is not OK / not the runtime-rollback marker is the offender.
        if (failedIndex == -1 && code != KeeperException.Code.OK
            && code != KeeperException.Code.RUNTIMEINCONSISTENCY) {
          failedIndex = i;
          failedCode = code;
        }
      }
      outcomes.add(new PinotZkMultiResult.OpOutcome(op.getPath(), statFromOpResult(r), code));
    }

    // failedIndex == -1 here means we couldn't localize the offender from per-op results;
    // caller sees the top-level code with index -1 ("unknown op").
    return PinotZkMultiResult.failure(outcomes, failedIndex, failedCode);
  }

  private static org.apache.zookeeper.data.Stat statFromOpResult(OpResult r) {
    if (r instanceof OpResult.SetDataResult) {
      return ((OpResult.SetDataResult) r).getStat();
    }
    if (r instanceof OpResult.CreateResult) {
      return ((OpResult.CreateResult) r).getStat();
    }
    return null;
  }
}
