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
package org.apache.pinot.query.planner.physical.v2;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.query.planner.physical.v2.mapping.PinotDistMapping;


/**
 * Describes how the data will be distributed across a distributed set of output streams for a given Plan Node.
 * They integer based keys, such as those in {@link HashDistributionDesc} are based on the output Row Type of the
 * corresponding plan node.
 */
public class PinotDataDistribution {
  /**
   * Denotes the type of distribution: broadcast, singleton, etc.
   */
  private final RelDistribution.Type _type;
  /**
   * In the format: "index@instanceId".
   * <p>
   *   <b>TODO:</b> An alternative is to store workers separately. One reason workers are needed is because
   *     Exchange is often required because the workers for RelNode with multiple inputs may be different.
   *     If we store workers separately, then we can just store the number of streams here. But that means then that
   *     we have to handle Exchanges for scenarios where workers are different in sender/receiver separately from
   *     Exchanges added due to the other reason.
   * </p>
   */
  private final List<String> _workers;
  /**
   * Precomputed hashCode of workers to allow quick comparisons. In large deployments, it is common to have 30-100+
   * servers, and if the plan is big enough, comparison of workers can become a bottleneck.
   */
  private final long _workerHash;
  /**
   * The set of hash distribution descriptors. This is a set, because a given stream can be partitioned by multiple
   * different sets of keys.
   */
  private final Set<HashDistributionDesc> _hashDistributionDesc;
  /**
   * Denotes the ordering of data in each output stream of data.
   */
  private final RelCollation _collation;

  public PinotDataDistribution(RelDistribution.Type type, List<String> workers, long workerHash,
      @Nullable Set<HashDistributionDesc> desc, @Nullable RelCollation collation) {
    _type = type;
    _workers = workers;
    _workerHash = workerHash;
    _hashDistributionDesc = desc == null ? Collections.emptySet() : desc;
    _collation = collation == null ? RelCollations.EMPTY : collation;
    validate();
  }

  public static PinotDataDistribution singleton(String worker, @Nullable RelCollation collation) {
    List<String> workers = ImmutableList.of(worker);
    return new PinotDataDistribution(RelDistribution.Type.SINGLETON, workers, workers.hashCode(), null,
        collation);
  }

  public PinotDataDistribution withCollation(RelCollation collation) {
    return new PinotDataDistribution(_type, _workers, _workerHash, _hashDistributionDesc, collation);
  }

  public RelDistribution.Type getType() {
    return _type;
  }

  public List<String> getWorkers() {
    return _workers;
  }

  public long getWorkerHash() {
    return _workerHash;
  }

  public Set<HashDistributionDesc> getHashDistributionDesc() {
    return _hashDistributionDesc;
  }

  public RelCollation getCollation() {
    return _collation;
  }

  /**
   * Given a distribution constraint, return whether this physical distribution meets the constraint or not.
   * E.g. say the distribution constraint is Broadcast. That means each stream in the output of this Plan Node should
   * contain all the records. This method will return true if that is already the case.
   */
  public boolean satisfies(@Nullable RelDistribution distributionConstraint) {
    if (distributionConstraint == null || _workers.size() == 1) {
      return true;
    }
    RelDistribution.Type constraintType = distributionConstraint.getType();
    switch (constraintType) {
      case ANY:
      case RANDOM_DISTRIBUTED:
        return true;
      case BROADCAST_DISTRIBUTED:
        return _type == RelDistribution.Type.BROADCAST_DISTRIBUTED;
      case SINGLETON:
        return _type == RelDistribution.Type.SINGLETON;
      case HASH_DISTRIBUTED:
        if (_type != RelDistribution.Type.HASH_DISTRIBUTED) {
          return false;
        }
        return satisfiesHashDistributionDesc(distributionConstraint.getKeys()) != null;
      default:
        throw new IllegalStateException("Unexpected distribution constraint type: " + distributionConstraint.getType());
    }
  }

  /**
   * Returns a Hash Distribution Desc
   */
  @Nullable
  public HashDistributionDesc satisfiesHashDistributionDesc(List<Integer> keys) {
    Preconditions.checkNotNull(_hashDistributionDesc, "null hashDistributionDesc in satisfies");
    // Return any hash distribution descriptor that matches the given constraint *exactly*.
    // TODO: Add support for partial check (i.e. if distributed by [1], then we can avoid re-dist for constraint [1, 2].
    return _hashDistributionDesc.stream().filter(x -> x.getKeys().equals(keys)).findFirst().orElse(null);
  }

  public boolean satisfies(@Nullable RelCollation relCollation) {
    if (relCollation == null || relCollation == RelCollations.EMPTY || relCollation.getKeys().isEmpty()) {
      return true;
    }
    if (_collation == null) {
      return false;
    }
    return _collation.satisfies(relCollation);
  }

  public PinotDataDistribution apply(@Nullable PinotDistMapping mapping) {
    if (mapping == null) {
      return new PinotDataDistribution(RelDistribution.Type.ANY, _workers, _workerHash, null, null);
    }
    Set<HashDistributionDesc> newHashDesc = new HashSet<>();
    for (HashDistributionDesc desc : _hashDistributionDesc) {
      Set<HashDistributionDesc> newDescs = desc.apply(mapping);
      newHashDesc.addAll(newDescs);
    }
    RelDistribution.Type newType = _type;
    if (newType == RelDistribution.Type.HASH_DISTRIBUTED && newHashDesc.isEmpty()) {
      newType = RelDistribution.Type.ANY;
    }
    // TODO: Preserve collation too.
    RelCollation newCollation = RelCollations.EMPTY;
    return new PinotDataDistribution(newType, _workers, _workerHash, newHashDesc, newCollation);
  }

  private void validate() {
    if (_type != RelDistribution.Type.SINGLETON && _workers.size() == 1) {
      throw new IllegalStateException("Single worker but non singleton distribution");
    }
    if (_type == RelDistribution.Type.SINGLETON && _workers.size() > 1) {
      throw new IllegalStateException("Singleton distribution with multiple workers");
    }
    if (_type != RelDistribution.Type.HASH_DISTRIBUTED && !_hashDistributionDesc.isEmpty()) {
      throw new IllegalStateException("Hash distribution desc with non-hash distribution");
    }
  }
}
