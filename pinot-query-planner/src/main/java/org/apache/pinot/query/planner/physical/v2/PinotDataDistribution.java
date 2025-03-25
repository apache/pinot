package org.apache.pinot.query.planner.physical.v2;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;


public class PinotDataDistribution {
  /**
   * Denotes the type of distribution: broadcast, singleton, etc.
   */
  private final RelDistribution.Type _type;
  /**
   * In the format: "index@instanceId".
   * <p>
   *   <b>TODO:</b> An alternative is to store workers separately. One reason workers are needed is because
   *     Exchange if often required because the workers for RelNode with multiple inputs may be different. With
   *     that kind of a design, we can just store the number of streams here and store workers separately.
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
   */
  public boolean satisfies(@Nullable RelDistribution distributionConstraint) {
    if (distributionConstraint == null) {
      return true;
    }
    RelDistribution.Type constraintType = distributionConstraint.getType();
    if (constraintType == RelDistribution.Type.ANY) {
      return true;
    } else if (constraintType == RelDistribution.Type.BROADCAST_DISTRIBUTED) {
      // TODO: We could do better when the input node is only on a single worker.
      return _type == RelDistribution.Type.BROADCAST_DISTRIBUTED;
    } else if (constraintType == RelDistribution.Type.SINGLETON) {
      return _type == RelDistribution.Type.SINGLETON;
    } else if (constraintType == RelDistribution.Type.RANDOM_DISTRIBUTED) {
      return _type == RelDistribution.Type.RANDOM_DISTRIBUTED;
    }
    if (constraintType != RelDistribution.Type.HASH_DISTRIBUTED) {
      throw new IllegalStateException("Unexpected distribution constraint type: " + distributionConstraint.getType());
    }
    if (_type != RelDistribution.Type.HASH_DISTRIBUTED) {
      return false;
    }
    HashDistributionDesc hashDistributionDesc = satisfiesHashDistributionConstraint(distributionConstraint);
    return hashDistributionDesc != null;
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

  public PinotDataDistribution apply(@Nullable Map<Integer, List<Integer>> mapping) {
    if (mapping == null) {
      return new PinotDataDistribution(RelDistribution.Type.ANY, _workers, _workerHash, null, null);
    }
    Set<HashDistributionDesc> newHashDesc = new HashSet<>();
    if (_hashDistributionDesc != null) {
      for (HashDistributionDesc desc : _hashDistributionDesc) {
        Set<HashDistributionDesc> newDescs = desc.apply(mapping);
        if (newDescs != null) {
          newHashDesc.addAll(newDescs);
        }
      }
    }
    RelDistribution.Type newType = _type;
    if (newType == RelDistribution.Type.HASH_DISTRIBUTED && newHashDesc.isEmpty()) {
      newType = RelDistribution.Type.ANY;
    }
    // TODO: Preserve collation too.
    RelCollation newCollation = RelCollations.EMPTY;
    return new PinotDataDistribution(newType, _workers, _workerHash, newHashDesc, newCollation);
  }

  @Nullable
  public HashDistributionDesc satisfiesHashDistributionConstraint(RelDistribution hashConstraint) {
    Preconditions.checkNotNull(_hashDistributionDesc, "Found hashDistributionDesc null in satisfies");
    // TODO: once we switch away from RelDistribution, we can also support partial constraints.
    // Return any hash distribution descriptor that matches the given constraint.
    return _hashDistributionDesc.stream().filter(x -> x.getKeyIndexes().equals(hashConstraint.getKeys())).findFirst()
        .orElse(null);
  }

  private void validate() {
    if (_type == RelDistribution.Type.SINGLETON && _workers.size() > 1) {
      throw new IllegalStateException("Singleton distribution with multiple workers");
    }
    if (_type != RelDistribution.Type.HASH_DISTRIBUTED && !_hashDistributionDesc.isEmpty()) {
      throw new IllegalStateException("Hash distribution desc with non-hash distribution");
    }
  }
}
