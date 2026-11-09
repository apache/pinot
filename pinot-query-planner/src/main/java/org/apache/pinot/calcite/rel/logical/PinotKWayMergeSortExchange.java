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
package org.apache.pinot.calcite.rel.logical;

import javax.annotation.Nullable;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.SortExchange;


/// Exchanges already sorted streams and preserves their collation with a k-way merge at the receiver.
/// The logical plan must establish the input ordering. This immutable node does not sort its input.
public class PinotKWayMergeSortExchange extends SortExchange {
  @Nullable
  private final Boolean _prePartitioned;
  private final int _fetch;
  private final int _offset;

  private PinotKWayMergeSortExchange(RelOptCluster cluster, RelTraitSet traits, RelNode input,
      RelDistribution distribution, RelCollation collation, @Nullable Boolean prePartitioned, int fetch, int offset) {
    super(cluster, traits, input, distribution, collation);
    _prePartitioned = prePartitioned;
    _fetch = fetch;
    _offset = offset;
  }

  public static PinotKWayMergeSortExchange create(RelNode input, RelDistribution distribution, RelCollation collation) {
    return create(input, distribution, collation, null, -1, -1);
  }

  public static PinotKWayMergeSortExchange create(RelNode input, RelDistribution distribution, RelCollation collation,
      @Nullable Boolean prePartitioned) {
    return create(input, distribution, collation, prePartitioned, -1, -1);
  }

  public static PinotKWayMergeSortExchange create(RelNode input, RelDistribution distribution, RelCollation collation,
      @Nullable Boolean prePartitioned, int fetch, int offset) {
    collation = RelCollationTraitDef.INSTANCE.canonize(collation);
    distribution = RelDistributionTraitDef.INSTANCE.canonize(distribution);
    return new PinotKWayMergeSortExchange(input.getCluster(),
        input.getTraitSet().replace(Convention.NONE).replace(distribution).replace(collation), input, distribution,
        collation, prePartitioned, fetch, offset);
  }

  @Nullable
  public Boolean getPrePartitioned() {
    return _prePartitioned;
  }

  public int getFetch() {
    return _fetch;
  }

  public int getOffset() {
    return _offset;
  }

  @Override
  public RelWriter explainTerms(RelWriter writer) {
    return super.explainTerms(writer).itemIf("fetch", _fetch, _fetch >= 0).itemIf("offset", _offset, _offset > 0);
  }

  @Override
  public SortExchange copy(RelTraitSet traits, RelNode input, RelDistribution distribution, RelCollation collation) {
    return new PinotKWayMergeSortExchange(getCluster(), traits, input, distribution, collation, _prePartitioned, _fetch,
        _offset);
  }
}
