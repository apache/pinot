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
package org.apache.pinot.query.planner.optimizer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.pinot.query.planner.spi.CostModelProvider;
import org.apache.pinot.query.planner.spi.QueryOptimizerProvider;


/// A no-op cost-based optimizer used by tests to exercise the plugin seam end-to-end.
///
/// It performs a `Convention.NONE` -> [NoopConvention] -> `Convention.NONE` round-trip in a
/// VolcanoPlanner. The `setNoneConventionHasInfiniteCost(false)` call keeps this test plugin
/// small: a real convention-based plugin must instead define its own convention-specific rel
/// nodes and converter rules so that every node ends up in a finite-cost convention (Volcano
/// treats `Convention.NONE` as infinite-cost by default). That full contract is documented on
/// [QueryOptimizerProvider].
public final class NoopOptimizerProvider implements QueryOptimizerProvider {

  public static final String NAME = "noop";

  static final AtomicInteger FIND_BEST_EXP_CALLS = new AtomicInteger();

  private final List<RelMetadataProvider> _metadataProviders;

  /// Used by [java.util.ServiceLoader]; supplies no metadata providers.
  public NoopOptimizerProvider() {
    this(List.of());
  }

  /// Supplies the given metadata providers, to exercise the broker's metadata installation.
  NoopOptimizerProvider(List<RelMetadataProvider> metadataProviders) {
    _metadataProviders = metadataProviders;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public List<RelMetadataProvider> getMetadataProviders() {
    return _metadataProviders;
  }

  @Override
  public CostModelProvider createCostModelProvider() {
    return NoopCostModelProvider.INSTANCE;
  }

  @Override
  public RelOptPlanner createPlanner(CostModelProvider costModelProvider, Context context,
      List<RelTraitDef> traitDefs) {
    return new NoopVolcanoPlanner(costModelProvider.getCostFactory(), context);
  }

  static final class NoopCostModelProvider implements CostModelProvider {

    static final NoopCostModelProvider INSTANCE = new NoopCostModelProvider();

    private NoopCostModelProvider() {
    }

    @Override
    public RelOptCostFactory getCostFactory() {
      return RelOptCostImpl.FACTORY;
    }

    @Override
    public void configure(Map<String, String> queryOptions) {
    }
  }

  static final class NoopConvention extends Convention.Impl {

    static final NoopConvention INSTANCE = new NoopConvention();

    private NoopConvention() {
      super("Noop", RelNode.class);
    }
  }

  static final class NoopConverter extends ConverterImpl {

    NoopConverter(RelNode input) {
      this(input.getCluster(), input.getTraitSet().replace(NoopConvention.INSTANCE), input);
    }

    NoopConverter(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
      super(cluster, ConventionTraitDef.INSTANCE, traits, input);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new NoopConverter(getCluster(), traitSet, inputs.get(0));
    }
  }

  static final class NoneToNoopConverterRule extends ConverterRule {

    static final NoneToNoopConverterRule INSTANCE = new NoneToNoopConverterRule();

    private NoneToNoopConverterRule() {
      super(RelNode.class, Convention.NONE, NoopConvention.INSTANCE, "NoneToNoop");
    }

    @Override
    public RelNode convert(RelNode rel) {
      return new NoopConverter(rel);
    }
  }

  static final class NoopVolcanoPlanner extends VolcanoPlanner {

    private RelNode _root;

    NoopVolcanoPlanner(RelOptCostFactory costFactory, Context context) {
      super(costFactory, context);
      addRelTraitDef(ConventionTraitDef.INSTANCE);
      addRule(NoneToNoopConverterRule.INSTANCE);
      // See class javadoc: a real plugin keeps this default and defines finite-cost convention rels.
      setNoneConventionHasInfiniteCost(false);
    }

    @Override
    public void setRoot(RelNode rel) {
      _root = rel;
      super.setRoot(rel);
    }

    @Override
    public RelNode findBestExp() {
      FIND_BEST_EXP_CALLS.incrementAndGet();
      // Volcano produces plans in the requested convention, so ask for the no-op convention and
      // strip the converters to get back to Convention.NONE before returning to Pinot.
      RelTraitSet desired = _root.getTraitSet().replace(NoopConvention.INSTANCE);
      super.setRoot(changeTraits(_root, desired));
      return toNone(super.findBestExp());
    }

    private static RelNode toNone(RelNode rel) {
      if (rel instanceof NoopConverter) {
        return toNone(((NoopConverter) rel).getInput());
      }
      List<RelNode> inputs = rel.getInputs();
      List<RelNode> convertedInputs = null;
      for (int i = 0; i < inputs.size(); i++) {
        RelNode converted = toNone(inputs.get(i));
        if (converted != inputs.get(i)) {
          if (convertedInputs == null) {
            convertedInputs = new ArrayList<>(inputs);
          }
          convertedInputs.set(i, converted);
        }
      }
      return convertedInputs == null ? rel : rel.copy(rel.getTraitSet(), convertedInputs);
    }
  }
}
