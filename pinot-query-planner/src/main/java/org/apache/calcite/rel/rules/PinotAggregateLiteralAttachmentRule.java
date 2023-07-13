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
package org.apache.calcite.rel.rules;

import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.hint.PinotHintOptions;
import org.apache.calcite.rel.hint.PinotHintStrategyTable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.Geometries;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Sarg;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.util.Util;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.spi.data.FieldSpec;
import org.checkerframework.checker.nullness.qual.Nullable;


/**
 * Special rule to attach Literal to Aggregate call.
 */
public class PinotAggregateLiteralAttachmentRule extends RelOptRule {
  public static final PinotAggregateLiteralAttachmentRule INSTANCE =
      new PinotAggregateLiteralAttachmentRule(PinotRuleUtils.PINOT_REL_FACTORY);

  public PinotAggregateLiteralAttachmentRule(RelBuilderFactory factory) {
    super(operand(LogicalAggregate.class, some(operand(LogicalProject.class, any()))), factory, null);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    if (call.rels.length < 1) {
      return false;
    }
    if (call.rel(0) instanceof Aggregate) {
      Aggregate agg = call.rel(0);
      ImmutableList<RelHint> hints = agg.getHints();
      return !PinotHintStrategyTable.containsHintOption(hints,
          PinotHintOptions.INTERNAL_AGG_OPTIONS, PinotHintOptions.InternalAggregateOptions.AGG_CALL_SIGNATURE);
    }
    return false;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Aggregate aggregate = call.rel(0);
    Map<Pair<Integer, Integer>, RexLiteral> rexLiterals = extractRexLiterals(call);
    List<RelHint> newHints = PinotHintStrategyTable.replaceHintOptions(aggregate.getHints(),
        PinotHintOptions.INTERNAL_AGG_OPTIONS, PinotHintOptions.InternalAggregateOptions.AGG_CALL_SIGNATURE,
        createRexLiteralHintsString(rexLiterals));
    call.transformTo(new LogicalAggregate(aggregate.getCluster(), aggregate.getTraitSet(), newHints,
        aggregate.getInput(), aggregate.getGroupSet(), aggregate.getGroupSets(), aggregate.getAggCallList()));
  }

  private static Map<Pair<Integer, Integer>, RexLiteral> extractRexLiterals(RelOptRuleCall call) {
    Aggregate aggregate = call.rel(0);
    Project project = call.rel(1);
    List<RexNode> rexNodes = project.getProjects();
    List<AggregateCall> aggCallList = aggregate.getAggCallList();
    final Map<Pair<Integer, Integer>, RexLiteral> rexLiteralMap = new HashMap<>();
    for (int aggIdx = 0; aggIdx < aggCallList.size(); aggIdx++) {
      AggregateCall aggCall = aggCallList.get(aggIdx);
      for (int argIdx = 0; argIdx < aggCall.getArgList().size(); argIdx++) {
        RexNode field = rexNodes.get(aggCall.getArgList().get(argIdx));
        if (field instanceof RexLiteral) {
          rexLiteralMap.put(new Pair<>(aggIdx, argIdx), (RexLiteral) field);
        }
      }
    }
    return rexLiteralMap;
  }

  private static String createRexLiteralHintsString(Map<Pair<Integer, Integer>, RexLiteral> rexLiterals) {
    List<String> rexLiteralStrings = new ArrayList<>(rexLiterals.size());
    for (Map.Entry<Pair<Integer, Integer>, RexLiteral> e : rexLiterals.entrySet()) {
      SqlTypeName typeName = e.getValue().getTypeName();
      String valueStr = RexLiteralToString(typeName, e.getValue().getValue());
      rexLiteralStrings.add(String.format("%d_%d_%s_%s", e.getKey().left, e.getKey().right, typeName.name(), valueStr));
    }
    return "{" + StringUtils.join(rexLiteralStrings, ",") + "}";
  }

  private static String RexLiteralToString(SqlTypeName sqlTypeName, Object value) {
    switch (sqlTypeName) {
      case DECIMAL:
      case DOUBLE:
      case REAL:
      case INTEGER:
      case BIGINT:
        return value.toString();
      case CHAR:
      case VARCHAR:
        return value instanceof NlsString ? ((NlsString) value).getValue() : value.toString();
      default:
        throw new UnsupportedOperationException("");
    }
  }
}
