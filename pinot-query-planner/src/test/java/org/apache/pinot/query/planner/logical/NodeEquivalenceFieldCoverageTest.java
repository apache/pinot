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
package org.apache.pinot.query.planner.logical;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.BasePlanNode;
import org.apache.pinot.query.planner.plannode.EnrichedJoinNode;
import org.apache.pinot.query.planner.plannode.ExchangeNode;
import org.apache.pinot.query.planner.plannode.ExplainedNode;
import org.apache.pinot.query.planner.plannode.FilterNode;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.PlanNodeVisitor;
import org.apache.pinot.query.planner.plannode.ProjectNode;
import org.apache.pinot.query.planner.plannode.SetOpNode;
import org.apache.pinot.query.planner.plannode.SortNode;
import org.apache.pinot.query.planner.plannode.TableScanNode;
import org.apache.pinot.query.planner.plannode.UnnestNode;
import org.apache.pinot.query.planner.plannode.ValueNode;
import org.apache.pinot.query.planner.plannode.WindowNode;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Guards `EquivalentStagesFinder.NodeEquivalence` against field drift.
///
/// The spool optimizer decides that two stages are interchangeable by comparing node fields by hand. That list is
/// maintained separately from `PlanNode.equals`, and a semantically significant field has now reached a plan node
/// without reaching the equivalence check five times: `ignoreNulls` (#14264), `matchCondition` (#15630),
/// `exclude` (#18482), `groupingSets` (#18817) and the `TableFunctionContext` passthrough fields (#18782). Every
/// occurrence gave silent wrong results rather than an error, because a missing comparison only makes the check more
/// permissive.
///
/// This test cannot check that a comparison is *correct*, only that no field is added without a decision: [#FIELDS]
/// must list every field that the decision reads, so a new field fails this test until someone either makes
/// `NodeEquivalence` compare it or records it in [#NOT_COMPARED] with a reason. Proving each comparison behaves
/// correctly needs a value per field and a node built around it, which is what `EquivalentStagesFinderTest` does
/// case by case.
public class NodeEquivalenceFieldCoverageTest {

  /// Every declared instance field that the equivalence decision reads.
  ///
  /// The scope is every type owned by this module that the decision reaches: the plan nodes themselves, and the value
  /// types they hold that are compared through `equals`. It stops at the module edge, so `DataSchema` and
  /// `RelFieldCollation` are out of scope.
  ///
  /// When this test fails, make `NodeEquivalence` compare the new field (or record it in [#NOT_COMPARED] with the
  /// reason it is safe to ignore), then list the field here.
  private static final Map<Class<?>, List<String>> FIELDS = Map.ofEntries(
      Map.entry(BasePlanNode.class, List.of("_stageId", "_dataSchema", "_nodeHint", "_inputs")),
      Map.entry(PlanNode.NodeHint.class, List.of("_hintOptions")),
      Map.entry(AggregateNode.class, List.of("_aggCalls", "_filterArgs", "_groupKeys", "_aggType",
          "_leafReturnFinalResult", "_groupingSets", "_collations", "_limit")),
      Map.entry(FilterNode.class, List.of("_condition")),
      Map.entry(JoinNode.class, List.of("_joinType", "_leftKeys", "_rightKeys", "_nonEquiConditions", "_joinStrategy",
          "_matchCondition")),
      // EnrichedJoinNode is deprecated for removal, but NodeEquivalence still has a visit method for it, so its
      // fields stay in scope until that method goes away.
      Map.entry(EnrichedJoinNode.class, List.of("_filterProjectRexes", "_joinResultSchema", "_projectResultSchema",
          "_fetch", "_offset")),
      Map.entry(EnrichedJoinNode.FilterProjectRex.class, List.of("_type", "_filter", "_projectAndResultSchema")),
      Map.entry(EnrichedJoinNode.FilterProjectRex.ProjectAndResultSchema.class, List.of("_project", "_schema")),
      Map.entry(MailboxReceiveNode.class, List.of("_senderStageId", "_exchangeType", "_distributionType", "_keys",
          "_collations", "_sort", "_sortedOnSender", "_sender")),
      Map.entry(MailboxSendNode.class, List.of("_receiverStages", "_exchangeType", "_distributionType", "_keys",
          "_prePartitioned", "_collations", "_sort", "_hashFunction")),
      Map.entry(ProjectNode.class, List.of("_projects")),
      Map.entry(SetOpNode.class, List.of("_setOpType", "_all")),
      Map.entry(SortNode.class, List.of("_collations", "_fetch", "_offset")),
      Map.entry(TableScanNode.class, List.of("_tableName", "_columns")),
      Map.entry(UnnestNode.class, List.of("_arrayExprs", "_tableFunctionContext")),
      Map.entry(UnnestNode.TableFunctionContext.class, List.of("_withOrdinality", "_elementIndexes",
          "_ordinalityIndex", "_passthroughInputIndexes", "_prunedPassthrough")),
      Map.entry(ValueNode.class, List.of("_literalRows")),
      Map.entry(WindowNode.class, List.of("_keys", "_collations", "_aggCalls", "_windowFrameType", "_lowerBound",
          "_upperBound", "_exclude", "_constants")),
      Map.entry(RexExpression.InputRef.class, List.of("_index")),
      Map.entry(RexExpression.Literal.class, List.of("_dataType", "_value")),
      Map.entry(RexExpression.FunctionCall.class, List.of("_dataType", "_functionName", "_functionOperands",
          "_isDistinct", "_ignoreNulls")));

  /// Fields that `NodeEquivalence` deliberately does not compare, and why. Before this test the reasoning lived in
  /// commented-out code inside the equivalence check, which cannot fail once the reasoning stops holding.
  ///
  /// A reason here is a claim about today's planner, not a permanent property. If one of them stops being true, the
  /// field has to move into the comparison.
  private static final Map<Class<?>, Map<String, String>> NOT_COMPARED = Map.of(
      BasePlanNode.class, Map.of(
          "_stageId", "Equivalence is asked across stages, so the ids always differ"),
      MailboxReceiveNode.class, Map.of(
          "_senderStageId", "The senders themselves are compared for equivalence instead"),
      MailboxSendNode.class, Map.of(
          "_receiverStages", "Who reads a stage does not change what the stage computes",
          "_sort", "Sending side sort is not implemented (see the TODO in MailboxSendOperator), and a difference "
              + "visible to a receiver is already compared in visitMailboxReceive",
          "_hashFunction", "One hash function is threaded through a whole v1 plan, so two send nodes in the same "
              + "plan always agree on it"),
      EnrichedJoinNode.class, Map.of(
          "_joinResultSchema", "Only PlanNodeDeserializer builds an EnrichedJoinNode, so the broker side planner "
              + "that runs this check never sees one",
          "_projectResultSchema", "This is the node's own data schema, which areBaseNodesEquivalent compares"));

  /// Node types that `NodeEquivalence` rejects outright, so none of their fields take part in the decision. Both
  /// `visitExchange` and `visitExplained` throw `UnsupportedOperationException`: the fragmenter removes exchanges
  /// before spooling runs, and explained nodes only exist once a plan is rendered.
  private static final Set<Class<?>> NOT_VISITED = Set.of(ExchangeNode.class, ExplainedNode.class);

  /// A new node type must either be registered in [#FIELDS] or declared out of scope in [#NOT_VISITED].
  @Test
  public void everyVisitedNodeTypeIsRegistered() {
    for (Method method : PlanNodeVisitor.class.getDeclaredMethods()) {
      if (!method.getName().startsWith("visit")) {
        continue;
      }
      Class<?> nodeType = method.getParameterTypes()[0];
      assertTrue(FIELDS.containsKey(nodeType) || NOT_VISITED.contains(nodeType),
          nodeType.getSimpleName() + " is visited by NodeEquivalence but is not registered in FIELDS. Add its declared "
              + "fields there, or add it to NOT_VISITED if the equivalence check rejects it.");
    }
  }

  @Test
  public void registeredFieldsMatchDeclaredFields() {
    for (Map.Entry<Class<?>, List<String>> entry : FIELDS.entrySet()) {
      Class<?> type = entry.getKey();
      Set<String> declared = new TreeSet<>();
      for (Field field : type.getDeclaredFields()) {
        // Skip statics and the synthetic fields that coverage builds add, e.g. $jacocoData.
        if (!field.isSynthetic() && !Modifier.isStatic(field.getModifiers())) {
          declared.add(field.getName());
        }
      }
      assertEquals(declared, new TreeSet<>(entry.getValue()),
          type.getSimpleName() + " declares different fields than this test expects. For every added field, make "
              + "NodeEquivalence compare it (or record it in NOT_COMPARED with a reason), then update FIELDS.");
      Set<String> excluded = NOT_COMPARED.getOrDefault(type, Map.of()).keySet();
      assertTrue(declared.containsAll(excluded),
          type.getSimpleName() + " has NOT_COMPARED entries for fields it no longer declares: " + excluded);
    }
  }
}
