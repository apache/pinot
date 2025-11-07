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

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalAsofJoin;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowExclusion;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.calcite.rel.logical.PinotLogicalAggregate;
import org.apache.pinot.calcite.rel.logical.PinotLogicalEnrichedJoin;
import org.apache.pinot.calcite.rel.logical.PinotLogicalExchange;
import org.apache.pinot.calcite.rel.logical.PinotLogicalSortExchange;
import org.apache.pinot.calcite.rel.logical.PinotLogicalTableScan;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.calcite.rel.rules.PinotRuleUtils;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.BasePlanNode;
import org.apache.pinot.query.planner.plannode.EnrichedJoinNode;
import org.apache.pinot.query.planner.plannode.ExchangeNode;
import org.apache.pinot.query.planner.plannode.FilterNode;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.PlanNode.NodeHint;
import org.apache.pinot.query.planner.plannode.ProjectNode;
import org.apache.pinot.query.planner.plannode.SetOpNode;
import org.apache.pinot.query.planner.plannode.SortNode;
import org.apache.pinot.query.planner.plannode.TableScanNode;
import org.apache.pinot.query.planner.plannode.UnnestNode;
import org.apache.pinot.query.planner.plannode.ValueNode;
import org.apache.pinot.query.planner.plannode.WindowNode;
import org.codehaus.commons.nullanalysis.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@link RelToPlanNodeConverter} converts a logical {@link RelNode} to a {@link PlanNode}.
 */
public final class RelToPlanNodeConverter {
  private static final Logger LOGGER = LoggerFactory.getLogger(RelToPlanNodeConverter.class);
  private static final int DEFAULT_STAGE_ID = -1;

  private final BrokerMetrics _brokerMetrics = BrokerMetrics.get();
  private boolean _joinFound;
  private boolean _windowFunctionFound;
  @Nullable
  private final TransformationTracker.Builder<PlanNode, RelNode> _tracker;
  private final String _hashFunction;

  public RelToPlanNodeConverter(@Nullable TransformationTracker.Builder<PlanNode, RelNode> tracker,
      String hashFunction) {
    _tracker = tracker;
    _hashFunction = hashFunction;
  }

  /**
   * Converts a {@link RelNode} into its serializable counterpart.
   * NOTE: Stage ID is not determined yet.
   */
  public PlanNode toPlanNode(RelNode node) {
    PlanNode result;
    if (node instanceof PinotLogicalTableScan) {
      result = convertPinotLogicalTableScan((PinotLogicalTableScan) node);
    } else if (node instanceof Uncollect) {
      result = convertLogicalUncollect((Uncollect) node);
    } else if (node instanceof LogicalCorrelate) {
      result = convertLogicalCorrelate((LogicalCorrelate) node);
    } else if (node instanceof LogicalProject) {
      result = convertLogicalProject((LogicalProject) node);
    } else if (node instanceof LogicalFilter) {
      result = convertLogicalFilter((LogicalFilter) node);
    } else if (node instanceof PinotLogicalAggregate) {
      result = convertLogicalAggregate((PinotLogicalAggregate) node);
    } else if (node instanceof LogicalSort) {
      result = convertLogicalSort((LogicalSort) node);
    } else if (node instanceof Exchange) {
      result = convertLogicalExchange((Exchange) node);
    } else if (node instanceof PinotLogicalEnrichedJoin) {
      result = convertLogicalEnrichedJoin((PinotLogicalEnrichedJoin) node);
    } else if (node instanceof LogicalJoin) {
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.JOIN_COUNT, 1);
      if (!_joinFound) {
        _brokerMetrics.addMeteredGlobalValue(BrokerMeter.QUERIES_WITH_JOINS, 1);
        _joinFound = true;
      }
      result = convertLogicalJoin((LogicalJoin) node);
    } else if (node instanceof LogicalAsofJoin) {
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.JOIN_COUNT, 1);
      if (!_joinFound) {
        _brokerMetrics.addMeteredGlobalValue(BrokerMeter.QUERIES_WITH_JOINS, 1);
        _joinFound = true;
      }
      result = convertLogicalAsofJoin((LogicalAsofJoin) node);
    } else if (node instanceof LogicalWindow) {
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.WINDOW_COUNT, 1);
      if (!_windowFunctionFound) {
        _brokerMetrics.addMeteredGlobalValue(BrokerMeter.QUERIES_WITH_WINDOW, 1);
        _windowFunctionFound = true;
      }
      result = convertLogicalWindow((LogicalWindow) node);
    } else if (node instanceof LogicalValues) {
      result = convertLogicalValues((LogicalValues) node);
    } else if (node instanceof SetOp) {
      result = convertLogicalSetOp((SetOp) node);
    } else {
      throw new IllegalStateException("Unsupported RelNode: " + node);
    }
    if (_tracker != null) {
      _tracker.trackCreation(node, result);
    }
    return result;
  }

  private UnnestNode convertLogicalUncollect(Uncollect node) {
    // Expect input provides a single array expression (typically a Project with one expression)
    RexExpression arrayExpr = null;
    RelNode input = node.getInput();
    if (input instanceof Project) {
      Project p = (Project) input;
      if (p.getProjects().size() == 1) {
        arrayExpr = RexExpressionUtils.fromRexNode(p.getProjects().get(0));
      }
    }
    if (arrayExpr == null) {
      // Fallback: refer to first input ref
      arrayExpr = new RexExpression.InputRef(0);
    }
    String columnAlias = null;
    if (!node.getRowType().getFieldList().isEmpty()) {
      columnAlias = node.getRowType().getFieldList().get(0).getName();
    }
    boolean withOrdinality = false;
    String ordinalityAlias = null;
    // Calcite Uncollect exposes withOrdinality via field names if present; if >1 fields and last is ordinality
    if (node.getRowType().getFieldList().size() > 1) {
      withOrdinality = true;
      ordinalityAlias = node.getRowType().getFieldList().get(1).getName();
    }
    return new UnnestNode(DEFAULT_STAGE_ID, toDataSchema(node.getRowType()), NodeHint.EMPTY,
        convertInputs(node.getInputs()), arrayExpr, columnAlias, withOrdinality, ordinalityAlias);
  }

  private BasePlanNode convertLogicalCorrelate(LogicalCorrelate node) {
    // Pattern: Correlate(left, Uncollect(Project(correlatedField)))
    RelNode right = node.getRight();
    RelDataType leftRowType = node.getLeft().getRowType();
    Project correlatedProject = findProjectUnderUncollect(right);
    RexExpression arrayExpr =
        correlatedProject != null ? deriveArrayExpression(correlatedProject, leftRowType) : null;
    if (arrayExpr == null) {
      arrayExpr = new RexExpression.InputRef(0);
    }
    LogicalFilter correlateFilter = findCorrelateFilter(right);
    boolean wrapWithFilter = correlateFilter != null;
    RexNode filterCondition = wrapWithFilter ? correlateFilter.getCondition() : null;
    // Use the entire correlate output schema
    PlanNode inputNode = toPlanNode(node.getLeft());
    // Ensure inputs list is mutable because downstream visitors (e.g., withInputs methods) may modify the inputs list
    List<PlanNode> inputs = new ArrayList<>(List.of(inputNode));
    ElementOrdinalInfo ordinalInfo = deriveElementOrdinalInfo(right, leftRowType);
    boolean withOrdinality = ordinalInfo.hasOrdinality();
    String elementAlias = ordinalInfo.getElementAlias();
    String ordinalityAlias = ordinalInfo.getOrdinalityAlias();
    int elementIndex = ordinalInfo.getElementIndex();
    int ordinalityIndex = ordinalInfo.getOrdinalityIndex();
    UnnestNode unnest = new UnnestNode(DEFAULT_STAGE_ID, toDataSchema(node.getRowType()), NodeHint.EMPTY,
        inputs, arrayExpr, elementAlias, withOrdinality, ordinalityAlias, elementIndex, ordinalityIndex);
    if (wrapWithFilter) {
      // Wrap Unnest with a FilterNode; rewrite filter InputRefs (0:elem,1:idx) to absolute output indexes
      RexExpression rewritten =
          rewriteInputRefs(RexExpressionUtils.fromRexNode(filterCondition), elementIndex, ordinalityIndex);
      return new FilterNode(DEFAULT_STAGE_ID, toDataSchema(node.getRowType()), NodeHint.EMPTY,
          new ArrayList<>(List.of(unnest)), rewritten);
    }
    return unnest;
  }

  @Nullable
  private static Project findProjectUnderUncollect(RelNode node) {
    RelNode current = node;
    while (current != null) {
      if (current instanceof Uncollect) {
        RelNode input = ((Uncollect) current).getInput();
        return input instanceof Project ? (Project) input : null;
      }
      if (current instanceof Project) {
        current = ((Project) current).getInput();
      } else if (current instanceof LogicalFilter) {
        current = ((LogicalFilter) current).getInput();
      } else {
        return null;
      }
    }
    return null;
  }

  @Nullable
  private static RexExpression deriveArrayExpression(Project project, RelDataType leftRowType) {
    if (project.getProjects().size() != 1) {
      return null;
    }
    RexNode rex = project.getProjects().get(0);
    Integer idx = resolveInputRefFromCorrel(rex, leftRowType);
    if (idx != null) {
      return new RexExpression.InputRef(idx);
    }
    RexExpression candidate = RexExpressionUtils.fromRexNode(rex);
    return candidate instanceof RexExpression.InputRef ? candidate : new RexExpression.InputRef(0);
  }

  @Nullable
  private static LogicalFilter findCorrelateFilter(RelNode node) {
    RelNode current = node;
    while (current instanceof Project || current instanceof LogicalFilter) {
      if (current instanceof LogicalFilter) {
        return (LogicalFilter) current;
      }
      current = ((Project) current).getInput();
    }
    return null;
  }

  private static ElementOrdinalInfo deriveElementOrdinalInfo(RelNode right, RelDataType leftRowType) {
    ElementOrdinalAccumulator accumulator = new ElementOrdinalAccumulator(leftRowType.getFieldCount());
    if (right instanceof Uncollect) {
      accumulator.populateFromRowType(right.getRowType());
    } else if (right instanceof Project) {
      accumulator.populateFromProject((Project) right);
    } else if (right instanceof LogicalFilter) {
      LogicalFilter filter = (LogicalFilter) right;
      RelNode filterInput = filter.getInput();
      if (filterInput instanceof Uncollect) {
        accumulator.populateFromRowType(filter.getRowType());
      } else if (filterInput instanceof Project) {
        accumulator.populateFromProject((Project) filterInput);
      }
    }
    return accumulator.toInfo();
  }

  private static final class ElementOrdinalAccumulator {
    private final int _base;
    private String _elementAlias;
    private String _ordinalityAlias;
    private int _elementIndex = -1;
    private int _ordinalityIndex = -1;

    ElementOrdinalAccumulator(int base) {
      _base = base;
    }

    void populateFromRowType(RelDataType rowType) {
      List<RelDataTypeField> fields = rowType.getFieldList();
      if (!fields.isEmpty() && _elementIndex < 0) {
        _elementAlias = fields.get(0).getName();
        _elementIndex = _base;
      }
      if (fields.size() > 1 && _ordinalityIndex < 0) {
        _ordinalityAlias = fields.get(1).getName();
        _ordinalityIndex = _base + 1;
      }
    }

    void populateFromProject(Project project) {
      List<RexNode> projects = project.getProjects();
      List<RelDataTypeField> projFields = project.getRowType().getFieldList();
      for (int j = 0; j < projects.size(); j++) {
        RexNode pj = projects.get(j);
        if (pj instanceof RexInputRef) {
          int idx = ((RexInputRef) pj).getIndex();
          String outName = projFields.get(j).getName();
          if (idx == 0 && _elementIndex < 0) {
            _elementIndex = _base + j;
            _elementAlias = outName;
          } else if (idx == 1 && _ordinalityIndex < 0) {
            _ordinalityIndex = _base + j;
            _ordinalityAlias = outName;
          }
        }
      }
    }

    ElementOrdinalInfo toInfo() {
      return new ElementOrdinalInfo(_elementAlias, _ordinalityAlias, _elementIndex, _ordinalityIndex);
    }
  }

  private static final class ElementOrdinalInfo {
    private final String _elementAlias;
    private final String _ordinalityAlias;
    private final int _elementIndex;
    private final int _ordinalityIndex;

    ElementOrdinalInfo(String elementAlias, String ordinalityAlias, int elementIndex, int ordinalityIndex) {
      _elementAlias = elementAlias;
      _ordinalityAlias = ordinalityAlias;
      _elementIndex = elementIndex;
      _ordinalityIndex = ordinalityIndex;
    }

    String getElementAlias() {
      return _elementAlias;
    }

    String getOrdinalityAlias() {
      return _ordinalityAlias;
    }

    int getElementIndex() {
      return _elementIndex;
    }

    int getOrdinalityIndex() {
      return _ordinalityIndex;
    }

    boolean hasOrdinality() {
      return _ordinalityIndex >= 0;
    }
  }

  private static RexExpression rewriteInputRefs(RexExpression expr, int elemOutIdx, int ordOutIdx) {
    if (expr instanceof RexExpression.InputRef) {
      int idx = ((RexExpression.InputRef) expr).getIndex();
      if (idx == 0 && elemOutIdx >= 0) {
        return new RexExpression.InputRef(elemOutIdx);
      } else if (idx == 1 && ordOutIdx >= 0) {
        return new RexExpression.InputRef(ordOutIdx);
      } else {
        return expr;
      }
    } else if (expr instanceof RexExpression.FunctionCall) {
      RexExpression.FunctionCall fc = (RexExpression.FunctionCall) expr;
      List<RexExpression> ops = fc.getFunctionOperands();
      List<RexExpression> rewritten = new ArrayList<>(ops.size());
      for (RexExpression op : ops) {
        rewritten.add(rewriteInputRefs(op, elemOutIdx, ordOutIdx));
      }
      return new RexExpression.FunctionCall(fc.getDataType(), fc.getFunctionName(), rewritten);
    } else {
      return expr;
    }
  }

  private static Integer resolveInputRefFromCorrel(RexNode expr, RelDataType leftRowType) {
    if (expr instanceof RexFieldAccess) {
      RexFieldAccess fieldAccess = (RexFieldAccess) expr;
      if (fieldAccess.getReferenceExpr() instanceof RexCorrelVariable) {
        String fieldName = fieldAccess.getField().getName();
        List<RelDataTypeField> fields = leftRowType.getFieldList();
        for (int i = 0; i < fields.size(); i++) {
          // SQL field names are case-insensitive, so we must use equalsIgnoreCase for correct matching.
          if (fields.get(i).getName().equalsIgnoreCase(fieldName)) {
            return i;
          }
        }
      }
    }
    return null;
  }

  private ExchangeNode convertLogicalExchange(Exchange node) {
    RelDistribution distribution = node.getDistribution();
    RelDistribution.Type distributionType = distribution.getType();
    PinotRelExchangeType exchangeType;
    List<Integer> keys;
    Boolean prePartitioned;
    List<RelFieldCollation> collations;
    boolean sortOnSender;
    boolean sortOnReceiver;
    if (node instanceof PinotLogicalSortExchange) {
      PinotLogicalSortExchange sortExchange = (PinotLogicalSortExchange) node;
      exchangeType = sortExchange.getExchangeType();
      keys = distribution.getKeys();
      prePartitioned = null;
      collations = sortExchange.getCollation().getFieldCollations();
      sortOnSender = sortExchange.isSortOnSender();
      sortOnReceiver = sortExchange.isSortOnReceiver();
    } else {
      assert node instanceof PinotLogicalExchange;
      PinotLogicalExchange exchange = (PinotLogicalExchange) node;
      exchangeType = exchange.getExchangeType();
      keys = exchange.getKeys();
      prePartitioned = exchange.getPrePartitioned();
      collations = null;
      sortOnSender = false;
      sortOnReceiver = false;
    }
    if (keys.isEmpty()) {
      keys = null;
    }
    if (prePartitioned == null) {
      if (distributionType == RelDistribution.Type.HASH_DISTRIBUTED) {
        RelDistribution inputDistributionTrait = node.getInputs().get(0).getTraitSet().getDistribution();
        prePartitioned = distribution.equals(inputDistributionTrait);
      } else {
        prePartitioned = false;
      }
    }
    return new ExchangeNode(DEFAULT_STAGE_ID, toDataSchema(node.getRowType()), convertInputs(node.getInputs()),
        exchangeType, distributionType, keys, prePartitioned, collations, sortOnSender, sortOnReceiver, null, null,
        _hashFunction);
  }

  private SetOpNode convertLogicalSetOp(SetOp node) {
    return new SetOpNode(DEFAULT_STAGE_ID, toDataSchema(node.getRowType()), NodeHint.fromRelHints(node.getHints()),
        convertInputs(node.getInputs()), SetOpNode.SetOpType.fromObject(node), node.all);
  }

  private ValueNode convertLogicalValues(LogicalValues node) {
    List<List<RexExpression.Literal>> literalRows = new ArrayList<>(node.tuples.size());
    for (List<RexLiteral> tuple : node.tuples) {
      List<RexExpression.Literal> literalRow = new ArrayList<>(tuple.size());
      for (RexLiteral rexLiteral : tuple) {
        literalRow.add(RexExpressionUtils.fromRexLiteral(rexLiteral));
      }
      literalRows.add(literalRow);
    }
    return new ValueNode(DEFAULT_STAGE_ID, toDataSchema(node.getRowType()), NodeHint.fromRelHints(node.getHints()),
        convertInputs(node.getInputs()), literalRows);
  }

  /**
   * TODO: Add support for exclude clauses ({@link org.apache.calcite.rex.RexWindowExclusion})
   */
  private WindowNode convertLogicalWindow(LogicalWindow node) {
    // Only a single Window Group should exist per WindowNode.
    Preconditions.checkState(node.groups.size() == 1, "Only a single window group is allowed, got: %s",
        node.groups.size());
    Window.Group windowGroup = node.groups.get(0);

    Preconditions.checkState(windowGroup.exclude == RexWindowExclusion.EXCLUDE_NO_OTHER,
        "EXCLUDE clauses for window functions are not currently supported");

    int numAggregates = windowGroup.aggCalls.size();
    List<RexExpression.FunctionCall> aggCalls = new ArrayList<>(numAggregates);
    for (int i = 0; i < numAggregates; i++) {
      aggCalls.add(RexExpressionUtils.fromWindowAggregateCall(windowGroup.aggCalls.get(i)));
    }
    WindowNode.WindowFrameType windowFrameType =
        windowGroup.isRows ? WindowNode.WindowFrameType.ROWS : WindowNode.WindowFrameType.RANGE;

    int lowerBound;
    if (windowGroup.lowerBound.isUnbounded()) {
      // Lower bound can't be unbounded following
      lowerBound = Integer.MIN_VALUE;
    } else if (windowGroup.lowerBound.isCurrentRow()) {
      lowerBound = 0;
    } else {
      // The literal value is extracted from the constants in the PinotWindowExchangeNodeInsertRule
      RexLiteral offset = (RexLiteral) windowGroup.lowerBound.getOffset();
      lowerBound = offset == null ? Integer.MIN_VALUE
          : (windowGroup.lowerBound.isPreceding() ? -1 * RexExpressionUtils.getValueAsInt(offset)
              : RexExpressionUtils.getValueAsInt(offset));
    }
    int upperBound;
    if (windowGroup.upperBound.isUnbounded()) {
      // Upper bound can't be unbounded preceding
      upperBound = Integer.MAX_VALUE;
    } else if (windowGroup.upperBound.isCurrentRow()) {
      upperBound = 0;
    } else {
      // The literal value is extracted from the constants in the PinotWindowExchangeNodeInsertRule
      RexLiteral offset = (RexLiteral) windowGroup.upperBound.getOffset();
      upperBound = offset == null ? Integer.MAX_VALUE
          : (windowGroup.upperBound.isFollowing() ? RexExpressionUtils.getValueAsInt(offset)
              : -1 * RexExpressionUtils.getValueAsInt(offset));
    }

    // TODO: The constants are already extracted in the PinotWindowExchangeNodeInsertRule, we can remove them from
    // the WindowNode and plan serde.
    List<RexExpression.Literal> constants = new ArrayList<>(node.constants.size());
    for (RexLiteral constant : node.constants) {
      constants.add(RexExpressionUtils.fromRexLiteral(constant));
    }
    return new WindowNode(DEFAULT_STAGE_ID, toDataSchema(node.getRowType()), NodeHint.fromRelHints(node.getHints()),
        convertInputs(node.getInputs()), windowGroup.keys.asList(), windowGroup.orderKeys.getFieldCollations(),
        aggCalls, windowFrameType, lowerBound, upperBound, constants);
  }

  private SortNode convertLogicalSort(LogicalSort node) {
    int fetch = RexExpressionUtils.getValueAsInt(node.fetch);
    int offset = RexExpressionUtils.getValueAsInt(node.offset);
    return new SortNode(DEFAULT_STAGE_ID, toDataSchema(node.getRowType()), NodeHint.fromRelHints(node.getHints()),
        convertInputs(node.getInputs()), node.getCollation().getFieldCollations(), fetch, offset);
  }

  private AggregateNode convertLogicalAggregate(PinotLogicalAggregate node) {
    List<AggregateCall> aggregateCalls = node.getAggCallList();
    int numAggregates = aggregateCalls.size();
    List<RexExpression.FunctionCall> functionCalls = new ArrayList<>(numAggregates);
    List<Integer> filterArgs = new ArrayList<>(numAggregates);
    for (AggregateCall aggregateCall : aggregateCalls) {
      functionCalls.add(RexExpressionUtils.fromAggregateCall(aggregateCall));
      filterArgs.add(aggregateCall.filterArg);
    }
    return new AggregateNode(DEFAULT_STAGE_ID, toDataSchema(node.getRowType()), NodeHint.fromRelHints(node.getHints()),
        convertInputs(node.getInputs()), functionCalls, filterArgs, node.getGroupSet().asList(), node.getAggType(),
        node.isLeafReturnFinalResult(), node.getCollations(), node.getLimit());
  }

  private ProjectNode convertLogicalProject(LogicalProject node) {
    return new ProjectNode(DEFAULT_STAGE_ID, toDataSchema(node.getRowType()), NodeHint.fromRelHints(node.getHints()),
        convertInputs(node.getInputs()), RexExpressionUtils.fromRexNodes(node.getProjects()));
  }

  private FilterNode convertLogicalFilter(LogicalFilter node) {
    return new FilterNode(DEFAULT_STAGE_ID, toDataSchema(node.getRowType()), NodeHint.fromRelHints(node.getHints()),
        convertInputs(node.getInputs()), RexExpressionUtils.fromRexNode(node.getCondition()));
  }

  private TableScanNode convertPinotLogicalTableScan(PinotLogicalTableScan node) {
    String tableName = getTableNameFromTableScan(node);
    List<RelDataTypeField> fields = node.getRowType().getFieldList();
    List<String> columns = new ArrayList<>(fields.size());
    for (RelDataTypeField field : fields) {
      columns.add(field.getName());
    }
    return new TableScanNode(DEFAULT_STAGE_ID, toDataSchema(node.getRowType()), NodeHint.fromRelHints(node.getHints()),
        convertInputs(node.getInputs()), tableName, columns);
  }

  private JoinNode convertLogicalJoin(LogicalJoin join) {
    JoinInfo joinInfo = join.analyzeCondition();
    DataSchema dataSchema = toDataSchema(join.getRowType());
    List<PlanNode> inputs = convertInputs(join.getInputs());
    JoinRelType joinType = join.getJoinType();

    // Run some validations for join
    Preconditions.checkState(inputs.size() == 2, "Join should have exactly 2 inputs, got: %s", inputs.size());
    PlanNode left = inputs.get(0);
    PlanNode right = inputs.get(1);
    int numLeftColumns = left.getDataSchema().size();
    int numResultColumns = dataSchema.size();
    if (joinType.projectsRight()) {
      int numRightColumns = right.getDataSchema().size();
      Preconditions.checkState(numLeftColumns + numRightColumns == numResultColumns,
          "Invalid number of columns for join type: %s, left: %s, right: %s, result: %s", joinType, numLeftColumns,
          numRightColumns, numResultColumns);
    } else {
      Preconditions.checkState(numLeftColumns == numResultColumns,
          "Invalid number of columns for join type: %s, left: %s, result: %s", joinType, numLeftColumns,
          numResultColumns);
    }

    // Check if the join hint specifies the join strategy
    JoinNode.JoinStrategy joinStrategy;
    if (PinotHintOptions.JoinHintOptions.useLookupJoinStrategy(join)) {
      joinStrategy = JoinNode.JoinStrategy.LOOKUP;

      // Run some validations for lookup join
      Preconditions.checkArgument(!joinInfo.leftKeys.isEmpty(), "Lookup join requires join keys");
      // Right table should be a dimension table, and the right input should be an identifier only ProjectNode over
      // TableScanNode.
      RelNode rightInput = PinotRuleUtils.unboxRel(join.getRight());
      Preconditions.checkState(rightInput instanceof Project, "Right input for lookup join must be a Project, got: %s",
          rightInput.getClass().getSimpleName());
      Project project = (Project) rightInput;
      for (RexNode node : project.getProjects()) {
        Preconditions.checkState(node instanceof RexInputRef,
            "Right input for lookup join must be an identifier (RexInputRef) only Project, got: %s in project",
            node.getClass().getSimpleName());
      }
      RelNode projectInput = PinotRuleUtils.unboxRel(project.getInput());
      Preconditions.checkState(projectInput instanceof TableScan,
          "Right input for lookup join must be a Project over TableScan, got Project over: %s",
          projectInput.getClass().getSimpleName());
    } else {
      // TODO: Consider adding DYNAMIC_BROADCAST as a separate join strategy
      joinStrategy = JoinNode.JoinStrategy.HASH;
    }

    return new JoinNode(DEFAULT_STAGE_ID, dataSchema, NodeHint.fromRelHints(join.getHints()), inputs, joinType,
        joinInfo.leftKeys, joinInfo.rightKeys, RexExpressionUtils.fromRexNodes(joinInfo.nonEquiConditions),
        joinStrategy);
  }

  private EnrichedJoinNode convertLogicalEnrichedJoin(PinotLogicalEnrichedJoin rel) {
    JoinInfo joinInfo = rel.analyzeCondition();
    DataSchema joinResultSchema = toDataSchema(rel.getJoinRowType());
    DataSchema projectedSchema = toDataSchema(rel.getRowType());
    List<PlanNode> inputs = convertInputs(rel.getInputs());
    JoinRelType joinType = rel.getJoinType();

    // Run some validations for join
    Preconditions.checkState(inputs.size() == 2, "Join should have exactly 2 inputs, got: %s", inputs.size());
    PlanNode left = inputs.get(0);
    PlanNode right = inputs.get(1);
    int numLeftColumns = left.getDataSchema().size();
    int numJoinResultColumns = joinResultSchema.size();
    if (joinType.projectsRight()) {
      int numRightColumns = right.getDataSchema().size();
      Preconditions.checkState(numLeftColumns + numRightColumns == numJoinResultColumns,
          "Invalid number of columns for join type: %s, left: %s, right: %s, result: %s", joinType, numLeftColumns,
          numRightColumns, numJoinResultColumns);
    } else {
      Preconditions.checkState(numLeftColumns == numJoinResultColumns,
          "Invalid number of columns for join type: %s, left: %s, result: %s", joinType, numLeftColumns,
          numJoinResultColumns);
    }

    // Check if the join hint specifies the join strategy
    JoinNode.JoinStrategy joinStrategy;
    if (PinotHintOptions.JoinHintOptions.useLookupJoinStrategy(rel)) {
      joinStrategy = JoinNode.JoinStrategy.LOOKUP;

      // Run some validations for lookup join
      Preconditions.checkArgument(!joinInfo.leftKeys.isEmpty(), "Lookup join requires join keys");
      // Right table should be a dimension table, and the right input should be an identifier only ProjectNode over
      // TableScanNode.
      RelNode rightInput = PinotRuleUtils.unboxRel(rel.getRight());
      Preconditions.checkState(rightInput instanceof Project, "Right input for lookup join must be a Project, got: %s",
          rightInput.getClass().getSimpleName());
      Project project = (Project) rightInput;
      for (RexNode node : project.getProjects()) {
        Preconditions.checkState(node instanceof RexInputRef,
            "Right input for lookup join must be an identifier (RexInputRef) only Project, got: %s in project",
            node.getClass().getSimpleName());
      }
      RelNode projectInput = PinotRuleUtils.unboxRel(project.getInput());
      Preconditions.checkState(projectInput instanceof TableScan,
          "Right input for lookup join must be a Project over TableScan, got Project over: %s",
          projectInput.getClass().getSimpleName());
    } else {
      // TODO: Consider adding DYNAMIC_BROADCAST as a separate join strategy
      joinStrategy = JoinNode.JoinStrategy.HASH;
    }

    // convert filter and project RexNode into RexExpression
    List<EnrichedJoinNode.FilterProjectRex> filterProjectRexes = getFilterProjectRexes(rel);

    int fetch = RexExpressionUtils.getValueAsInt(rel.getFetch());
    int offset = RexExpressionUtils.getValueAsInt(rel.getOffset());

    return new EnrichedJoinNode(DEFAULT_STAGE_ID, joinResultSchema, projectedSchema,
        NodeHint.fromRelHints(rel.getHints()), inputs, joinType,
        joinInfo.leftKeys, joinInfo.rightKeys, RexExpressionUtils.fromRexNodes(joinInfo.nonEquiConditions),
        joinStrategy,
        null,
        filterProjectRexes,
        fetch, offset);
  }

  @NotNull
  private static List<EnrichedJoinNode.FilterProjectRex> getFilterProjectRexes(PinotLogicalEnrichedJoin rel) {
    List<PinotLogicalEnrichedJoin.FilterProjectRexNode> filterProjectRexNode = rel.getFilterProjectRexNodes();
    List<EnrichedJoinNode.FilterProjectRex> filterProjectRexes = new ArrayList<>();
    filterProjectRexNode.forEach((node) -> {
      if (node.getType() == PinotLogicalEnrichedJoin.FilterProjectRexNodeType.FILTER) {
        filterProjectRexes.add(new EnrichedJoinNode.FilterProjectRex(RexExpressionUtils.fromRexNode(node.getFilter())));
      } else {
        filterProjectRexes.add(
            new EnrichedJoinNode.FilterProjectRex(
                RexExpressionUtils.fromRexNodes(node.getProjectAndResultRowType().getProject()),
                toDataSchema(node.getProjectAndResultRowType().getDataType())
            ));
      }
    });
    return filterProjectRexes;
  }

  private JoinNode convertLogicalAsofJoin(LogicalAsofJoin join) {
    JoinInfo joinInfo = join.analyzeCondition();
    DataSchema dataSchema = toDataSchema(join.getRowType());
    List<PlanNode> inputs = convertInputs(join.getInputs());
    JoinRelType joinType = join.getJoinType();

    // Basic validations
    Preconditions.checkState(inputs.size() == 2, "Join should have exactly 2 inputs, got: %s", inputs.size());
    Preconditions.checkState(joinInfo.nonEquiConditions.isEmpty(),
        "Non-equi conditions are not supported for ASOF join, got: %s", joinInfo.nonEquiConditions);
    Preconditions.checkState(joinType == JoinRelType.ASOF || joinType == JoinRelType.LEFT_ASOF,
        "Join type should be ASOF or LEFT_ASOF, got: %s", joinType);

    PlanNode left = inputs.get(0);
    PlanNode right = inputs.get(1);
    int numLeftColumns = left.getDataSchema().size();
    int numResultColumns = dataSchema.size();
    int numRightColumns = right.getDataSchema().size();
    Preconditions.checkState(numLeftColumns + numRightColumns == numResultColumns,
        "Invalid number of columns for join type: %s, left: %s, right: %s, result: %s", joinType, numLeftColumns,
        numRightColumns, numResultColumns);

    RexExpression matchCondition = RexExpressionUtils.fromRexNode(join.getMatchCondition());
    Preconditions.checkState(matchCondition != null, "ASOF_JOIN must have a match condition");
    Preconditions.checkState(matchCondition instanceof RexExpression.FunctionCall,
        "ASOF JOIN only supports function call match condition, got: %s", matchCondition);

    List<RexExpression> matchKeys = ((RexExpression.FunctionCall) matchCondition).getFunctionOperands();
    // TODO: Add support for MATCH_CONDITION containing two columns of different types. In that case, there would be
    //       a CAST RexExpression.FunctionCall on top of the RexExpression.InputRef, and the physical ASOF join operator
    //       can't currently handle that.
    Preconditions.checkState(
        matchKeys.size() == 2 && matchKeys.get(0) instanceof RexExpression.InputRef
            && matchKeys.get(1) instanceof RexExpression.InputRef,
        "ASOF_JOIN only supports match conditions with a comparison between two columns of the same type");

    return new JoinNode(DEFAULT_STAGE_ID, dataSchema, NodeHint.fromRelHints(join.getHints()), inputs, joinType,
        joinInfo.leftKeys, joinInfo.rightKeys, RexExpressionUtils.fromRexNodes(joinInfo.nonEquiConditions),
        JoinNode.JoinStrategy.ASOF, RexExpressionUtils.fromRexNode(join.getMatchCondition()));
  }

  private List<PlanNode> convertInputs(List<RelNode> inputs) {
    // NOTE: Inputs can be modified in place. Do not create immutable List here.
    int numInputs = inputs.size();
    List<PlanNode> planNodes = new ArrayList<>(numInputs);
    for (RelNode input : inputs) {
      planNodes.add(toPlanNode(input));
    }
    return planNodes;
  }

  private static DataSchema toDataSchema(RelDataType rowType) {
    if (rowType instanceof RelRecordType) {
      RelRecordType recordType = (RelRecordType) rowType;
      String[] columnNames = recordType.getFieldNames().toArray(new String[]{});
      ColumnDataType[] columnDataTypes = new ColumnDataType[columnNames.length];
      for (int i = 0; i < columnNames.length; i++) {
        columnDataTypes[i] = convertToColumnDataType(recordType.getFieldList().get(i).getType());
      }
      return new DataSchema(columnNames, columnDataTypes);
    } else {
      throw new IllegalArgumentException("Unsupported RelDataType: " + rowType);
    }
  }

  public static ColumnDataType convertToColumnDataType(RelDataType relDataType) {
    SqlTypeName sqlTypeName = relDataType.getSqlTypeName();
    if (sqlTypeName == SqlTypeName.NULL) {
      return ColumnDataType.UNKNOWN;
    }
    boolean isArray = (sqlTypeName == SqlTypeName.ARRAY);
    if (isArray) {
      assert relDataType.getComponentType() != null;
      sqlTypeName = relDataType.getComponentType().getSqlTypeName();
    }
    switch (sqlTypeName) {
      case BOOLEAN:
        return isArray ? ColumnDataType.BOOLEAN_ARRAY : ColumnDataType.BOOLEAN;
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return isArray ? ColumnDataType.INT_ARRAY : ColumnDataType.INT;
      case BIGINT:
        return isArray ? ColumnDataType.LONG_ARRAY : ColumnDataType.LONG;
      case DECIMAL:
        return resolveDecimal(relDataType, isArray);
      case FLOAT:
      case REAL:
        return isArray ? ColumnDataType.FLOAT_ARRAY : ColumnDataType.FLOAT;
      case DOUBLE:
        return isArray ? ColumnDataType.DOUBLE_ARRAY : ColumnDataType.DOUBLE;
      case DATE:
      case TIME:
      case TIMESTAMP:
        return isArray ? ColumnDataType.TIMESTAMP_ARRAY : ColumnDataType.TIMESTAMP;
      case CHAR:
      case VARCHAR:
        return isArray ? ColumnDataType.STRING_ARRAY : ColumnDataType.STRING;
      case BINARY:
      case VARBINARY:
        return isArray ? ColumnDataType.BYTES_ARRAY : ColumnDataType.BYTES;
      case MAP:
        return ColumnDataType.MAP;
      case OTHER:
      case ANY:
        return ColumnDataType.OBJECT;
      default:
        if (relDataType.getComponentType() != null) {
          throw new IllegalArgumentException("Unsupported collection type: " + relDataType);
        }
        LOGGER.warn("Unexpected SQL type: {}, use OBJECT instead", sqlTypeName);
        return ColumnDataType.OBJECT;
    }
  }

  /**
   * Calcite uses DEMICAL type to infer data type hoisting and infer arithmetic result types. down casting this back to
   * the proper primitive type for Pinot.
   * TODO: Revisit this method:
   *  - Currently we are converting exact value to approximate value
   *  - Integer can only cover all values with precision 9; Long can only cover all values with precision 18
   *
   * {@link RequestUtils#getLiteralExpression(SqlLiteral)}
   * @param relDataType the DECIMAL rel data type.
   * @param isArray
   * @return proper {@link ColumnDataType}.
   * @see {@link org.apache.calcite.rel.type.RelDataTypeFactoryImpl#decimalOf}.
   */
  private static ColumnDataType resolveDecimal(RelDataType relDataType, boolean isArray) {
    int precision = relDataType.getPrecision();
    int scale = relDataType.getScale();
    if (scale == 0) {
      if (precision <= 10) {
        return isArray ? ColumnDataType.INT_ARRAY : ColumnDataType.INT;
      } else if (precision <= 38) {
        return isArray ? ColumnDataType.LONG_ARRAY : ColumnDataType.LONG;
      } else {
        return isArray ? ColumnDataType.DOUBLE_ARRAY : ColumnDataType.BIG_DECIMAL;
      }
    } else {
      // NOTE: Do not use FLOAT to represent DECIMAL to be consistent with single-stage engine behavior.
      //       See {@link RequestUtils#getLiteralExpression(SqlLiteral)}.
      if (precision <= 30) {
        return isArray ? ColumnDataType.DOUBLE_ARRAY : ColumnDataType.DOUBLE;
      } else {
        return isArray ? ColumnDataType.DOUBLE_ARRAY : ColumnDataType.BIG_DECIMAL;
      }
    }
  }

  public static String getTableNameFromTableScan(TableScan tableScan) {
    return getTableNameFromRelTable(tableScan.getTable());
  }

  public static Set<String> getTableNamesFromRelRoot(RelNode relRoot) {
    List<RelOptTable> tables = RelOptUtil.findAllTables(relRoot);
    Set<String> tableNames = Sets.newHashSetWithExpectedSize(tables.size());
    for (RelOptTable table : tables) {
      tableNames.add(getTableNameFromRelTable(table));
    }
    return tableNames;
  }

  public static String getTableNameFromRelTable(RelOptTable table) {
    List<String> qualifiedName = table.getQualifiedName();
    return qualifiedName.size() == 1 ? qualifiedName.get(0)
        : DatabaseUtils.constructFullyQualifiedTableName(qualifiedName.get(0), qualifiedName.get(1));
  }
}
