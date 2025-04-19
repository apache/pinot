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
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.calcite.rel.logical.PinotLogicalAggregate;
import org.apache.pinot.calcite.rel.logical.PinotLogicalExchange;
import org.apache.pinot.calcite.rel.logical.PinotLogicalSortExchange;
import org.apache.pinot.calcite.rel.logical.PinotLogicalTableScan;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.calcite.rel.rules.PinotRuleUtils;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.metrics.BrokerMeter;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.ExchangeNode;
import org.apache.pinot.query.planner.plannode.FilterNode;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.PlanNode.NodeHint;
import org.apache.pinot.query.planner.plannode.ProjectNode;
import org.apache.pinot.query.planner.plannode.SetOpNode;
import org.apache.pinot.query.planner.plannode.SortNode;
import org.apache.pinot.query.planner.plannode.TableScanNode;
import org.apache.pinot.query.planner.plannode.ValueNode;
import org.apache.pinot.query.planner.plannode.WindowNode;
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
  private final TableCache _tableCache;

  public RelToPlanNodeConverter(@Nullable TransformationTracker.Builder<PlanNode, RelNode> tracker,
      TableCache tableCache) {
    _tracker = tracker;
    _tableCache = tableCache;
  }

  /**
   * Converts a {@link RelNode} into its serializable counterpart.
   * NOTE: Stage ID is not determined yet.
   */
  public PlanNode toPlanNode(RelNode node) {
    PlanNode result;
    if (node instanceof PinotLogicalTableScan) {
      result = convertPinotLogicalTableScan((PinotLogicalTableScan) node);
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
    } else if (node instanceof LogicalJoin) {
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.JOIN_COUNT, 1);
      if (!_joinFound) {
        _brokerMetrics.addMeteredGlobalValue(BrokerMeter.QUERIES_WITH_JOINS, 1);
        _joinFound = true;
      }
      result = convertLogicalJoin((LogicalJoin) node);
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
        exchangeType, distributionType, keys, prePartitioned, collations, sortOnSender, sortOnReceiver, null, null);
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

  private WindowNode convertLogicalWindow(LogicalWindow node) {
    // Only a single Window Group should exist per WindowNode.
    Preconditions.checkState(node.groups.size() == 1, "Only a single window group is allowed, got: %s",
        node.groups.size());
    Window.Group windowGroup = node.groups.get(0);

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
    String tableName = _tableCache.getActualTableName(getTableNameFromTableScan(node));
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
