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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.logical.RexExpressionUtils;
import org.apache.pinot.query.planner.logical.TransformationTracker;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalAggregate;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalExchange;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalJoin;
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


public class PRelToPlanNodeConverter {
  private static final Logger LOGGER = LoggerFactory.getLogger(PRelToPlanNodeConverter.class);
  private static final int DEFAULT_STAGE_ID = -1;

  private final BrokerMetrics _brokerMetrics = BrokerMetrics.get();
  private boolean _joinFound;
  private boolean _windowFunctionFound;
  @Nullable
  private final TransformationTracker.Builder<PlanNode, RelNode> _tracker;

  public PRelToPlanNodeConverter(@Nullable TransformationTracker.Builder<PlanNode, RelNode> tracker) {
    _tracker = tracker;
  }

  /**
   * Converts a {@link RelNode} into its serializable counterpart.
   * NOTE: Stage ID is not determined yet.
   */
  public static PlanNode toPlanNode(PRelNode pRelNode, int stageId) {
    RelNode node = pRelNode.unwrap();
    PlanNode result;
    if (node instanceof TableScan) {
      result = convertTableScan((TableScan) node);
    } else if (node instanceof Project) {
      result = convertProject((Project) node);
    } else if (node instanceof Filter) {
      result = convertFilter((Filter) node);
    } else if (node instanceof PhysicalAggregate) {
      result = convertAggregate((PhysicalAggregate) node);
    } else if (node instanceof Sort) {
      result = convertSort((Sort) node);
    } else if (node instanceof Exchange) {
      result = convertPhysicalExchange((PhysicalExchange) node);
    } else if (node instanceof PhysicalJoin) {
      /* _brokerMetrics.addMeteredGlobalValue(BrokerMeter.JOIN_COUNT, 1);
      if (!_joinFound) {
        _brokerMetrics.addMeteredGlobalValue(BrokerMeter.QUERIES_WITH_JOINS, 1);
        _joinFound = true;
      } */
      result = convertJoin((PhysicalJoin) node);
    } else if (node instanceof Window) {
      /* _brokerMetrics.addMeteredGlobalValue(BrokerMeter.WINDOW_COUNT, 1);
      if (!_windowFunctionFound) {
        _brokerMetrics.addMeteredGlobalValue(BrokerMeter.QUERIES_WITH_WINDOW, 1);
        _windowFunctionFound = true;
      } */
      result = convertWindow((Window) node);
    } else if (node instanceof Values) {
      result = convertValues((Values) node);
    } else if (node instanceof SetOp) {
      result = convertSetOp((SetOp) node);
    } else {
      throw new IllegalStateException("Unsupported RelNode: " + node);
    }
    result.setStageId(stageId);
    /* if (_tracker != null) {
      _tracker.trackCreation(node, result);
    } */
    return result;
  }

  public static ExchangeNode convertPhysicalExchange(PhysicalExchange node) {
    // TODO(mse-physical): Why are table names passed to ExchangeNode?
    return new ExchangeNode(DEFAULT_STAGE_ID, toDataSchema(node.getRowType()),
        new ArrayList<>(), node.getRelExchangeType(), RelDistribution.Type.ANY, node.getDistributionKeys(),
        false, node.getRelCollation().getFieldCollations(), false,
        !node.getRelCollation().getKeys().isEmpty(), Set.of() /* table names */, node.getExchangeStrategy());
  }

  public static SetOpNode convertSetOp(SetOp node) {
    return new SetOpNode(DEFAULT_STAGE_ID, toDataSchema(node.getRowType()), NodeHint.fromRelHints(node.getHints()),
        new ArrayList<>(), SetOpNode.SetOpType.fromObject(node), node.all);
  }

  public static ValueNode convertValues(Values node) {
    List<List<RexExpression.Literal>> literalRows = new ArrayList<>(node.tuples.size());
    for (List<RexLiteral> tuple : node.tuples) {
      List<RexExpression.Literal> literalRow = new ArrayList<>(tuple.size());
      for (RexLiteral rexLiteral : tuple) {
        literalRow.add(RexExpressionUtils.fromRexLiteral(rexLiteral));
      }
      literalRows.add(literalRow);
    }
    return new ValueNode(DEFAULT_STAGE_ID, toDataSchema(node.getRowType()), NodeHint.fromRelHints(node.getHints()),
        new ArrayList<>(), literalRows);
  }

  public static WindowNode convertWindow(Window node) {
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
        new ArrayList<>(), windowGroup.keys.asList(), windowGroup.orderKeys.getFieldCollations(),
        aggCalls, windowFrameType, lowerBound, upperBound, constants);
  }

  public static SortNode convertSort(Sort node) {
    int fetch = RexExpressionUtils.getValueAsInt(node.fetch);
    int offset = RexExpressionUtils.getValueAsInt(node.offset);
    return new SortNode(DEFAULT_STAGE_ID, toDataSchema(node.getRowType()), NodeHint.fromRelHints(node.getHints()),
        new ArrayList<>(), node.getCollation().getFieldCollations(), fetch, offset);
  }

  public static AggregateNode convertAggregate(PhysicalAggregate node) {
    List<AggregateCall> aggregateCalls = node.getAggCallList();
    int numAggregates = aggregateCalls.size();
    List<RexExpression.FunctionCall> functionCalls = new ArrayList<>(numAggregates);
    List<Integer> filterArgs = new ArrayList<>(numAggregates);
    for (AggregateCall aggregateCall : aggregateCalls) {
      functionCalls.add(RexExpressionUtils.fromAggregateCall(aggregateCall));
      filterArgs.add(aggregateCall.filterArg);
    }
    return new AggregateNode(DEFAULT_STAGE_ID, toDataSchema(node.getRowType()), NodeHint.fromRelHints(node.getHints()),
        new ArrayList<>(), functionCalls, filterArgs, node.getGroupSet().asList(), node.getAggType(),
        node.isLeafReturnFinalResult(), node.getCollations(), node.getLimit());
  }

  public static ProjectNode convertProject(Project node) {
    return new ProjectNode(DEFAULT_STAGE_ID, toDataSchema(node.getRowType()), NodeHint.fromRelHints(node.getHints()),
        new ArrayList<>(), RexExpressionUtils.fromRexNodes(node.getProjects()));
  }

  public static FilterNode convertFilter(Filter node) {
    return new FilterNode(DEFAULT_STAGE_ID, toDataSchema(node.getRowType()), NodeHint.fromRelHints(node.getHints()),
        new ArrayList<>(), RexExpressionUtils.fromRexNode(node.getCondition()));
  }

  public static TableScanNode convertTableScan(TableScan node) {
    String tableName = getTableNameFromTableScan(node);
    List<RelDataTypeField> fields = node.getRowType().getFieldList();
    List<String> columns = new ArrayList<>(fields.size());
    for (RelDataTypeField field : fields) {
      columns.add(field.getName());
    }
    return new TableScanNode(DEFAULT_STAGE_ID, toDataSchema(node.getRowType()), NodeHint.fromRelHints(node.getHints()),
        List.of(), tableName, columns);
  }

  public static JoinNode convertJoin(PhysicalJoin join) {
    JoinInfo joinInfo = join.analyzeCondition();
    DataSchema dataSchema = toDataSchema(join.getRowType());
    List<PlanNode> inputs = new ArrayList<>();
    JoinRelType joinType = join.getJoinType();
    JoinNode.JoinStrategy joinStrategy;
    if (PinotHintOptions.JoinHintOptions.useLookupJoinStrategy(join)) {
      joinStrategy = JoinNode.JoinStrategy.LOOKUP;
    } else {
      joinStrategy = JoinNode.JoinStrategy.HASH;
    }
    return new JoinNode(DEFAULT_STAGE_ID, dataSchema, NodeHint.fromRelHints(join.getHints()), inputs, joinType,
        joinInfo.leftKeys, joinInfo.rightKeys, RexExpressionUtils.fromRexNodes(joinInfo.nonEquiConditions),
        joinStrategy);
  }

  public static DataSchema toDataSchema(RelDataType rowType) {
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

  public static String getTableNameFromRelTable(RelOptTable table) {
    List<String> qualifiedName = table.getQualifiedName();
    return qualifiedName.size() == 1 ? qualifiedName.get(0)
        : DatabaseUtils.constructFullyQualifiedTableName(qualifiedName.get(0), qualifiedName.get(1));
  }
}
