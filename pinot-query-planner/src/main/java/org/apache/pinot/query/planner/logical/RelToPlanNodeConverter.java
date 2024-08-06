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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.pinot.calcite.rel.logical.PinotLogicalAggregate;
import org.apache.pinot.calcite.rel.logical.PinotLogicalExchange;
import org.apache.pinot.calcite.rel.logical.PinotLogicalSortExchange;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
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

  /**
   * Converts a {@link RelNode} into its serializable counterpart.
   * NOTE: Stage ID is not determined yet.
   */
  public PlanNode toPlanNode(RelNode node) {
    if (node instanceof LogicalTableScan) {
      return convertLogicalTableScan((LogicalTableScan) node);
    } else if (node instanceof LogicalProject) {
      return convertLogicalProject((LogicalProject) node);
    } else if (node instanceof LogicalFilter) {
      return convertLogicalFilter((LogicalFilter) node);
    } else if (node instanceof PinotLogicalAggregate) {
      return convertLogicalAggregate((PinotLogicalAggregate) node);
    } else if (node instanceof LogicalSort) {
      return convertLogicalSort((LogicalSort) node);
    } else if (node instanceof Exchange) {
      return convertLogicalExchange((Exchange) node);
    } else if (node instanceof LogicalJoin) {
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.JOIN_COUNT, 1);
      if (!_joinFound) {
        _brokerMetrics.addMeteredGlobalValue(BrokerMeter.QUERIES_WITH_JOINS, 1);
        _joinFound = true;
      }
      return convertLogicalJoin((LogicalJoin) node);
    } else if (node instanceof LogicalWindow) {
      _brokerMetrics.addMeteredGlobalValue(BrokerMeter.WINDOW_COUNT, 1);
      if (!_windowFunctionFound) {
        _brokerMetrics.addMeteredGlobalValue(BrokerMeter.QUERIES_WITH_WINDOW, 1);
        _windowFunctionFound = true;
      }
      return convertLogicalWindow((LogicalWindow) node);
    } else if (node instanceof LogicalValues) {
      return convertLogicalValues((LogicalValues) node);
    } else if (node instanceof SetOp) {
      return convertLogicalSetOp((SetOp) node);
    } else {
      throw new IllegalStateException("Unsupported RelNode: " + node);
    }
  }

  private ExchangeNode convertLogicalExchange(Exchange node) {
    PinotRelExchangeType exchangeType;
    List<RelFieldCollation> collations;
    boolean sortOnSender;
    boolean sortOnReceiver;
    if (node instanceof PinotLogicalSortExchange) {
      PinotLogicalSortExchange sortExchange = (PinotLogicalSortExchange) node;
      exchangeType = sortExchange.getExchangeType();
      collations = sortExchange.getCollation().getFieldCollations();
      sortOnSender = sortExchange.isSortOnSender();
      sortOnReceiver = sortExchange.isSortOnReceiver();
    } else {
      assert node instanceof PinotLogicalExchange;
      exchangeType = ((PinotLogicalExchange) node).getExchangeType();
      collations = null;
      sortOnSender = false;
      sortOnReceiver = false;
    }
    RelDistribution distribution = node.getDistribution();
    RelDistribution.Type distributionType = distribution.getType();
    List<Integer> keys;
    boolean prePartitioned;
    if (distributionType == RelDistribution.Type.HASH_DISTRIBUTED) {
      keys = distribution.getKeys();
      RelDistribution inputDistributionTrait = node.getInputs().get(0).getTraitSet().getDistribution();
      prePartitioned = distribution.equals(inputDistributionTrait);
    } else {
      keys = null;
      prePartitioned = false;
    }
    return new ExchangeNode(DEFAULT_STAGE_ID, toDataSchema(node.getRowType()), convertInputs(node.getInputs()),
        exchangeType, distributionType, keys, prePartitioned, collations, sortOnSender, sortOnReceiver, null);
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

    // TODO: For now only the default frame is supported. Add support for custom frames including rows support.
    //       Frame literals come in the constants from the LogicalWindow and the bound.getOffset() stores the
    //       InputRef to the constants array offset by the input array length. These need to be extracted here and
    //       set to the bounds.
    // Lower bound can only be unbounded preceding for now, set to Integer.MIN_VALUE
    int lowerBound = Integer.MIN_VALUE;
    // Upper bound can only be unbounded following or current row for now
    int upperBound = windowGroup.upperBound.isUnbounded() ? Integer.MAX_VALUE : 0;

    // TODO: Constants are used to store constants needed such as the frame literals. For now just save this, need to
    //       extract the constant values into bounds as a part of frame support.
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
        convertInputs(node.getInputs()), functionCalls, filterArgs, node.getGroupSet().asList(), node.getAggType());
  }

  private ProjectNode convertLogicalProject(LogicalProject node) {
    return new ProjectNode(DEFAULT_STAGE_ID, toDataSchema(node.getRowType()), NodeHint.fromRelHints(node.getHints()),
        convertInputs(node.getInputs()), RexExpressionUtils.fromRexNodes(node.getProjects()));
  }

  private FilterNode convertLogicalFilter(LogicalFilter node) {
    return new FilterNode(DEFAULT_STAGE_ID, toDataSchema(node.getRowType()), NodeHint.fromRelHints(node.getHints()),
        convertInputs(node.getInputs()), RexExpressionUtils.fromRexNode(node.getCondition()));
  }

  private TableScanNode convertLogicalTableScan(LogicalTableScan node) {
    String tableName;
    List<String> qualifiedName = node.getTable().getQualifiedName();
    if (qualifiedName.size() == 1) {
      tableName = qualifiedName.get(0);
    } else {
      tableName = DatabaseUtils.translateTableName(qualifiedName.get(1), qualifiedName.get(0));
    }
    List<RelDataTypeField> fields = node.getRowType().getFieldList();
    List<String> columns = new ArrayList<>(fields.size());
    for (RelDataTypeField field : fields) {
      columns.add(field.getName());
    }
    return new TableScanNode(DEFAULT_STAGE_ID, toDataSchema(node.getRowType()), NodeHint.fromRelHints(node.getHints()),
        convertInputs(node.getInputs()), tableName, columns);
  }

  private JoinNode convertLogicalJoin(LogicalJoin node) {
    JoinInfo joinInfo = node.analyzeCondition();
    return new JoinNode(DEFAULT_STAGE_ID, toDataSchema(node.getRowType()), NodeHint.fromRelHints(node.getHints()),
        convertInputs(node.getInputs()), node.getJoinType(), joinInfo.leftKeys, joinInfo.rightKeys,
        RexExpressionUtils.fromRexNodes(joinInfo.nonEquiConditions));
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

  public static Set<String> getTableNamesFromRelRoot(RelNode relRoot) {
    Set<String> tableNames = new HashSet<>();
    List<String> qualifiedTableNames = RelOptUtil.findAllTableQualifiedNames(relRoot);
    for (String qualifiedTableName : qualifiedTableNames) {
      // Calcite encloses table and schema names in square brackets to properly quote and delimit them in SQL
      // statements, particularly to handle cases when they contain special characters or reserved keywords.
      String tableName = qualifiedTableName.replaceAll("^\\[(.*)\\]$", "$1");
      String[] split = tableName.split(", ");
      if (split.length == 1) {
        tableNames.add(tableName);
      } else {
        tableNames.add(DatabaseUtils.translateTableName(split[1], split[0]));
      }
    }
    return tableNames;
  }
}
