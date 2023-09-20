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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.SortExchange;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.logical.PinotLogicalExchange;
import org.apache.calcite.rel.logical.PinotLogicalSortExchange;
import org.apache.calcite.rel.logical.PinotRelExchangeType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.ExchangeNode;
import org.apache.pinot.query.planner.plannode.FilterNode;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.ProjectNode;
import org.apache.pinot.query.planner.plannode.SetOpNode;
import org.apache.pinot.query.planner.plannode.SortNode;
import org.apache.pinot.query.planner.plannode.TableScanNode;
import org.apache.pinot.query.planner.plannode.ValueNode;
import org.apache.pinot.query.planner.plannode.WindowNode;
import org.slf4j.Logger;


/**
 * The {@link RelToPlanNodeConverter} converts a logical {@link RelNode} to a {@link PlanNode}.
 */
public final class RelToPlanNodeConverter {

  private static final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(RelToPlanNodeConverter.class);

  private RelToPlanNodeConverter() {
    // do not instantiate.
  }

  /**
   * convert a normal relation node into stage node with just the expression piece.
   *
   * TODO: we should convert this to a more structured pattern once we determine the serialization format used.
   *
   * @param node relational node
   * @return stage node.
   */
  public static PlanNode toStageNode(RelNode node, int currentStageId) {
    if (node instanceof LogicalTableScan) {
      return convertLogicalTableScan((LogicalTableScan) node, currentStageId);
    } else if (node instanceof LogicalJoin) {
      return convertLogicalJoin((LogicalJoin) node, currentStageId);
    } else if (node instanceof LogicalProject) {
      return convertLogicalProject((LogicalProject) node, currentStageId);
    } else if (node instanceof LogicalFilter) {
      return convertLogicalFilter((LogicalFilter) node, currentStageId);
    } else if (node instanceof LogicalAggregate) {
      return convertLogicalAggregate((LogicalAggregate) node, currentStageId);
    } else if (node instanceof LogicalSort) {
      return convertLogicalSort((LogicalSort) node, currentStageId);
    } else if (node instanceof LogicalValues) {
      return convertLogicalValues((LogicalValues) node, currentStageId);
    } else if (node instanceof LogicalWindow) {
      return convertLogicalWindow((LogicalWindow) node, currentStageId);
    } else if (node instanceof SetOp) {
      return convertLogicalSetOp((SetOp) node, currentStageId);
    } else if (node instanceof Exchange) {
      return convertLogicalExchange((Exchange) node, currentStageId);
    } else {
      throw new UnsupportedOperationException("Unsupported logical plan node: " + node);
    }
  }

  private static PlanNode convertLogicalExchange(Exchange node, int currentStageId) {
    RelCollation collation = null;
    boolean isSortOnSender = false;
    boolean isSortOnReceiver = false;
    PinotRelExchangeType exchangeType = PinotRelExchangeType.getDefaultExchangeType();
    if (node instanceof SortExchange) {
      collation = ((SortExchange) node).getCollation();
      if (node instanceof PinotLogicalSortExchange) {
        // These flags only take meaning if the collation is not null or empty
        isSortOnSender = ((PinotLogicalSortExchange) node).isSortOnSender();
        isSortOnReceiver = ((PinotLogicalSortExchange) node).isSortOnReceiver();
        exchangeType = ((PinotLogicalSortExchange) node).getExchangeType();
      }
    } else {
      if (node instanceof PinotLogicalExchange) {
        exchangeType = ((PinotLogicalExchange) node).getExchangeType();
      }
    }
    List<RelFieldCollation> fieldCollations = (collation == null) ? null : collation.getFieldCollations();

    // Compute all the tables involved under this exchange node
    Set<String> tableNames = getTableNamesFromRelRoot(node);

    return new ExchangeNode(currentStageId, toDataSchema(node.getRowType()), exchangeType, tableNames,
        node.getDistribution(), fieldCollations, isSortOnSender, isSortOnReceiver);
  }

  private static PlanNode convertLogicalSetOp(SetOp node, int currentStageId) {
    return new SetOpNode(SetOpNode.SetOpType.fromObject(node), currentStageId, toDataSchema(node.getRowType()),
        node.all);
  }

  private static PlanNode convertLogicalValues(LogicalValues node, int currentStageId) {
    return new ValueNode(currentStageId, toDataSchema(node.getRowType()), node.tuples);
  }

  private static PlanNode convertLogicalWindow(LogicalWindow node, int currentStageId) {
    return new WindowNode(currentStageId, node.groups, node.constants, toDataSchema(node.getRowType()));
  }

  private static PlanNode convertLogicalSort(LogicalSort node, int currentStageId) {
    int fetch = RexExpressionUtils.getValueAsInt(node.fetch);
    int offset = RexExpressionUtils.getValueAsInt(node.offset);
    return new SortNode(currentStageId, node.getCollation().getFieldCollations(), fetch, offset,
        toDataSchema(node.getRowType()));
  }

  private static PlanNode convertLogicalAggregate(LogicalAggregate node, int currentStageId) {
    return new AggregateNode(currentStageId, toDataSchema(node.getRowType()), node.getAggCallList(),
        RexExpressionUtils.fromInputRefs(node.getGroupSet()), node.getHints());
  }

  private static PlanNode convertLogicalProject(LogicalProject node, int currentStageId) {
    return new ProjectNode(currentStageId, toDataSchema(node.getRowType()), node.getProjects());
  }

  private static PlanNode convertLogicalFilter(LogicalFilter node, int currentStageId) {
    return new FilterNode(currentStageId, toDataSchema(node.getRowType()), node.getCondition());
  }

  private static PlanNode convertLogicalTableScan(LogicalTableScan node, int currentStageId) {
    String tableName = node.getTable().getQualifiedName().get(0);
    List<String> columnNames =
        node.getRowType().getFieldList().stream().map(RelDataTypeField::getName).collect(Collectors.toList());
    return new TableScanNode(currentStageId, toDataSchema(node.getRowType()), node.getHints(), tableName, columnNames);
  }

  private static PlanNode convertLogicalJoin(LogicalJoin node, int currentStageId) {
    JoinRelType joinType = node.getJoinType();

    // Parse out all equality JOIN conditions
    JoinInfo joinInfo = node.analyzeCondition();
    JoinNode.JoinKeys joinKeys = new JoinNode.JoinKeys(joinInfo.leftKeys, joinInfo.rightKeys);
    List<RexExpression> joinClause =
        joinInfo.nonEquiConditions.stream().map(RexExpressionUtils::fromRexNode).collect(Collectors.toList());
    return new JoinNode(currentStageId, toDataSchema(node.getRowType()), toDataSchema(node.getLeft().getRowType()),
        toDataSchema(node.getRight().getRowType()), joinType, joinKeys, joinClause, node.getHints());
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
    boolean isArray = (sqlTypeName == SqlTypeName.ARRAY);
    if (isArray) {
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
   *
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
      if (precision <= 14) {
        return isArray ? ColumnDataType.FLOAT_ARRAY : ColumnDataType.FLOAT;
      } else if (precision <= 30) {
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
      tableNames.add(tableName);
    }
    return tableNames;
  }
}
