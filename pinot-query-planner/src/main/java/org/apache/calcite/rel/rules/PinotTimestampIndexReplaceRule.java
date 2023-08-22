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

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.pinot.spi.config.table.TimestampIndexGranularity;
import org.apache.pinot.spi.utils.TimestampIndexUtils;


public class PinotTimestampIndexReplaceRule extends RelOptRule {
  public static final PinotTimestampIndexReplaceRule INSTANCE =
      new PinotTimestampIndexReplaceRule(PinotRuleUtils.PINOT_REL_FACTORY);

  public PinotTimestampIndexReplaceRule(RelBuilderFactory factory) {
    super(operand(LogicalProject.class, any()), factory, null);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public boolean matches(RelOptRuleCall call) {
    if (call.rels.length < 1) {
      return false;
    }
    LogicalProject logicalProject = call.rel(0);
    for (RexNode rexNode : logicalProject.getProjects()) {
      if (rexNode instanceof RexCall) {
        return ((RexCall) rexNode).getOperator().getName().equalsIgnoreCase("datetrunc");
      }
    }
    return false;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LogicalProject logicalProject = call.rel(0);
    List<RexNode> newProjects = new ArrayList<>();
    for (RexNode rexNode : logicalProject.getProjects()) {
      newProjects.add(convertToTimestampIndex(rexNode, logicalProject.getInput().getRowType()));
    }
    call.transformTo(
        LogicalProject.create(logicalProject.getInput(), logicalProject.getHints(), newProjects,
            logicalProject.getRowType()));
  }

  private RexNode convertToTimestampIndex(RexNode rexNode, RelDataType rowType) {
    if (rexNode instanceof RexCall) {
      RexCall rexCall = (RexCall) rexNode;
      if (rexCall.getOperator().getName().equalsIgnoreCase("datetrunc")) {
        List<RexNode> operands = rexCall.getOperands();
        String timestampColumn = null;
        String granularity = null;
        if (operands.size() == 2) {
          if (operands.get(0) instanceof RexLiteral) {
            granularity =
                operands.get(0).toString().substring(1, operands.get(0).toString().length() - 1).toUpperCase();
          }
          if (operands.get(1) instanceof RexInputRef && ((operands.get(1).getType().getSqlTypeName()
              == SqlTypeName.BIGINT) || operands.get(1).getType().getSqlTypeName() == SqlTypeName.TIMESTAMP)) {
            String timestampColumnRef = operands.get(1).toString();
            int tsColumnIndexRef = Integer.parseInt(timestampColumnRef.substring(1));
            timestampColumn = rowType.getFieldNames().get(tsColumnIndexRef);
          }
          if (timestampColumn != null && granularity != null) {
            String columnWithGranularity = TimestampIndexUtils.getColumnWithGranularity(timestampColumn,
                TimestampIndexGranularity.valueOf(granularity));
            int newTsFieldIndex = rowType.getFieldNames().indexOf(columnWithGranularity);
            if (newTsFieldIndex >= 0) {
              return RexInputRef.of(newTsFieldIndex, rowType);
            }
          }
        }
      }
    }
    return rexNode;
  }
}
