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
package org.apache.pinot.query.planner;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;


/**
 * Utilities used by planner.
 */
public class PlannerUtils {
  private PlannerUtils() {
    // do not instantiate.
  }

  public static List<List<Integer>> parseJoinConditions(RexCall joinCondition, int leftNodeOffset) {
    switch (joinCondition.getOperator().getKind()) {
      case EQUALS:
        RexNode left = joinCondition.getOperands().get(0);
        RexNode right = joinCondition.getOperands().get(1);
        Preconditions.checkState(left instanceof RexInputRef, "only reference supported");
        Preconditions.checkState(right instanceof RexInputRef, "only reference supported");
        return Arrays.asList(Collections.singletonList(((RexInputRef) left).getIndex()),
            Collections.singletonList(((RexInputRef) right).getIndex() - leftNodeOffset));
      case AND:
        List<List<Integer>> predicateColumns = new ArrayList<>(2);
        predicateColumns.add(new ArrayList<>());
        predicateColumns.add(new ArrayList<>());
        for (RexNode operand : joinCondition.getOperands()) {
          Preconditions.checkState(operand instanceof RexCall);
          List<List<Integer>> subPredicate = parseJoinConditions((RexCall) operand, leftNodeOffset);
          predicateColumns.get(0).addAll(subPredicate.get(0));
          predicateColumns.get(1).addAll(subPredicate.get(1));
        }
        return predicateColumns;
      default:
        throw new UnsupportedOperationException("Only equality JOIN conditions are supported.");
    }
  }

  public static boolean isRootStage(int stageId) {
    return stageId == 0;
  }
}
