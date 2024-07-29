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
package org.apache.pinot.core.common;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.annotations.InterfaceAudience;
import org.apache.pinot.spi.exception.EarlyTerminationException;


@InterfaceAudience.Private
public interface Operator<T extends Block> {

  /**
   * Get the next {@link Block}.
   * <p>For filter operator and operators above projection phase (aggregation, selection, combine etc.), method should
   * only be called once, and will return a non-null block.
   * <p>For operators in projection phase (docIdSet, projection, transformExpression), method can be called multiple
   * times, and will return non-empty block or null if no more documents available
   *
   * @throws EarlyTerminationException if the operator is early-terminated (interrupted) before processing the next
   *         block of data. Operator can early terminated when the query times out, or is already satisfied.
   */
  T nextBlock();

  /** @return List of {@link Operator}s that this operator depends upon. */
  List<? extends Operator> getChildOperators();

  /** @return Explain Plan description if available; otherwise, null. */
  @Nullable
  String toExplainString();

  default void prepareForExplainPlan(ExplainPlanRows explainPlanRows) {
  }

  default void postExplainPlan(ExplainPlanRows explainPlanRows) {
  }

  default void explainPlan(ExplainPlanRows explainPlanRows, int[] globalId, int parentId) {
    prepareForExplainPlan(explainPlanRows);
    String explainPlanString = toExplainString();
    if (explainPlanString != null) {
      ExplainPlanRowData explainPlanRowData = new ExplainPlanRowData(explainPlanString, globalId[0], parentId);
      parentId = globalId[0]++;
      explainPlanRows.appendExplainPlanRowData(explainPlanRowData);
    }

    List<? extends Operator> children = getChildOperators();
    for (Operator child : children) {
      if (child != null) {
        child.explainPlan(explainPlanRows, globalId, parentId);
      }
    }
    postExplainPlan(explainPlanRows);
  }

  /**
   * Returns the index segment associated with the operator.
   */
  default IndexSegment getIndexSegment() {
    return null;
  }

  /**
   * Returns the execution statistics associated with the operator. This method should be called after the operator has
   * finished execution.
   */
  default ExecutionStatistics getExecutionStatistics() {
    throw new UnsupportedOperationException();
  }
}
