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
package org.apache.pinot.query.runtime.operator.operands;

import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.datablock.ArrowDataBlock;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;


public interface TransformOperand {

  ColumnDataType getResultType();

  default Object apply(Object[] row) {
    return apply(Arrays.asList(row));
  }

  @Nullable
  Object apply(List<Object> row);

  /**
   * Creates an {@link ArrowEvaluator} bound to the given left and right Arrow blocks.
   *
   * <p>The default implementation throws; operator implementations that support Arrow evaluation should override this.
   */
  default ArrowEvaluator createArrowEvaluator(ArrowDataBlock left, ArrowDataBlock right) {
    throw new UnsupportedOperationException(
        "Arrow evaluation not yet supported for " + getClass().getSimpleName());
  }

  /** Evaluates a predicate or expression given row indices into the left and right Arrow blocks. */
  interface ArrowEvaluator {
    @Nullable
    Object apply(int leftIdx, int rightIdx);
  }
}
