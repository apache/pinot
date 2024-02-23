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
package org.apache.pinot.query.runtime.operator;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.data.table.Record;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Minus/Except operator.
 */
public class MinusOperator extends SetOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MinusOperator.class);
  private static final String EXPLAIN_NAME = "MINUS";

  public MinusOperator(OpChainExecutionContext opChainExecutionContext, List<MultiStageOperator> upstreamOperators,
      DataSchema dataSchema) {
    super(opChainExecutionContext, upstreamOperators, dataSchema);
  }

  @Override
  protected Logger logger() {
    return LOGGER;
  }

  @Nullable
  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected boolean handleRowMatched(Object[] row) {
    return _rightRowSet.add(new Record(row));
  }
}
