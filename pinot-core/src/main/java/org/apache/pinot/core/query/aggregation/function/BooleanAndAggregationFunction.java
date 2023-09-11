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

package org.apache.pinot.core.query.aggregation.function;

import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.segment.spi.AggregationFunctionType;


public class BooleanAndAggregationFunction extends BaseBooleanAggregationFunction {

  public BooleanAndAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    super(verifyArguments(arguments), nullHandlingEnabled, BooleanMerge.AND);
  }

  private static ExpressionContext verifyArguments(List<ExpressionContext> arguments) {
    Preconditions.checkArgument(arguments.size() == 1, "BOOL_AND expects 1 argument, got: %s", arguments.size());
    return arguments.get(0);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.BOOLAND;
  }
}
