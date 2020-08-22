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
package org.apache.pinot.core.segment.processing.partitioner;

import org.apache.pinot.core.data.function.FunctionEvaluator;
import org.apache.pinot.core.data.function.FunctionEvaluatorFactory;


/**
 * PartitionFilter which evaluates the filter function to decide
 */
public class FunctionEvaluatorPartitionFilter implements PartitionFilter {

  private final FunctionEvaluator _filterFunctionEvaluator;

  public FunctionEvaluatorPartitionFilter(String filterFunction) {
    _filterFunctionEvaluator = FunctionEvaluatorFactory.getExpressionEvaluator(filterFunction);
  }

  @Override
  public boolean filter(String partition) {
    if (_filterFunctionEvaluator != null) {
      Object filter = _filterFunctionEvaluator.evaluate(new Object[]{partition});
      return Boolean.TRUE.equals(filter);
    }
    return false;
  }
}
