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
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * Partitioner which evaluates a transform function using the row to get the partition value
 */
public class TransformFunctionPartitioner implements Partitioner {

  private final FunctionEvaluator _functionEvaluator;

  public TransformFunctionPartitioner(String transformFunction) {
    _functionEvaluator = FunctionEvaluatorFactory.getExpressionEvaluator(transformFunction);
  }

  @Override
  public String getPartition(GenericRow genericRow) {
    return String.valueOf(_functionEvaluator.evaluate(genericRow));
  }
}
