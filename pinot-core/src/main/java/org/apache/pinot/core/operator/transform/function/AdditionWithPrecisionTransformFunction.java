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
package org.apache.pinot.core.operator.transform.function;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.function.scalar.DataTypeConversionFunctions;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.plan.DocIdSetPlanNode;


public class AdditionWithPrecisionTransformFunction extends BaseTransformFunction {

  public static final String FUNCTION_NAME = "addWithPrecision";

  private List<TransformFunction> _transformFunctions = new ArrayList<>();
  private BigDecimal[] _sums;
  private Integer _precision = null;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    // Check that there are more than 1 arguments
    if (arguments.size() < 3) {
      throw new IllegalArgumentException("At least 3 arguments are required for ADD transform function");
    }

    for (TransformFunction argument : arguments) {
      if (argument instanceof LiteralTransformFunction) {
        if (_precision != null) {
          throw new IllegalArgumentException("Only one precision value can be specified in ADD transform function");
        }
        _precision = Integer.parseInt(((LiteralTransformFunction) argument).getLiteral());
      } else {
        if (!argument.getResultMetadata().isSingleValue()) {
          throw new IllegalArgumentException("All the arguments of ADD transform function must be single-valued");
        }
        _transformFunctions.add(argument);
      }
    }
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return BYTES_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public byte[][] transformToBytesValuesSV(ProjectionBlock projectionBlock) {
    if (_sums == null) {
      _sums = new BigDecimal[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }

    int length = projectionBlock.getNumDocs();
    Arrays.fill(_sums, 0, length, new BigDecimal(0));
    MathContext mathContext;
    if (_precision != null) {
      mathContext = new MathContext(_precision);
    } else {
      mathContext = new MathContext(0);
    }

    for (TransformFunction transformFunction : _transformFunctions) {
      byte[][] values = transformFunction.transformToBytesValuesSV(projectionBlock);
      for (int i = 0; i < length; i++) {
        BigDecimal bigDecimalValue = new BigDecimal(DataTypeConversionFunctions.bytesToBigDecimal(values[i]));
        _sums[i] = _sums[i].add(bigDecimalValue, mathContext);
      }
    }

    byte[][] result = new byte[DocIdSetPlanNode.MAX_DOC_PER_CALL][];
    for (int i = 0; i < length; i++) {
      result[i] = DataTypeConversionFunctions.bigDecimalToBytes(_sums[i]);
    }
    return result;
  }
}
