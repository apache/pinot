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

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;


/**
 * The <code>Not</code> extends implement the Not operator.
 *
 * The results are in boolean format and stored as an integer array with 1 represents true and 0 represents false.
 * It takes a single argument and negates it and the argument has to be a boolean/integer.
 *
 * Expected result:
 * Not (1 = 1) | 0
 * Not 1       | 0
 *
 * SQL Syntax:
 *    Not <Boolean Expression>
 *
 * Sample Usage:
 *    Not(booleanA)
 *    Not booleanA
 */
public class NotOperatorTransformFunction extends BaseTransformFunction {
  private TransformFunction _argument;
  private int[] _results;

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    Preconditions.checkArgument(arguments.size() == 1, "Exact 1 argument1 is required for not transform function");
    TransformResultMetadata argumentMetadata = arguments.get(0).getResultMetadata();
    Preconditions.checkState(
        argumentMetadata.isSingleValue() && argumentMetadata.getDataType().getStoredType().isNumeric(),
        "Unsupported argument type. Expecting single-valued boolean/number");
    _argument = arguments.get(0);
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return BOOLEAN_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public String getName() {
    return TransformFunctionType.NOT.getName();
  }

  @Override
  public int[] transformToIntValuesSV(ProjectionBlock projectionBlock) {
    int numDocs = projectionBlock.getNumDocs();
    if (_results == null) {
      _results = new int[numDocs];
    }
    int[] intValues = _argument.transformToIntValuesSV(projectionBlock);
    for (int i = 0; i < numDocs; i++) {
      _results[i] = getLogicalNegate(intValues[i]);
    }
    return _results;
  }

  private static int getLogicalNegate(int val) {
    if (val == 0) {
      return 1;
    }
    return 0;
  }
}
