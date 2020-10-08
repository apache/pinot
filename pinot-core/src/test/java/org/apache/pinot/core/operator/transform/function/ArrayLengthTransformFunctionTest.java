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

import org.apache.pinot.core.query.exception.BadQueryRequestException;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class ArrayLengthTransformFunctionTest extends ArrayBaseTransformFunctionTest {

  @Override
  String getFunctionName() {
    return ArrayLengthTransformFunction.FUNCTION_NAME;
  }

  @Override
  Object getExpectResult(int[] intArrary) {
    return intArrary.length;
  }

  @Override
  Class getArrayFunctionClass() {
    return ArrayLengthTransformFunction.class;
  }

  @Override
  FieldSpec.DataType getResultDataType(FieldSpec.DataType inputDataType) {
    return FieldSpec.DataType.INT;
  }
}
