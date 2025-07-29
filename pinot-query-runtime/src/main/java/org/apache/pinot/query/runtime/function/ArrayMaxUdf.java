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
package org.apache.pinot.query.runtime.function;

import com.google.auto.service.AutoService;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.function.PinotScalarFunction;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.core.operator.transform.function.ArrayMaxTransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfExampleBuilder;
import org.apache.pinot.core.udf.UdfParameter;
import org.apache.pinot.core.udf.UdfSignature;
import org.apache.pinot.spi.data.FieldSpec;


@AutoService(Udf.class)
public class ArrayMaxUdf extends Udf {
  @Override
  public String getMainName() {
    return "arrayMax";
  }

  @Override
  public String getDescription() {
    return "Given an array with numeric values, this function returns the maximum value in the array. "
        + "* asdf ";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UdfExampleBuilder.forSignature(UdfSignature.of(
            UdfParameter.of("arr", FieldSpec.DataType.INT)
                .withDescription("Input array of integers")
                .asMultiValued(),  // Input is an array of INT
            UdfParameter.result(FieldSpec.DataType.INT)  // Return type is single value INT
        ))
        .addExample("normal array", List.of(1, 2, 3), 3)
        .addExample("negative values", List.of(-5, -2, 0), 0)
        .addExample("single element", List.of(42), 42)
        .addExample(UdfExample.create("empty array", List.of(), null).withoutNull(-2147483648))
        .addExample(UdfExample.create("null array", null, null).withoutNull(-2147483648))
        .build()
        .generateExamples();
  }

  @Override
  public Pair<TransformFunctionType, Class<? extends TransformFunction>> getTransformFunction() {
    return Pair.of(TransformFunctionType.ARRAY_MAX, ArrayMaxTransformFunction.class);
  }

  @Override
  public PinotScalarFunction getScalarFunction() {
    return null;
  }
}
