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
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.function.PinotScalarFunction;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.function.scalar.ArrayFunctions;
import org.apache.pinot.core.operator.transform.function.ArraySumTransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfExampleBuilder;
import org.apache.pinot.core.udf.UdfParameter;
import org.apache.pinot.core.udf.UdfSignature;
import org.apache.pinot.spi.data.FieldSpec;

@AutoService(Udf.class)
public class ArraySumUdf extends Udf {
  @Override
  public String getMainName() {
    return "arraySum";
  }

  @Override
  public String getDescription() {
    return "Returns the sum of all elements in the input array of doubles. "
        + "If the array is null, returns null. If the array is empty, returns 0.";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UdfExampleBuilder.forSignature(UdfSignature.of(
            UdfParameter.of("array", FieldSpec.DataType.DOUBLE)
                .asMultiValued()
                .withDescription("Input array of doubles"),
            UdfParameter.result(FieldSpec.DataType.DOUBLE)
                .withDescription("Sum of all elements in the array")
        ))
        .addExample("sum array", List.of(1.0, 2.0, 3.0), 6.0)
        .addExample("sum array with negatives", List.of(-1.0, 2.0, -3.0), -2.0)
        .addExample("sum single element", List.of(42.0), 42.0)
        .addExample("sum empty array", List.of(), 0.0)
        .addExample(UdfExample.create("null array", null, null).withoutNull(Double.NEGATIVE_INFINITY))
        .build()
        .generateExamples();
  }

  @Nullable
  @Override
  public PinotScalarFunction getScalarFunction() {
    return null;
  }

  @Nullable
  @Override
  public Pair<TransformFunctionType, Class<? extends TransformFunction>> getTransformFunction() {
    return Pair.of(TransformFunctionType.ARRAY_SUM, ArraySumTransformFunction.class);
  }
}

