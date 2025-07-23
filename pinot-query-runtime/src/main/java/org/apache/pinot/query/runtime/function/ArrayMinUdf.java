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
import org.apache.pinot.common.function.PinotScalarFunction;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfExampleBuilder;
import org.apache.pinot.core.udf.UdfParameter;
import org.apache.pinot.core.udf.UdfSignature;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.core.operator.transform.function.ArrayMinTransformFunction;

@AutoService(Udf.class)
public class ArrayMinUdf extends Udf {
  @Override
  public String getMainName() {
    return "arrayMin";
  }

  @Override
  public String getDescription() {
    return "Returns the minimum value in the input array. If the array is empty or null, returns null.";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UdfExampleBuilder.forSignature(UdfSignature.of(
            UdfParameter.of("array", FieldSpec.DataType.INT)
                .asMultiValued()
                .withDescription("Array of numbers"),
            UdfParameter.result(FieldSpec.DataType.INT)
                .withDescription("Minimum value in the array, or null if array is empty or null.")
        ))
        .addExample("normal array", List.of(1, 2, 3), 1)
        .addExample("negative values", List.of(-5, -2, 0), -5)
        .addExample("single element", List.of(42), 42)
        .addExample(UdfExample.create("empty array", List.of(), null).withoutNull(-2147483648	))
        .addExample(UdfExample.create("null array", null, null).withoutNull(-2147483648	))
        .build()
        .generateExamples();
  }

  @Override
  public Set<PinotScalarFunction> getScalarFunctions() {
    return Set.of();
  }

  @Override
  public Map<TransformFunctionType, Class<? extends TransformFunction>> getTransformFunctions() {
    return Map.of(TransformFunctionType.ARRAY_MIN, ArrayMinTransformFunction.class);
  }
}

