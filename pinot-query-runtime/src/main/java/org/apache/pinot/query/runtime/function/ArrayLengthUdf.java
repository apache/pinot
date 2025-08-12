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
import org.apache.pinot.common.function.scalar.array.ArrayLengthScalarFunction;
import org.apache.pinot.core.operator.transform.function.ArrayLengthTransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfExampleBuilder;
import org.apache.pinot.core.udf.UdfParameter;
import org.apache.pinot.core.udf.UdfSignature;
import org.apache.pinot.spi.data.FieldSpec;

@AutoService(Udf.class)
public class ArrayLengthUdf extends Udf {
  @Override
  public String getMainName() {
    return "arrayLength";
  }

  @Override
  public Set<String> getAllNames() {
    return Set.of("array_length", "cardinality");
  }

  @Override
  public String getDescription() {
    return "Returns the length of the input array.";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UdfExampleBuilder.forSignature(UdfSignature.of(
            UdfParameter.of("arr", FieldSpec.DataType.STRING)
                .asMultiValued()
                .withDescription("Input array of strings"),
            UdfParameter.result(FieldSpec.DataType.INT)
                .withDescription("Length of the array")
        ))
        .addExample("normal array", List.of("a", "b", "c"), 3)
        .addExample("empty array", List.of(), 0)
        .addExample("single element", List.of("x"), 1)
        .addExample(UdfExample.create("null array", null, null).withoutNull(0))
        .build()
        .generateExamples();
  }

  @Override
  public Pair<TransformFunctionType, Class<? extends TransformFunction>> getTransformFunction() {
    return Pair.of(TransformFunctionType.ARRAY_LENGTH, ArrayLengthTransformFunction.class);
  }

  @Override
  public PinotScalarFunction getScalarFunction() {
    return new ArrayLengthScalarFunction();
  }
}
