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
import org.apache.pinot.core.operator.transform.function.ArrayAverageTransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfExampleBuilder;
import org.apache.pinot.core.udf.UdfParameter;
import org.apache.pinot.core.udf.UdfSignature;
import org.apache.pinot.spi.data.FieldSpec;


@AutoService(Udf.class)
public class ArrayAverageUdf extends Udf {
  @Override
  public String getMainName() {
    return "arrayaverage";
  }

  @Override
  public String getDescription() {
    return "Returns the average of the elements in the input array.";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UdfExampleBuilder.forSignature(UdfSignature.of(
            UdfParameter.of("input", FieldSpec.DataType.DOUBLE)
                .asMultiValued()
                .withDescription("Input array of doubles"),
            UdfParameter.result(FieldSpec.DataType.DOUBLE)
                .withDescription("Average of the input array")
        ))
        .addExample("average of [1, 2, 3, 4]", List.of(1d, 2d, 3d, 4d), 2.5)
        .addExample("average of [5]", List.of(5d), 5d)
        .addExample("empty array", List.of(), null)
        .addExample(UdfExample.create("null input", null, null).withoutNull(Double.NEGATIVE_INFINITY))
        .build()
        .generateExamples();
  }

  @Override
  public PinotScalarFunction getScalarFunction() {
    return null;
  }

  @Override
  public Pair<TransformFunctionType, Class<? extends TransformFunction>> getTransformFunction() {
    return Pair.of(TransformFunctionType.ARRAY_AVERAGE, ArrayAverageTransformFunction.class);
  }
}
