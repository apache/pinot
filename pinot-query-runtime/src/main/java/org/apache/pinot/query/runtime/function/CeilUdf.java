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
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.function.scalar.ArithmeticFunctions;
import org.apache.pinot.core.operator.transform.function.SingleParamMathTransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfExampleBuilder;
import org.apache.pinot.core.udf.UdfParameter;
import org.apache.pinot.core.udf.UdfSignature;
import org.apache.pinot.spi.data.FieldSpec;

@AutoService(Udf.class)
public class CeilUdf extends Udf.FromAnnotatedMethod {

  public CeilUdf() throws NoSuchMethodException {
    super(ArithmeticFunctions.class.getMethod("ceil", double.class));
  }

  @Override
  public String getDescription() {
    return "Returns the smallest integer value greater than or equal to the input.";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UdfExampleBuilder.forSignature(UdfSignature.of(
            UdfParameter.of("value", FieldSpec.DataType.DOUBLE)
                .withDescription("The numeric value to apply the ceiling function to."),
            UdfParameter.result(FieldSpec.DataType.DOUBLE)
                .withDescription("The smallest integer greater than or equal to the input.")
        ))
        .addExample("ceil(3.2)", 3.2, 4.0)
        .addExample("ceil(-2.7)", -2.7, -2.0)
        .addExample("ceil(0)", 0.0, 0.0)
        .addExample(UdfExample.create("null input", null, null).withoutNull(0.0))
        .build()
        .generateExamples();
  }

  @Override
  public Pair<TransformFunctionType, Class<? extends TransformFunction>> getTransformFunction() {
    return Pair.of(TransformFunctionType.CEIL, SingleParamMathTransformFunction.CeilTransformFunction.class);
  }
}

