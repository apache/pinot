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
import org.apache.pinot.common.function.PinotScalarFunction;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.function.scalar.LogicalFunctions;
import org.apache.pinot.core.operator.transform.function.NotOperatorTransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfExampleBuilder;
import org.apache.pinot.core.udf.UdfParameter;
import org.apache.pinot.core.udf.UdfSignature;
import org.apache.pinot.spi.data.FieldSpec;


@AutoService(Udf.class)
public class NotUdf extends Udf.FromAnnotatedMethod {

  public NotUdf()
      throws NoSuchMethodException {
    super(LogicalFunctions.class.getMethod("not", boolean.class));
  }

  @Override
  public String getMainName() {
    return "not";
  }

  @Override
  public String getDescription() {
    return "Logical NOT function for a boolean value. Returns true if the argument is false, false if true, and null "
        + "if null.";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UdfExampleBuilder.forSignature(
            UdfSignature.of(
                UdfParameter.of("value", FieldSpec.DataType.BOOLEAN)
                    .withDescription("A boolean value to negate."),
                UdfParameter.result(FieldSpec.DataType.BOOLEAN)
                    .withDescription("Returns the logical negation of the input value.")
            ))
        .addExample("not true", true, false)
        .addExample("not false", false, true)
        .addExample(UdfExample.create("not null", null, null).withoutNull(true))
        .build()
        .generateExamples();
  }

  @Override
  public PinotScalarFunction getScalarFunction() {
    return null;
  }

  @Override
  public Pair<TransformFunctionType, Class<? extends TransformFunction>> getTransformFunction() {
    return Pair.of(TransformFunctionType.NOT, NotOperatorTransformFunction.class);
  }
}
