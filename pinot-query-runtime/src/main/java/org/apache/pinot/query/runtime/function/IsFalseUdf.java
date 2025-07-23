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
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.function.PinotScalarFunction;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.core.operator.transform.function.IsFalseTransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfExampleBuilder;
import org.apache.pinot.core.udf.UdfParameter;
import org.apache.pinot.core.udf.UdfSignature;
import org.apache.pinot.spi.data.FieldSpec;


@AutoService(Udf.class)
public class IsFalseUdf extends Udf {
  @Override
  public String getMainName() {
    return "isFalse";
  }

  @Override
  public String getDescription() {
    return "Returns true if the input is false (0), otherwise false. Null inputs return false.";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UdfExampleBuilder.forSignature(UdfSignature.of(
            UdfParameter.of("input", FieldSpec.DataType.INT)
                .withDescription("Input value to check if it is false (0)"),
            UdfParameter.result(FieldSpec.DataType.BOOLEAN)
        ))
        .addExample("input is 0 (false)", 0, true)
        .addExample("input is 1 (true)", 1, false)
        .addExample("input is -1 (not false)", -1, false)
        .addExample(UdfExample.create("null input", null, false).withoutNull(true))
        .build()
        .generateExamples();
  }

  @Override
  public Pair<TransformFunctionType, Class<? extends TransformFunction>> getTransformFunction() {
    return Pair.of(TransformFunctionType.IS_FALSE, IsFalseTransformFunction.class);
  }

  @Override
  @Nullable
  public PinotScalarFunction getScalarFunction() {
    return null;
  }
}
