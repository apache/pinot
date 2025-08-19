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
import org.apache.pinot.core.operator.transform.function.AndOperatorTransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfExampleBuilder;
import org.apache.pinot.core.udf.UdfParameter;
import org.apache.pinot.core.udf.UdfSignature;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * UDF stub for and (not implemented).
 */
@AutoService(Udf.class)
public class AndUdf extends Udf {
  @Override
  public String getMainName() {
    return "and";
  }

  @Override
  public String getDescription() {
    return "Stub for and function. Not implemented.";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UdfExampleBuilder.forSignature(UdfSignature.of(
            UdfParameter.of("left", FieldSpec.DataType.BOOLEAN)
                .withDescription("Left operand of the AND operation"),
            UdfParameter.of("right", FieldSpec.DataType.BOOLEAN)
                .withDescription("Right operand of the AND operation"),
            UdfParameter.result(FieldSpec.DataType.BOOLEAN)
                .withDescription("Result of the AND operation, true if both operands are true, false otherwise")
        ))
        .addExample("true and true", true, true, true)
        .addExample("true and false", true, false, false)
        .addExample("false and true", false, true, false)
        .addExample("false and false", false, false, false)
        .addExample(UdfExample.create("true and null", true, null, null).withoutNull(false))
        .addExample(UdfExample.create("null and true", null, true, null).withoutNull(false))
        .addExample(UdfExample.create("null and null", null, null, null).withoutNull(false))
        .build()
        .generateExamples();
  }

  @Override
  public PinotScalarFunction getScalarFunction() {
    return null;
  }

  @Override
  public Pair<TransformFunctionType, Class<? extends TransformFunction>> getTransformFunction() {
    return Pair.of(TransformFunctionType.AND, AndOperatorTransformFunction.class);
  }
}
