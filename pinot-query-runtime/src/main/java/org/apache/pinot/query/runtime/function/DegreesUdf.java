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
import org.apache.pinot.common.function.scalar.TrigonometricFunctions;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.core.operator.transform.function.TrigonometricTransformFunctions.DegreesTransformFunction;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfExampleBuilder;
import org.apache.pinot.core.udf.UdfParameter;
import org.apache.pinot.core.udf.UdfSignature;
import org.apache.pinot.spi.data.FieldSpec;


@AutoService(Udf.class)
public class DegreesUdf extends Udf.FromAnnotatedMethod {

  public DegreesUdf()
      throws NoSuchMethodException {
    super(TrigonometricFunctions.class.getMethod("degrees", double.class));
  }

  @Override
  public String getDescription() {
    return "Converts an angle measured in radians to an approximately equivalent angle measured in degrees.";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UdfExampleBuilder.forSignature(UdfSignature.of(
            UdfParameter.of("radians", FieldSpec.DataType.DOUBLE)
                .withDescription("The angle in radians to convert to degrees."),
            UdfParameter.result(FieldSpec.DataType.DOUBLE)
                .withDescription("The angle in degrees.")
        ))
        .addExample("degrees(0)", 0d, 0d)
        .addExample("degrees(PI/2)", Math.PI / 2, 90d)
        .addExample("degrees(PI)", Math.PI, 180d)
        .addExample("degrees(2*PI)", 2 * Math.PI, 360d)
        .addExample(UdfExample.create("null input", null, null)
            .withoutNull(0d))
        .build()
        .generateExamples();
  }

  @Override
  public Pair<TransformFunctionType, Class<? extends TransformFunction>> getTransformFunction() {
    return Pair.of(TransformFunctionType.DEGREES, DegreesTransformFunction.class);
  }
}
