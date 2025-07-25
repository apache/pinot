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
import org.apache.pinot.common.function.scalar.TrigonometricFunctions;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfExampleBuilder;
import org.apache.pinot.core.udf.UdfParameter;
import org.apache.pinot.core.udf.UdfSignature;
import org.apache.pinot.spi.data.FieldSpec;

@AutoService(Udf.class)
public class Atan2Udf extends Udf.FromAnnotatedMethod {
  public Atan2Udf() throws NoSuchMethodException {
    super(TrigonometricFunctions.class.getMethod("atan2", double.class, double.class));
  }

  @Override
  public String getDescription() {
    return "Returns the arc tangent of the quotient of its arguments (in radians).";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UdfExampleBuilder.forSignature(UdfSignature.of(
            UdfParameter.of("y", FieldSpec.DataType.DOUBLE)
                .withDescription("Y coordinate (numerator)"),
            UdfParameter.of("x", FieldSpec.DataType.DOUBLE)
                .withDescription("X coordinate (denominator)"),
            UdfParameter.result(FieldSpec.DataType.DOUBLE)
                .withDescription("Arc tangent of y/x (radians)")
        ))
        .addExample("atan2(0, 1)", 0.0, 1.0, 0.0)
        .addExample("atan2(1, 0)", 1.0, 0.0, Math.PI / 2)
        .addExample("atan2(-1, 0)", -1.0, 0.0, -Math.PI / 2)
        .addExample(UdfExample.create("null y", null, 1.0, null)
            .withoutNull(0d))
        .addExample(UdfExample.create("null x", 1.0, null, null)
            .withoutNull(Math.PI / 2))
        .addExample(UdfExample.create("null both", null, null, null)
            .withoutNull(0d))
        .build()
        .generateExamples();
  }
}

