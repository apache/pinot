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
public class AsinUdf extends Udf.FromAnnotatedMethod {
  public AsinUdf() throws NoSuchMethodException {
    super(TrigonometricFunctions.class.getMethod("asin", double.class));
  }

  @Override
  public String getDescription() {
    return "Returns the arc sine of the input value (in radians).";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UdfExampleBuilder.forSignature(UdfSignature.of(
            UdfParameter.of("value", FieldSpec.DataType.DOUBLE)
                .withDescription("Input value (-1 to 1)"),
            UdfParameter.result(FieldSpec.DataType.DOUBLE)
                .withDescription("Arc sine of the input value (radians)")
        ))
        .addExample("asin(0)", 0.0, 0.0)
        .addExample("asin(1)", 1.0, Math.PI / 2)
        .addExample("asin(-1)", -1.0, -Math.PI / 2)
        .addExample("out of range (positive)", 10d, Double.NaN)
        .addExample("out of range (negative)", -2d, Double.NaN)
        .addExample(UdfExample.create("null value", null, null)
            .withoutNull(0d))
        .build()
        .generateExamples();
  }
}

