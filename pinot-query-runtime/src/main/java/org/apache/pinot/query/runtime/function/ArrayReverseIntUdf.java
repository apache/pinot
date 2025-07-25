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
import org.apache.pinot.common.function.scalar.ArrayFunctions;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfExampleBuilder;
import org.apache.pinot.core.udf.UdfParameter;
import org.apache.pinot.core.udf.UdfSignature;
import org.apache.pinot.spi.data.FieldSpec;

@AutoService(Udf.class)
public class ArrayReverseIntUdf extends Udf.FromAnnotatedMethod {
  public ArrayReverseIntUdf() throws NoSuchMethodException {
    super(ArrayFunctions.class.getMethod("arrayReverseInt", int[].class));
  }

  @Override
  public String getDescription() {
    return "Reverses the order of elements in the input array of integers.";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UdfExampleBuilder.forSignature(UdfSignature.of(
            UdfParameter.of("array", FieldSpec.DataType.INT)
                .asMultiValued()
                .withDescription("Input array of integers"),
            UdfParameter.result(FieldSpec.DataType.INT)
                .asMultiValued()
                .withDescription("Array with elements in reverse order")
        ))
        .addExample("reverse array", List.of(1, 2, 3), List.of(3, 2, 1))
        .addExample("reverse array with duplicates", List.of(1, 2, 2, 3), List.of(3, 2, 2, 1))
        .addExample("reverse single element", List.of(42), List.of(42))
        .addExample("reverse empty array", List.of(), List.of())
        .addExample(UdfExample.create("null array", null, null).withoutNull(List.of()))
        .build()
        .generateExamples();
  }
}

